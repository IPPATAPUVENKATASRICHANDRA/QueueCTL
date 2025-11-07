import os
import sys
import time
import json
import shutil
import subprocess

import storage
import worker


def run_cli(args):
    proc = subprocess.run([sys.executable, 'queuectl.py'] + args, capture_output=True, text=True)
    return proc.returncode, (proc.stdout or ''), (proc.stderr or '')


def wait_for(pred, timeout_s: float, interval: float = 0.2):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


def read_job(job_id: int):
    return storage.get_job(job_id)


def ensure_fresh_db():
    db = 'queuectl.db'
    if os.environ.get('KEEP_DB'):
        return
    if os.path.exists(db):
        shutil.move(db, db + '.bak')
    storage.make_db()


def print_section(title):
    print('\n=== ' + title + ' ===')


def main():
    print('starting end-to-end tests...')
    ensure_fresh_db()

    # 0) baseline config
    storage.set_config('workers_should_stop', '0')
    storage.set_config('backoff', '2')
    storage.set_config('max_retries', '3')

    # 1) config set/get via CLI
    print_section('config')
    rc, out, err = run_cli(['config', 'set', 'max-retries', '3'])
    assert rc == 0 and 'max_retries=3' in out
    rc, out, err = run_cli(['config', 'get', 'max_retries'])
    assert rc == 0 and out.strip() == '3'

    # 2) start 2 workers (internal threads to keep test non-blocking)
    print_section('start workers')
    t1, wid1 = worker.func_start_background_worker(poll_interval=0.2, backoff_base=2)
    t2, wid2 = worker.func_start_background_worker(poll_interval=0.2, backoff_base=2)
    assert wait_for(lambda: storage.count_active_workers(10) >= 2, 3.0)
    print(f'started workers: {wid1}, {wid2}')

    # 3) enqueue success (CLI JSON) and wait complete
    print_section('enqueue success')
    payload = '{"command":"python -c \\\"print(123)\\\"","max_retries":3}'
    rc, out, err = run_cli(['enqueue', payload])
    assert rc == 0 and out.strip().startswith('enqueued ')
    ok_id = int(out.strip().split()[-1])
    assert wait_for(lambda: (read_job(ok_id) or [None, None, ''])[2] == 'completed', 5.0)

    # 4) enqueue failing (dead after retries)
    print_section('enqueue failing')
    payload = '{"command":"python -c \\\"import sys; sys.exit(2)\\\"","max_retries":2}'
    rc, out, err = run_cli(['enqueue', payload])
    bad_id = int(out.strip().split()[-1])
    assert wait_for(lambda: (read_job(bad_id) or [None, None, ''])[2] == 'dead', 8.0)

    # 5) dlq list / retry
    print_section('dlq ops')
    rc, out, err = run_cli(['dlq', 'list'])
    assert rc == 0 and str(bad_id) in out
    rc, out, err = run_cli(['dlq', 'retry', str(bad_id)])
    assert rc == 0 and 'retried' in out

    # It will likely go dead again; wait briefly for it to settle
    wait_for(lambda: (read_job(bad_id) or [None, None, ''])[2] in ('pending', 'processing', 'dead', 'completed'), 3.0)

    # 6) list by states
    print_section('list states')
    rc, out, err = run_cli(['list', '--state', 'pending'])
    assert rc == 0
    rc, out, err = run_cli(['list', '--state', 'completed'])
    assert rc == 0 and str(ok_id) in out

    # 7) history json lines (all, and per job)
    print_section('history')
    rc, out, err = run_cli(['history'])
    assert rc == 0 and '{' in out
    first_line = out.strip().splitlines()[0]
    j = json.loads(first_line)
    for k in ['id','command','state','attempts','max_retries','created_at','updated_at']:
        assert k in j
    rc, out, err = run_cli(['history', '--job-id', str(ok_id)])
    assert rc == 0 and str(ok_id) in out

    # 8) parallelism check: 3 jobs with 2 workers should finish in ~<=4s
    print_section('parallelism')
    ids = []
    for _ in range(3):
        rc, out, err = run_cli(['enqueue', '{"command":"python -c \\\"import time; time.sleep(2)\\\""}'])
        ids.append(int(out.strip().split()[-1]))
    start = time.time()
    for jid in ids:
        assert wait_for(lambda jid=jid: (read_job(jid) or [None, None, ''])[2] == 'completed', 7.0)
    duration = time.time() - start
    print(f'parallel jobs finished in ~{duration:.1f}s')

    # 9) status prints
    print_section('status')
    rc, out, err = run_cli(['status'])
    assert rc == 0 and 'Jobs:' in out

    # 10) graceful stop
    print_section('stop workers')
    rc, out, err = run_cli(['worker', 'stop'])
    assert rc == 0
    assert wait_for(lambda: storage.count_active_workers(10) == 0, 5.0)
    print('all tests passed')


if __name__ == '__main__':
    main()


