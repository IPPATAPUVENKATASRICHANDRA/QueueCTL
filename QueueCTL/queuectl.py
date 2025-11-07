import argparse
import json
import sys
import time
import storage
import worker

# function for enqueuing a command to the queue
def cmd_enqueue(args):
    storage.make_db()
    command = None 
    try:
        load_json = None
        try:
        # parsing the command from the user in json format

            payload_str = args.payload if isinstance(args.payload, str) else ' '.join(args.payload or [])
            load_json = json.loads(payload_str)
        except Exception:
            load_json = None
        
        # getting command from the json and setting the default retries and saving in the database
        if load_json:
            command = load_json.get('command')
            default_retries = int(storage.get_config('max_retries', str(args.retries)) or args.retries)
            retries = int(load_json.get('max_retries') or default_retries)
            external_id = load_json.get('id')
        else:
            # if the command is not in json format, then we are setting the command and retries from the command line arguments
            payload_str = args.payload if isinstance(args.payload, str) else ' '.join(args.payload or [])
            command = payload_str
            retries = int(storage.get_config('max_retries', str(args.retries)) or args.retries)
            external_id = None
    except Exception:
        payload_str = args.payload if isinstance(args.payload, str) else ' '.join(args.payload or [])
        command = payload_str
        retries = int(storage.get_config('max_retries', str(args.retries)) or args.retries)
        external_id = None
    job_id = storage.add_job(command, state='pending', max_retires=retries, external_id=external_id)
    if job_id is not None:
        print(f"enqueued {job_id}")
        try:
            storage.add_event(job_id, 'enqueued', f'cmd={command}, max_retires={retries}')
        except Exception:
            pass
    else:
        sys.exit(1)


# function for listing jobs from the queue
def cmd_list(args):
    storage.make_db()
    # getting the jobs from the database based on the state
    rows = storage.list_jobs(args.state)
    job_values = f"Jobs ({args.state if args.state else 'all'}):"
    print(job_values)
    if not rows:
        print("No jobs found.")
        return
    else:
        for r in rows:
            print(f"  {r[0]}\t{r[2]}\tattempts={r[3]}/{r[4]}\tcmd={r[1]}")



# function for handling the dead letter queue
def cmd_dlq(args):
    storage.make_db()
    # listing the jobs in the dead letter queue
    if args.action == 'list':
        rows = storage.list_dead_jobs_with_external()
        # if no rows found print dlq is empty else print the jobs in dead letter queue
        if not rows:
            print('DLQ is empty')
        else:
            for r in rows:
                # r: id, external_id, command
                ext = r[1] or '-'
                print(f"{r[0]} ({ext})\tdead\tcmd={r[2]}")
    # if the action is retry then retry the job in the dead letter queue
    elif args.action == 'retry':
        # support numeric id or external id, e.g., 'job1'
        res = storage.retry_dead_by_identifier(str(args.job_id))
        if res:
            print(f"retried {args.job_id}")
            try:
                # if args.job_id isn't numeric, look up id for event
                try:
                    jid = int(args.job_id)
                except Exception:
                    row = storage.get_job_by_external_id(str(args.job_id))
                    jid = int(row[0]) if row else None
                if jid is not None:
                    storage.add_event(jid, 'dlq_retry', None)
            except Exception:
                pass
        else:
            print(f"job {args.job_id} not in DLQ", file=sys.stderr)
            sys.exit(1)


# function for showing job and worker status
def cmd_status(args):
    storage.make_db()
    try:
        counts = storage.counts_by_state() or {}
    except Exception:
        storage.make_db()
        counts = storage.counts_by_state() or {}
    full = {'pending': 0, 'processing': 0, 'completed': 0, 'failed': 0, 'dead': 0}
    full.update(counts)
    print("Jobs:")
    for s in ['pending', 'processing', 'completed', 'failed', 'dead']:
        print(f"  {s}: {full.get(s,0)}")
    try:
        active = storage.count_active_workers(10)
    except Exception:
        storage.make_db()
        active = storage.count_active_workers(10)
    print(f"Active workers: {active}")


def cmd_history(args):
    storage.make_db()
    # Output jobs in JSON schema: id, command, state, attempts, max_retries, created_at, updated_at
    def to_iso_z(ts: str | None) -> str:
        if not ts:
            return ""
        s = str(ts)
        if 'T' not in s and ' ' in s:
            s = s.replace(' ', 'T')
        if not s.endswith('Z'):
            s = s + 'Z'
        return s

    def print_job_row(row):
        job = {
            "id": str(row[0]),
            "command": row[1],
            "state": row[2],
            "attempts": int(row[3] or 0),
            "max_retries": int(row[4] or 0),
            "created_at": to_iso_z(row[5]),
            "updated_at": to_iso_z(row[6]),
        }
        import json as _json
        print(_json.dumps(job))

    if args.job_id is not None:
        row = storage.get_job(int(args.job_id))
        if not row:
            print("<none>")
            return
        print_job_row(row)
        return

    rows = storage.list_jobs(None)
    if not rows:
        print("<none>")
        return
    for row in rows:
        print_job_row(row)

# function for starting and stopping workers
def cmd_worker(args):
    storage.make_db()
    # if the action is start then start the workers by setting the workers_should_stop config to 0 because 0 means workers should not stop
    if args.action == 'start':
        storage.set_config('workers_should_stop', '0')
        threads = []
        worker_ids = []
        # starting the workers in the background by creating each worker a new thread
        for _ in range(args.count):
            t, wid = worker.func_start_background_worker(poll_interval=1.0, backoff_base=args.backoff)
            threads.append(t)
            worker_ids.append(wid)
        print(f"started {len(worker_ids)} worker(s): {', '.join(worker_ids)}")
        # keeping the parent thread running by sleeping for 1 second
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print('stopping workers...')
            storage.set_config('workers_should_stop', '1')
    # if the action is stop then stop the workers by setting the workers_should_stop config to 1 because 1 means workers should stop
    elif args.action == 'stop':
        storage.set_config('workers_should_stop', '1')
        print('signaled workers to stop')
    else:
        print('unknown action', file=sys.stderr)
        sys.exit(1)


# function in which users can set and get the config values
def cmd_config(args):
    storage.make_db()
    # normalize common key variants (e.g., max-retries -> max_retries)
    key = (args.key or '').replace('-', '_')
    # if user want to make 'set config' then set the config value in the database
    if args.cfg_action == 'set':
        if args.value is None:
            print('value is required for config set', file=sys.stderr)
            sys.exit(1)
        storage.set_config(key, args.value)
        print(f"{key}={args.value}")
    # if user want to make 'get config' then get the config value from the database
    elif args.cfg_action == 'get':
        val = storage.get_config(key, None)
        if val is None:
            print('not found')
        else:
            print(val)
    else:
        print('config requires set|get', file=sys.stderr)
        sys.exit(1)


# function to build the parser for the cli
def build_parser():
    
    # building the parser for cli
    parser = argparse.ArgumentParser(prog='queuectl', description='Queue CLI')
    sub = parser.add_subparsers(dest='cmd', required=True)

    # building the parser for enqueue command
    # eg command : python queuectl.py enqueue '{"command": "python -c \"print(123)\"", "max_retries": 3}'
    p_enq = sub.add_parser('enqueue', help='enqueue a command')
    p_enq.add_argument('payload', nargs=argparse.REMAINDER)
    p_enq.add_argument('--retries', type=int, default=3)
    p_enq.set_defaults(func=cmd_enqueue)

    # building the parser for list command
    # eg command : python queuectl.py list --state pending
    p_list = sub.add_parser('list', help='list jobs')
    p_list.add_argument('--state', choices=['pending','processing','completed','failed','dead'], required=False)
    p_list.set_defaults(func=cmd_list)

    # status command
    p_status = sub.add_parser('status', help='show counts and active workers')
    p_status.set_defaults(func=cmd_status)

    # building the parser for dead letter queue command
    # eg command : python queuectl.py dlq list
    p_dlq = sub.add_parser('dlq', help='dead letter queue ops')
    p_dlq.add_argument('action', choices=['list','retry'])
    p_dlq.add_argument('job_id', nargs='?')
    p_dlq.set_defaults(func=cmd_dlq)

    # building the parser for worker command
    # eg command : python queuectl.py worker start
    p_worker = sub.add_parser('worker', help='start/stop workers')
    p_worker.add_argument('action', choices=['start','stop'])
    p_worker.add_argument('--count', nargs='?', const=1, type=int, default=1)
    p_worker.add_argument('--backoff', type=int, default=2)
    p_worker.set_defaults(func=cmd_worker)      

    # building the parser for config command
    # eg command : python queuectl.py config set max_retries 3
    # eg command : python queuectl.py config get max_retries
    p_cfg = sub.add_parser('config', help='configuration')
    p_cfg.add_argument('cfg_action', choices=['set','get'])
    p_cfg.add_argument('key')
    p_cfg.add_argument('value', nargs='?')
    p_cfg.set_defaults(func=cmd_config)

    # history command
    p_hist = sub.add_parser('history', help='show job/event history')
    p_hist.add_argument('--job-id', type=int, required=False)
    # history now prints job records; keep args for forward compatibility but they are ignored
    p_hist.add_argument('--limit', type=int, default=100)
    p_hist.add_argument('--all', action='store_true')
    p_hist.add_argument('--since', type=str, required=False)
    p_hist.add_argument('--until', type=str, required=False)
    p_hist.add_argument('--order', type=str, choices=['asc','desc'], default='desc')
    p_hist.set_defaults(func=cmd_history)
    

    return parser


# main function

def main(argv=None):

    parser = build_parser()
    args = parser.parse_args(argv)
    if hasattr(args, 'func'):
        args.func(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()


