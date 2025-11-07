
# worker management

import threading
import time
import subprocess
import os
import uuid
import storage
from queue import Queue


# function to get the next job and mark it as processing
def func_next_job():
    storage.make_db()
    conn, cur = storage.connect_db()
    try:
        cur.execute('BEGIN IMMEDIATE')
        cur.execute(
            "SELECT id, command, attempts, max_retires FROM jobs WHERE state='pending' ORDER BY created_at LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            conn.commit()
            return None
        job_id, command, attempts, max_retires = row
        cur.execute(
            "UPDATE jobs SET state='processing', updated_at=CURRENT_TIMESTAMP WHERE id=? AND state='pending'",
            (job_id,),
        )
        if cur.rowcount != 1:
            conn.rollback()
            return None
        conn.commit()
        return {
            'id': job_id,
            'command': command,
            'attempts': attempts,
            'max_retires': max_retires,
        }
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    finally:
        conn.close()


# function for marking the job as completed
def func_mark_complete(job_id: int):

    conn, cur = storage.connect_db()
    try:
        cur.execute(
            "UPDATE jobs SET state='completed', updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (job_id,),
        )
        conn.commit()
    finally:
        conn.close()


# function for marking the job as dead
def func_mark_dead(job_id: int):

    conn, cur = storage.connect_db()
    try:
        cur.execute(
            "UPDATE jobs SET state='dead', updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (job_id,),
        )
        conn.commit()
    finally:
        conn.close()


# function for requeuing the job with the next attempt
def func_requeue_with_attempt(job_id: int, next_attempts: int):
    conn, cur = storage.connect_db()
    try:
        cur.execute(
            "UPDATE jobs SET state='pending', attempts=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (next_attempts, job_id),
        )
        conn.commit()
    finally:
        conn.close()


# function for executing the command
def func_execute_command(command: str) -> bool:
    cmd = (command or '').strip()
    if len(cmd) >= 2 and ((cmd[0] == cmd[-1] == '"') or (cmd[0] == cmd[-1] == "'")):
        cmd = cmd[1:-1]
    try:
        result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return result.returncode == 0
    except Exception:
        return False


# function for the worker loop
def worker_loop(poll_interval: float = 1.0, backoff_base: int = 2, worker_id: str | None = None):
    storage.make_db()
    # generating a unique worker id
    worker_id = worker_id or f"{os.getpid()}-{uuid.uuid4().hex[:6]}-{threading.get_ident()}"
    storage.register_worker(worker_id, os.getpid())
    while True:
        storage.timestamp_worker(worker_id, 'running')
        # Check global stop flag from config; if set, finish pending work and exit when idle
        stop_flag = storage.get_config('workers_should_stop', '0') == '1'
        # getting the next job from the database
        job = func_next_job()
        if not job:
            if stop_flag:
                storage.timestamp_worker(worker_id, 'stopped')
                break
            time.sleep(poll_interval)
            continue

        # getting the job id, command, attempts, max_retires from the job
        job_id = job['id']
        command = job['command']
        attempts = int(job['attempts'] or 0)
        max_retires = int(job['max_retires'] or 3)

        # executing the command
        print(f"worker {worker_id} processing job {job_id} (attempt {attempts + 1}/{max_retires})")
        try:
            storage.add_event(job_id, 'processing', f'worker={worker_id}')
        except Exception:
            pass
        result = func_execute_command(command)
        if result:
            func_mark_complete(job_id)
            print(f"worker {worker_id} completed job {job_id}")
            try:
                storage.add_event(job_id, 'completed', None)
            except Exception:
                pass
            continue

        # if the command failed, then we are marking the job as dead only if the attempts are greater than or equal to the max_retires
        next_attempts = attempts + 1
        if next_attempts >= max_retires:
            func_mark_dead(job_id)
            print(f"worker {worker_id} moved job {job_id} to DLQ")
            try:
                storage.add_event(job_id, 'dead', None)
            except Exception:
                pass
            continue

        # getting the backoff value from the config
        cfg_backoff = storage.get_config('backoff', None)
        # if the backoff value is not found, then we are using the default backoff value
        base = int(cfg_backoff) if cfg_backoff is not None else backoff_base
        # calculating the delay
        delay = base ** next_attempts
        print(f"worker {worker_id} retrying job {job_id} in {delay}s (attempt {next_attempts}/{max_retires})")
        # sleeping for the delay to avoid duplicate processing
        time.sleep(delay)
        func_requeue_with_attempt(job_id, next_attempts)
        try:
            storage.add_event(job_id, 'retry_scheduled', f'attempts={next_attempts}, delay={delay}')
        except Exception:
            pass
        storage.timestamp_worker(worker_id, 'running')


# function for starting the background worker
def func_start_background_worker(poll_interval: float = 1.0, backoff_base: int = 2):

    # creating a new thread for the worker
    wid = f"{os.getpid()}-{uuid.uuid4().hex[:6]}"
    t = threading.Thread(target=worker_loop, kwargs={
        'poll_interval': poll_interval,
        'backoff_base': backoff_base,
        'worker_id': wid,
    }, daemon=True)
    t.start()
    return t, wid

