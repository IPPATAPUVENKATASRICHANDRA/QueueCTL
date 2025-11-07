## queuectl

This is a minimal cli-based background job queue with sqlite persistence, retries with exponential backoff, a dead-letter queue, and worker management. This work is an Assignment given by FLAM for backend development.

### setup

- requirements: python 3.10+
- no external dependencies
- database file `queuectl.db` is created automatically in the project directory

run commands from the project root:

```bash
python queuectl.py -h
```

### files

The `queuectl.py` file is the command-line entry point. It parses subcommands (enqueue, worker start|stop, status, list, dlq list|retry, config set|get, history), validates inputs, invokes the storage and worker modules, and prints user-facing output such as counts, listings, and JSON history lines.

The `storage.py` file contains the SQLite data layer and manages the `queuectl.db` database. It creates and maintains the `jobs`, `config`, `workers`, and `events` tables, and stores an optional `external_id` for jobs when an `id` is provided at enqueue time. It exposes functions to add, list, and get jobs, retry dead jobs, track worker registration and heartbeats, set and get configuration values, and record/read lifecycle events. It also provides the primitives the worker uses to prevent duplicate processing.

The `worker.py` file implements the background worker loop that runs in threads. A worker atomically claims one pending job, executes the command, retries with exponential backoff on failure, and moves the job to the DLQ after the retry limit. It records lifecycle events and heartbeats and respects the `workers_should_stop` flag to shut down gracefully after finishing work.

The `testing.py` file is an end-to-end test script that exercises the main flows. It verifies config set/get, worker startup, success and failure paths, DLQ list/retry, list by state, history output, status, graceful stop, and basic parallel processing. It can keep or reset the database using the `KEEP_DB` environment variable.


### Imports used are:

- argparse - for parsing command line arguments
- sqlite3 - for database operations
- uuid - for generating unique ids
- threading - for running workers in threads
- os - for getting the process id
- subprocess - for executing commands
- time - for sleeping
- sys - for exiting the program
- json - for parsing json
- shutil - for copying files


### usage examples

- enqueue a job (json payload)

```bash
python queuectl.py enqueue '{"id":"job1","command":"echo Hello World"}'
# output: enqueued 1
```

- start workers (threads)

```bash
python queuectl.py worker start --count 2
# press Ctrl+C to stop the foreground launcher, or run the stop command below
```

- stop workers gracefully

```bash
python queuectl.py worker stop
# output: signaled workers to stop
```

- status

```bash
python queuectl.py status
# output example:
# Jobs:
#   pending: 0
#   processing: 0
#   completed: 1
#   failed: 0
#   dead: 0
# Active workers: 2
```

- list jobs by state

```bash
python queuectl.py list --state pending
# output example:
# Jobs (pending):
#   3	pending	attempts=0/3	cmd=python -c "print(456)"
```

- dead letter queue (dlq)

```bash
python queuectl.py dlq list
# output example:
# 5	dead	cmd=python -c "import sys; sys.exit(2)"

python queuectl.py dlq retry 5
# output: retried 5
```

- configuration

```bash
python queuectl.py config set max_retries 3
python queuectl.py config get max_retries
# output: 3
```

- history (events)

```bash
# recent events
python queuectl.py history
# output example:
# {"id": "1", "command": "(command:python-c\"print(789)\"]", "state": "dead", "attempts": 2, "max_retries": 3, "created_at": "2025-11-06T14:36:592", "updated_at": "2025-11-06T14:37 :08Z")
# (" id": "2", "command": "[id:job_invalid,command:not_a_real_command,max_retries:2]", "state": "dead", "attempts": 2, "max retries": 3, "created at": "2025-11-06T14:37:247", "updated
# _at": "2025-11-06T14:37:512"}
# events for a specific job

python queuectl.py history --job-id 1
# output example:
# {"id": "1", "command": "(command:python-c\"print(789)\"]", "state": "dead", "attempts": 2, "max_retries": 3, "created_at": "2025-11-06T14:36:592", "updated_at": "2025-11-06T14:37:08Z")

```

### architecture overview

queuectl saves all its data in a local SQLite database file called queuectl.db.

The jobs table keeps one record per job, including the command to run, its state (like pending or completed), how many times it was tried, the retry limit, and timestamps.

The config table stores key-value settings such as max_retries, backoff, and workers_should_stop.

The workers table keeps track of each worker’s ID, process ID, and last heartbeat to know which workers are currently active.

The jobs table also stores an optional external_id when you pass an "id" field in the enqueue JSON. This lets you reference jobs by your own string IDs (e.g., dlq retry job1). In addition, an events table records job lifecycle events (enqueued, processing, retry_scheduled, completed, dead, dlq_retry) that power the history command.

Workers run in a loop:

Each worker picks one pending job and marks it as “processing” so no other worker can take it.

It runs the job’s command using subprocess.

If the job succeeds (exit code 0), it’s marked as “completed.”

If it fails, the worker increases the attempt count and either retries the job after a delay (using exponential backoff) or marks it as “dead” if the retry limit is reached.

Each worker updates its heartbeat regularly and checks if it should stop.

Multiple workers can run at the same time using threads.


it is a simple tool that allows us to:

Add new jobs using enqueue.

Start or stop workers with worker start|stop.

Check system status with status.

List jobs by state using list --state.

View or retry dead jobs with dlq list and dlq retry <id>.

Read or change settings with config set|get.

### assumptions and trade-offs

workers run as threads within the launcher process. this is simple and portable. if processes are required, the start logic can be switched to `multiprocessing`.
the schema uses `max_retires` (as named in code) for retry limit.
a `failed` state is included in counts for completeness, but current flow sets `completed`, `pending`, `processing`, and `dead`.
enqueue accepts a json payload and stores `command` and retry limit; any provided `id` in payload is not stored, an autoincrement integer id is used instead.


## The working video link:

https://drive.google.com/file/d/19tIR5QaXm4VsM5XKsNqxeIhuZgVQvHXC/view?usp=sharing


### testing instructions

- basic success

```bash
python queuectl.py enqueue '{"command":"python -c \"print(123)\""}'
python queuectl.py worker start --count 1
# in another terminal
python queuectl.py status
```

- retry and dlq

```bash
python queuectl.py enqueue '{"command":"python -c \"import sys; sys.exit(2)\"","max_retries":2}'
python queuectl.py worker start --count 1
python queuectl.py dlq list
```

- multiple workers and no overlap

```bash
python queuectl.py enqueue '{"command":"python -c \"import time; time.sleep(2); print(\"done\")\""}'
python queuectl.py enqueue '{"command":"python -c \"import time; time.sleep(2); print(\"done\")\""}'
python queuectl.py worker start --count 2
```

- persistence across restart

```bash
python queuectl.py enqueue '{"command":"python -c \"print(789)\""}'
# close and reopen the terminal; job remains in sqlite
python queuectl.py status
```

- config

```bash
python queuectl.py config set backoff 2
python queuectl.py config get backoff
```

- automated test script

```bash
# run full end-to-end checks (config, enqueue, retries/DLQ, list, history, status, stop)
python testing.py
```


