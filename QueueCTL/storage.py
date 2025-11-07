import sqlite3

dp_path='queuectl.db'

# function to make a database
# 3 db : jobs, config, workers
def make_db():
   
    conn=sqlite3.connect(dp_path)
    cur=conn.cursor()
    # jobs : to store the jobs in the database
    cur.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            command TEXT NOT NULL,
            state TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_retires INTEGER NOT NULL DEFAULT 3,
            created_at DateTime NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DateTime NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # config : to store the config values in the database
    cur.execute('''
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    ''')
    # workers : to store the workers in the database
    cur.execute('''
        CREATE TABLE IF NOT EXISTS workers (
            worker_id TEXT PRIMARY KEY,
            pid INTEGER,
            started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_heartbeat DATETIME,
            status TEXT
        )
    ''')
    # events : to store history of job lifecycle
    cur.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            event TEXT NOT NULL,
            detail TEXT,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

    # ensure schema migrations (idempotent)
    _ensure_jobs_external_id()


# function to connect the database
def connect_db():
    conn=sqlite3.connect(dp_path)
    cur=conn.cursor()
    return conn, cur


def _ensure_jobs_external_id():
    conn, cur = connect_db()
    try:
        cur.execute("PRAGMA table_info(jobs)")
        cols = [r[1] for r in cur.fetchall()]
        if 'external_id' not in cols:
            try:
                cur.execute("ALTER TABLE jobs ADD COLUMN external_id TEXT")
                conn.commit()
            except Exception:
                pass
    finally:
        conn.close()


# function to add a job to the database
def add_job(command, state='pending', max_retires=3, external_id=None):
    try:
        
        conn, cur=connect_db()
        # inserting the job into the database
        _ensure_jobs_external_id()
        cur.execute('INSERT INTO jobs (command, state, max_retires, external_id) VALUES (?, ?, ?, ?)', (command, state, max_retires, external_id))
        conn.commit()
        job_id = cur.lastrowid
        conn.close()
        return job_id
    except sqlite3.Error as e:
        print(f"Error adding job: {e}")
        try:
            conn.close()
        except Exception:
            pass
        return None

# function to list the jobs in the database
def list_jobs(state=None):
    conn, cur = connect_db()
    try:
        # if the state is provided, then we are filtering the jobs by the state
        # otherwise, we are listing all the jobs
        if state:
            cur.execute('SELECT id, command, state, attempts, max_retires, created_at, updated_at FROM jobs WHERE state=? ORDER BY created_at', (state,))
        else:
            cur.execute('SELECT id, command, state, attempts, max_retires, created_at, updated_at FROM jobs ORDER BY created_at')
        rows = cur.fetchall()
        return rows
    finally:
        conn.close()

# function to get a job from the database
# job_id: the id of the job to get
def get_job(job_id: int):
    # connecting to the database
    conn, cur = connect_db()
    try:
        cur.execute('SELECT id, command, state, attempts, max_retires, created_at, updated_at FROM jobs WHERE id=?', (job_id,))
        return cur.fetchone()
    finally:
        conn.close()


def get_job_by_external_id(external_id: str):
    conn, cur = connect_db()
    try:
        cur.execute('SELECT id, command, state, attempts, max_retires, created_at, updated_at FROM jobs WHERE external_id=?', (external_id,))
        return cur.fetchone()
    finally:
        conn.close()

# function to get the counts of the jobs in the database
def counts_by_state():
    conn, cur = connect_db()
    try:
        # counting the jobs by the state
        cur.execute("SELECT state, COUNT(*) FROM jobs GROUP BY state")
        rows = cur.fetchall()
        counts = {'pending': 0, 'processing': 0, 'completed': 0, 'failed': 0, 'dead': 0}
        # counting the jobs by the state
        for state, cnt in rows:
            counts[state] = cnt
        return counts
    finally:
        conn.close()


# function to retry a job in the dead letter queue
def retry_dead(job_id: int):
    conn, cur = connect_db()
    try:
        # updating the job in the database
        cur.execute("UPDATE jobs SET state='pending', attempts=0, updated_at=CURRENT_TIMESTAMP WHERE id=? AND state='dead'", (job_id,))
        conn.commit()
        return cur.rowcount == 1
    finally:
        conn.close()


def retry_dead_by_identifier(identifier: str):
    """Retry a dead job by numeric id or external_id string."""
    # try numeric id first
    try:
        jid = int(identifier)
        return retry_dead(jid)
    except Exception:
        pass
    # fallback to external id
    row = get_job_by_external_id(identifier)
    if not row:
        return False
    jid = int(row[0])
    return retry_dead(jid)


def list_dead_jobs_with_external():
    conn, cur = connect_db()
    try:
        _ensure_jobs_external_id()
        cur.execute("SELECT id, external_id, command FROM jobs WHERE state='dead' ORDER BY updated_at DESC, id DESC")
        return cur.fetchall()
    finally:
        conn.close()

# function to set a config value in the database

def set_config(key: str, value: str):

    conn, cur = connect_db()
    try:
        cur.execute("INSERT INTO config(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
        conn.commit()
    finally:
        conn.close()

# function to get a config value from the database
def get_config(key: str, default: str | None = None):
    conn, cur = connect_db()
    try:
        # getting the config value from the database
        cur.execute("SELECT value FROM config WHERE key=?", (key,))
        row = cur.fetchone()
        return row[0] if row else default
    finally:
        conn.close()


# function to register a worker in the database

def register_worker(worker_id: str, pid: int):
    conn, cur = connect_db()
    try:
        # registering the worker in the database with the worker_id, pid, started_at, last_heartbeat, status
        cur.execute(
            "INSERT INTO workers(worker_id,pid,started_at,last_heartbeat,status) VALUES(?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'running')\n"
            "ON CONFLICT(worker_id) DO UPDATE SET pid=excluded.pid, last_heartbeat=excluded.last_heartbeat, status='running'",
            (worker_id, pid),
        )
        conn.commit()
    finally:
        conn.close()


# function to update the time stap of the worker
def timestamp_worker(worker_id: str, status: str = 'running'):

    conn, cur = connect_db()
    try:
        # updating the worker in the database with the last_heartbeat, status
        cur.execute("UPDATE workers SET last_heartbeat=CURRENT_TIMESTAMP, status=? WHERE worker_id=?", (status, worker_id))
        conn.commit()
    finally:
        conn.close()


# function to count the active workers in the database
def count_active_workers(threshold_seconds: int = 10) -> int:
    conn, cur = connect_db()
    try:
        # Only count workers that are currently running and have a fresh heartbeat
        cur.execute("SELECT COUNT(*) FROM workers WHERE status='running' AND last_heartbeat >= datetime('now', ?)", (f'-{threshold_seconds} seconds',))
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


# function to add an event to the events table
def add_event(job_id: int, event: str, detail: str | None = None):
    conn, cur = connect_db()
    try:
        # adding the event to the database with the job_id, event, detail
        cur.execute('INSERT INTO events(job_id, event, detail) VALUES(?,?,?)', (job_id, event, detail))
        conn.commit()
    finally:
        conn.close()


# function to list events (optionally for a single job)
def list_events(job_id: int | None = None, limit: int | None = 100, since: str | None = None, until: str | None = None, order: str = 'desc'):
    conn, cur = connect_db()
    try:
        # creating a list of clauses and parameters
        clauses = []
        params = []
        if job_id is not None:
            clauses.append('job_id = ?')
            params.append(job_id)
        # if the since is provided, then we are filtering the events by the created_at
        if since:
            clauses.append('created_at >= ?')
            params.append(since)
        # if the until is provided, then we are filtering the events by the created_at
        if until:
            clauses.append('created_at <= ?')
            params.append(until)
        where_sql = ('WHERE ' + ' AND '.join(clauses)) if clauses else ''
        # if the order is provided, then we are ordering the events by the created_at
        ord = 'ASC' if str(order).lower() == 'asc' else 'DESC'
        sql = f'SELECT id, job_id, event, detail, created_at FROM events {where_sql} ORDER BY created_at {ord}, id {ord}'
        # if the limit is provided, then we are limiting the events by the limit
        if isinstance(limit, int) and limit > 0:
            sql += ' LIMIT ?'
            params.append(limit)
        # executing the sql query
        cur.execute(sql, tuple(params))
        return cur.fetchall()
    finally:
        conn.close()


