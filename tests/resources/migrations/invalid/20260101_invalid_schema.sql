CREATE TABLE IF NOT EXISTS xx (
    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
    target TEXT,
    datastore_id INTEGER,
    status TEXT,
    created_at TIMESTAMP:....
    created_by TEXT,
    parameters TEXT,
    FOREIGN KEY (datastore_id) REFERENCES datastore (datastore_id)
);

