CREATE TABLE IF NOT EXISTS job (
    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
    target TEXT,
    datastore_id INTEGER,
    status TEXT,
    created_at TIMESTAMP,
    created_by TEXT,
    parameters TEXT,
    FOREIGN KEY (datastore_id) REFERENCES datastore (datastore_id)
);

CREATE TABLE IF NOT EXISTS maintenance (
    maintenance_id INTEGER PRIMARY KEY AUTOINCREMENT,
    msg TEXT,
    paused BOOLEAN,
    timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS target (
    name TEXT,
    datastore_id INTEGER,
    status TEXT,
    action TEXT,
    last_updated_at TIMESTAMP,
    last_updated_by TEXT,
    PRIMARY KEY (name, datastore_id) FOREIGN KEY (datastore_id) REFERENCES datastore (datastore_id)
);

CREATE TABLE IF NOT EXISTS job_log (
    job_log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    msg TEXT,
    at TIMESTAMP,
    FOREIGN KEY (job_id) REFERENCES job (job_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS datastore (
    datastore_id INTEGER PRIMARY KEY AUTOINCREMENT,
    rdn TEXT,
    description TEXT,
    directory TEXT,
    name TEXT,
    bump_enabled BOOLEAN DEFAULT FALSE
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_rdn ON datastore (rdn);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dir ON datastore (directory);