CREATE TABLE IF NOT EXISTS datastore (
    datastore_id INTEGER PRIMARY KEY AUTOINCREMENT,
    rdn TEXT,
    description TEXT,
    directory TEXT,
    name TEXT,
    bump_enabled BOOLEAN DEFAULT FALSE
);