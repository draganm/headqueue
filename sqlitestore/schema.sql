CREATE TABLE IF NOT EXISTS blocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    number BIGINT,
    hash BLOB NOT NULL,
    parent BLOB NOT NULL,
    payload BLOB
);

