CREATE TABLE IF NOT EXISTS blocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    number BIGINT PRIMARY KEY,
    hash BLOB NOT NULL,
    parent BLOB NOT NULL,
    block BLOB,
    receipts BLOB,
    call_traces BLOB,
    prestate_traces BLOB,
    keccak256_preimage_traces BLOB,
    state_access_traces BLOB
);

