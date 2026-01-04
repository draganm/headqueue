-- name: InsertBlock :exec
INSERT INTO blocks (number, hash, parent, block, receipts, call_traces, prestate_traces, keccak256_preimage_traces, state_access_traces)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);


-- name: GetBlockCount :one
SELECT COUNT(*) FROM blocks;

