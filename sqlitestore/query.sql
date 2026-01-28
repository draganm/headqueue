-- name: InsertBlock :exec
INSERT INTO blocks (
        number,
        hash,
        parent,
        block,
        receipts,
        call_traces,
        prestate_traces,
        keccak256_preimage_traces,
        state_access_traces
    )
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetBlocksAfter :many
SELECT id,
    number,
    hash,
    parent,
    block,
    receipts,
    call_traces,
    prestate_traces,
    keccak256_preimage_traces,
    state_access_traces
FROM blocks
WHERE id > ?
LIMIT ?;

-- name: GetBlockCount :one
SELECT COUNT(*)
FROM blocks;

-- name: DeleteBlocksBeforeIncluding :exec
DELETE FROM blocks WHERE id <= ?;