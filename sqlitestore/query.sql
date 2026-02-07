-- name: InsertBlock :exec
INSERT INTO blocks (
        number,
        hash,
        parent,
        payload
    )
VALUES (?, ?, ?, ?);

-- name: GetBlocksAfter :many
SELECT id,
    number,
    hash,
    parent,
    payload
FROM blocks
WHERE id > ?
LIMIT ?;

-- name: GetBlockCount :one
SELECT COUNT(*)
FROM blocks;

-- name: DeleteBlocksBeforeIncluding :exec
DELETE FROM blocks WHERE id <= ?;