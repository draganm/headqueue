package headqueue

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/draganm/headqueue/sqlitestore"
)

type HeadQueue struct {
	db        *sql.DB
	maxBlocks int
}

func NewHeadQueue(
	db *sql.DB,
	maxBlocks int,
) *HeadQueue {
	return &HeadQueue{
		db:        db,
		maxBlocks: maxBlocks,
	}
}

func (hq *HeadQueue) Enqueue(ctx context.Context, block *sqlitestore.Block) error {

	// wait for space to free up
	{
		queries := sqlitestore.New(hq.db)

		for {
			blockCount, err := queries.GetBlockCount(ctx)
			if err != nil {
				return err
			}
			if blockCount < int64(hq.maxBlocks) {
				break
			}
			time.Sleep(1 * time.Second)
		}
	}

	// TODO: implement backoff

	tx, err := hq.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	queries := sqlitestore.New(tx)
	err = queries.InsertBlock(ctx, sqlitestore.InsertBlockParams{
		Number:                  block.Number,
		Hash:                    block.Hash,
		Parent:                  block.Parent,
		Block:                   block.Block,
		Receipts:                block.Receipts,
		CallTraces:              block.CallTraces,
		PrestateTraces:          block.PrestateTraces,
		Keccak256PreimageTraces: block.Keccak256PreimageTraces,
		StateAccessTraces:       block.StateAccessTraces,
	})
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (hq *HeadQueue) PeekQueue(ctx context.Context, lastID int64) ([]sqlitestore.Block, error) {
	// TODO: implement backoff when locked
	queries := sqlitestore.New(hq.db)
	blocks, err := queries.GetBlocksAfter(ctx, int64(lastID))
	if err != nil {
		return nil, err
	}

	return blocks, nil

}

func (hq *HeadQueue) DropTail(ctx context.Context, id int64) error {
	// TODO: implement backoff when locked
	queries := sqlitestore.New(hq.db)
	err := queries.DeleteBlocksBeforeIncluding(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to delete blocks before including %d: %w", id, err)
	}
	return nil
}
