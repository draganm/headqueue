package headqueue

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"strings"
	"time"

	"github.com/draganm/headqueue/sqlitestore"
)

//go:embed sqlitestore/schema.sql
var schema string

type HeadQueue struct {
	db        *sql.DB
	maxBlocks int
}

func NewHeadQueue(
	db *sql.DB,
	maxBlocks int,
) (*HeadQueue, error) {
	_, err := db.Exec(schema)
	if err != nil {
		return nil, err
	}

	return &HeadQueue{
		db:        db,
		maxBlocks: maxBlocks,
	}, nil
}

var ErrMaxBlocksBuffered = errors.New("max blocks buffered")

func (hq *HeadQueue) Enqueue(ctx context.Context, block *sqlitestore.Block) error {

	queries := sqlitestore.New(hq.db)
	for {

		err := func() error {

			blockCount, err := queries.GetBlockCount(ctx)
			if err != nil {
				return err
			}

			if blockCount >= int64(hq.maxBlocks) {
				return ErrMaxBlocksBuffered
			}

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
		}()

		switch {
		case err == ErrMaxBlocksBuffered, err != nil && strings.Contains(err.Error(), "database is locked"):
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(1 * time.Second):
				continue
			}
		default:
			return err
		}

	}
}

func (hq *HeadQueue) PeekQueue(ctx context.Context, lastID int64, limit int64) ([]sqlitestore.Block, error) {
	queries := sqlitestore.New(hq.db)

	for {
		blocks, err := queries.GetBlocksAfter(
			ctx, sqlitestore.GetBlocksAfterParams{
				ID:    lastID,
				Limit: limit,
			})

		switch {
		case err != nil && strings.Contains(err.Error(), "database is locked"):
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(1 * time.Second):
				continue
			}
		case err != nil:
			return nil, err
		default:
			return blocks, nil
		}
	}
}

func (hq *HeadQueue) DropTail(ctx context.Context, id int64) error {
	queries := sqlitestore.New(hq.db)

	for {
		err := queries.DeleteBlocksBeforeIncluding(ctx, id)

		switch {
		case err != nil && strings.Contains(err.Error(), "database is locked"):
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(1 * time.Second):
				continue
			}
		default:
			return err
		}
	}
}
