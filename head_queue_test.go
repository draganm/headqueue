package headqueue

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/draganm/headqueue/sqlitestore"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/sync/errgroup"
)

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := fmt.Sprintf("file:%s/test.db?_journal_mode=WAL", tmpDir)

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	t.Cleanup(func() {
		db.Close()
	})

	return db
}

func setupTestQueue(t *testing.T, maxBlocks int) (*sql.DB, *HeadQueue) {
	t.Helper()
	db := setupTestDB(t)
	hq, err := NewHeadQueue(db, maxBlocks)
	if err != nil {
		t.Fatalf("failed to create HeadQueue: %v", err)
	}
	return db, hq
}

func createTestBlock(number int64) *sqlitestore.Block {
	return &sqlitestore.Block{
		Number:                  number,
		Hash:                    []byte(fmt.Sprintf("hash-%d", number)),
		Parent:                  []byte(fmt.Sprintf("parent-%d", number)),
		Block:                   []byte(fmt.Sprintf("block-%d", number)),
		Receipts:                []byte(fmt.Sprintf("receipts-%d", number)),
		CallTraces:              []byte(fmt.Sprintf("call-traces-%d", number)),
		PrestateTraces:          []byte(fmt.Sprintf("prestate-traces-%d", number)),
		Keccak256PreimageTraces: []byte(fmt.Sprintf("keccak256-preimage-traces-%d", number)),
		StateAccessTraces:       []byte(fmt.Sprintf("state-access-traces-%d", number)),
	}
}

func TestNewHeadQueue(t *testing.T) {
	db := setupTestDB(t)
	maxBlocks := 10

	hq, err := NewHeadQueue(db, maxBlocks)
	if err != nil {
		t.Fatalf("NewHeadQueue failed: %v", err)
	}

	if hq == nil {
		t.Fatal("NewHeadQueue returned nil")
	}
	if hq.db != db {
		t.Error("db field not set correctly")
	}
	if hq.maxBlocks != maxBlocks {
		t.Errorf("maxBlocks = %d, want %d", hq.maxBlocks, maxBlocks)
	}
}

func TestEnqueue_SingleBlock(t *testing.T) {
	_, hq := setupTestQueue(t, 10)
	ctx := context.Background()

	block := createTestBlock(1)
	err := hq.Enqueue(ctx, block)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	blocks, err := hq.PeekQueue(ctx, 0)
	if err != nil {
		t.Fatalf("PeekQueue failed: %v", err)
	}

	if len(blocks) != 1 {
		t.Fatalf("expected 1 block, got %d", len(blocks))
	}

	if blocks[0].Number != block.Number {
		t.Errorf("block number = %d, want %d", blocks[0].Number, block.Number)
	}
	if string(blocks[0].Hash) != string(block.Hash) {
		t.Errorf("block hash = %s, want %s", blocks[0].Hash, block.Hash)
	}
}

func TestEnqueue_MultipleBlocks(t *testing.T) {
	_, hq := setupTestQueue(t, 10)
	ctx := context.Background()

	for i := int64(1); i <= 5; i++ {
		block := createTestBlock(i)
		err := hq.Enqueue(ctx, block)
		if err != nil {
			t.Fatalf("Enqueue block %d failed: %v", i, err)
		}
	}

	blocks, err := hq.PeekQueue(ctx, 0)
	if err != nil {
		t.Fatalf("PeekQueue failed: %v", err)
	}

	if len(blocks) != 5 {
		t.Fatalf("expected 5 blocks, got %d", len(blocks))
	}

	for i, block := range blocks {
		expectedNumber := int64(i + 1)
		if block.Number != expectedNumber {
			t.Errorf("block[%d].Number = %d, want %d", i, block.Number, expectedNumber)
		}
	}
}

func TestPeekQueue_EmptyQueue(t *testing.T) {
	_, hq := setupTestQueue(t, 10)
	ctx := context.Background()

	blocks, err := hq.PeekQueue(ctx, 0)
	if err != nil {
		t.Fatalf("PeekQueue failed: %v", err)
	}

	if len(blocks) != 0 {
		t.Errorf("expected empty slice, got %d blocks", len(blocks))
	}
}

func TestPeekQueue_WithLastID(t *testing.T) {
	_, hq := setupTestQueue(t, 10)
	ctx := context.Background()

	for i := int64(1); i <= 5; i++ {
		block := createTestBlock(i)
		err := hq.Enqueue(ctx, block)
		if err != nil {
			t.Fatalf("Enqueue block %d failed: %v", i, err)
		}
	}

	// Get blocks after ID 2 (should return blocks with ID 3, 4, 5)
	blocks, err := hq.PeekQueue(ctx, 2)
	if err != nil {
		t.Fatalf("PeekQueue failed: %v", err)
	}

	if len(blocks) != 3 {
		t.Fatalf("expected 3 blocks, got %d", len(blocks))
	}

	// Verify IDs are 3, 4, 5
	expectedIDs := []int64{3, 4, 5}
	for i, block := range blocks {
		if block.ID != expectedIDs[i] {
			t.Errorf("block[%d].ID = %d, want %d", i, block.ID, expectedIDs[i])
		}
	}
}

func TestDropTail(t *testing.T) {
	_, hq := setupTestQueue(t, 10)
	ctx := context.Background()

	for i := int64(1); i <= 5; i++ {
		block := createTestBlock(i)
		err := hq.Enqueue(ctx, block)
		if err != nil {
			t.Fatalf("Enqueue block %d failed: %v", i, err)
		}
	}

	// Drop blocks with ID <= 3
	err := hq.DropTail(ctx, 3)
	if err != nil {
		t.Fatalf("DropTail failed: %v", err)
	}

	blocks, err := hq.PeekQueue(ctx, 0)
	if err != nil {
		t.Fatalf("PeekQueue failed: %v", err)
	}

	if len(blocks) != 2 {
		t.Fatalf("expected 2 blocks remaining, got %d", len(blocks))
	}

	// Verify remaining blocks have ID 4 and 5
	expectedIDs := []int64{4, 5}
	for i, block := range blocks {
		if block.ID != expectedIDs[i] {
			t.Errorf("block[%d].ID = %d, want %d", i, block.ID, expectedIDs[i])
		}
	}
}

func TestEnqueue_BackpressureBlocking(t *testing.T) {
	_, hq := setupTestQueue(t, 2)
	ctx := context.Background()

	// Fill the queue
	for i := int64(1); i <= 2; i++ {
		block := createTestBlock(i)
		err := hq.Enqueue(ctx, block)
		if err != nil {
			t.Fatalf("Enqueue block %d failed: %v", i, err)
		}
	}

	// Try to enqueue a 3rd block - should block
	eg, egCtx := errgroup.WithContext(ctx)
	enqueueStarted := make(chan struct{})
	enqueueDone := make(chan struct{})

	eg.Go(func() error {
		close(enqueueStarted)
		block := createTestBlock(3)
		err := hq.Enqueue(egCtx, block)
		close(enqueueDone)
		return err
	})

	<-enqueueStarted

	// Verify enqueue is blocking (not done after short wait)
	select {
	case <-enqueueDone:
		t.Fatal("Enqueue should be blocking but completed immediately")
	case <-time.After(100 * time.Millisecond):
		// Expected - enqueue is blocking
	}

	// Free up space by dropping the tail
	err := hq.DropTail(ctx, 1)
	if err != nil {
		t.Fatalf("DropTail failed: %v", err)
	}

	// Wait for enqueue to complete
	select {
	case <-enqueueDone:
		// Expected
	case <-time.After(3 * time.Second):
		t.Fatal("Enqueue did not complete after freeing space")
	}

	if err := eg.Wait(); err != nil {
		t.Errorf("Enqueue returned error: %v", err)
	}

	// Verify we have 3 blocks now (IDs 2, 3, and the new one)
	blocks, err := hq.PeekQueue(ctx, 0)
	if err != nil {
		t.Fatalf("PeekQueue failed: %v", err)
	}

	if len(blocks) != 2 {
		t.Errorf("expected 2 blocks, got %d", len(blocks))
	}
}

func TestEnqueue_ContextCancellation(t *testing.T) {
	_, hq := setupTestQueue(t, 1)
	ctx := context.Background()

	// Fill the queue
	block := createTestBlock(1)
	err := hq.Enqueue(ctx, block)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Try to enqueue with a cancellable context
	cancelCtx, cancel := context.WithCancel(context.Background())

	eg := new(errgroup.Group)
	enqueueStarted := make(chan struct{})

	eg.Go(func() error {
		close(enqueueStarted)
		block := createTestBlock(2)
		return hq.Enqueue(cancelCtx, block)
	})

	<-enqueueStarted

	// Give the goroutine time to start blocking
	time.Sleep(100 * time.Millisecond)

	// Cancel the context
	cancel()

	// Wait for enqueue to return
	enqueueErr := eg.Wait()

	if enqueueErr == nil {
		t.Error("expected error from cancelled context, got nil")
	}
	if enqueueErr != context.Canceled {
		t.Errorf("expected context.Canceled error, got: %v", enqueueErr)
	}
}

func TestEnqueue_RetryOnDatabaseLocked(t *testing.T) {
	db, hq := setupTestQueue(t, 10)
	ctx := context.Background()

	// Start a long-running transaction to lock the database
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Execute a write operation to acquire the lock
	_, err = tx.ExecContext(ctx, "INSERT INTO blocks (number, hash, parent) VALUES (999, x'00', x'00')")
	if err != nil {
		tx.Rollback()
		t.Fatalf("failed to insert: %v", err)
	}

	eg := new(errgroup.Group)
	enqueueStarted := make(chan struct{})
	enqueueDone := make(chan struct{})

	eg.Go(func() error {
		close(enqueueStarted)
		block := createTestBlock(1)
		err := hq.Enqueue(ctx, block)
		close(enqueueDone)
		return err
	})

	<-enqueueStarted

	// Give the goroutine time to encounter the lock and start retrying
	time.Sleep(200 * time.Millisecond)

	// Verify enqueue is blocking (retrying)
	select {
	case <-enqueueDone:
		t.Fatal("Enqueue should be retrying but completed immediately")
	default:
		// Expected - enqueue is retrying
	}

	// Release the lock
	tx.Rollback()

	// Wait for enqueue to complete
	select {
	case <-enqueueDone:
		// Expected
	case <-time.After(5 * time.Second):
		t.Fatal("Enqueue did not complete after lock was released")
	}

	if err := eg.Wait(); err != nil {
		t.Errorf("Enqueue returned error: %v", err)
	}

	// Verify the block was inserted
	blocks, err := hq.PeekQueue(ctx, 0)
	if err != nil {
		t.Fatalf("PeekQueue failed: %v", err)
	}

	if len(blocks) != 1 {
		t.Errorf("expected 1 block, got %d", len(blocks))
	}
}

func TestPeekQueue_RetryOnDatabaseLocked(t *testing.T) {
	db, hq := setupTestQueue(t, 10)
	ctx := context.Background()

	// Insert a block first
	block := createTestBlock(1)
	err := hq.Enqueue(ctx, block)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Start a long-running exclusive transaction to lock the database
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Execute a write operation to acquire the lock
	_, err = tx.ExecContext(ctx, "INSERT INTO blocks (number, hash, parent) VALUES (999, x'00', x'00')")
	if err != nil {
		tx.Rollback()
		t.Fatalf("failed to insert: %v", err)
	}

	eg := new(errgroup.Group)
	var peekResult []sqlitestore.Block
	peekStarted := make(chan struct{})
	peekDone := make(chan struct{})

	eg.Go(func() error {
		close(peekStarted)
		var err error
		peekResult, err = hq.PeekQueue(ctx, 0)
		close(peekDone)
		return err
	})

	<-peekStarted

	// Give the goroutine time to encounter the lock
	time.Sleep(200 * time.Millisecond)

	// Release the lock
	tx.Rollback()

	// Wait for peek to complete
	select {
	case <-peekDone:
		// Expected
	case <-time.After(5 * time.Second):
		t.Fatal("PeekQueue did not complete after lock was released")
	}

	if err := eg.Wait(); err != nil {
		t.Errorf("PeekQueue returned error: %v", err)
	}

	if len(peekResult) != 1 {
		t.Errorf("expected 1 block, got %d", len(peekResult))
	}
}

func TestDropTail_RetryOnDatabaseLocked(t *testing.T) {
	db, hq := setupTestQueue(t, 10)
	ctx := context.Background()

	// Insert blocks
	for i := int64(1); i <= 3; i++ {
		block := createTestBlock(i)
		err := hq.Enqueue(ctx, block)
		if err != nil {
			t.Fatalf("Enqueue block %d failed: %v", i, err)
		}
	}

	// Start a long-running transaction to lock the database
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Execute a write operation to acquire the lock
	_, err = tx.ExecContext(ctx, "INSERT INTO blocks (number, hash, parent) VALUES (999, x'00', x'00')")
	if err != nil {
		tx.Rollback()
		t.Fatalf("failed to insert: %v", err)
	}

	eg := new(errgroup.Group)
	dropStarted := make(chan struct{})
	dropDone := make(chan struct{})

	eg.Go(func() error {
		close(dropStarted)
		err := hq.DropTail(ctx, 2)
		close(dropDone)
		return err
	})

	<-dropStarted

	// Give the goroutine time to encounter the lock
	time.Sleep(200 * time.Millisecond)

	// Verify drop is blocking (retrying)
	select {
	case <-dropDone:
		t.Fatal("DropTail should be retrying but completed immediately")
	default:
		// Expected - drop is retrying
	}

	// Release the lock
	tx.Rollback()

	// Wait for drop to complete
	select {
	case <-dropDone:
		// Expected
	case <-time.After(5 * time.Second):
		t.Fatal("DropTail did not complete after lock was released")
	}

	if err := eg.Wait(); err != nil {
		t.Errorf("DropTail returned error: %v", err)
	}

	// Verify only block 3 remains
	blocks, err := hq.PeekQueue(ctx, 0)
	if err != nil {
		t.Fatalf("PeekQueue failed: %v", err)
	}

	if len(blocks) != 1 {
		t.Errorf("expected 1 block remaining, got %d", len(blocks))
	}

	if len(blocks) > 0 && blocks[0].ID != 3 {
		t.Errorf("expected block ID 3, got %d", blocks[0].ID)
	}
}

func TestDropTail_ContextCancellationDuringRetry(t *testing.T) {
	db, hq := setupTestQueue(t, 10)
	ctx := context.Background()

	// Insert blocks
	for i := int64(1); i <= 3; i++ {
		block := createTestBlock(i)
		err := hq.Enqueue(ctx, block)
		if err != nil {
			t.Fatalf("Enqueue block %d failed: %v", i, err)
		}
	}

	// Start a long-running transaction to lock the database
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Execute a write operation to acquire the lock
	_, err = tx.ExecContext(ctx, "INSERT INTO blocks (number, hash, parent) VALUES (999, x'00', x'00')")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	cancelCtx, cancel := context.WithCancel(context.Background())

	eg := new(errgroup.Group)
	dropStarted := make(chan struct{})

	eg.Go(func() error {
		close(dropStarted)
		return hq.DropTail(cancelCtx, 2)
	})

	<-dropStarted

	// Give the goroutine time to start retrying
	time.Sleep(200 * time.Millisecond)

	// Cancel the context
	cancel()

	dropErr := eg.Wait()

	if dropErr == nil {
		t.Error("expected error from cancelled context, got nil")
	}
	if dropErr != context.Canceled {
		t.Errorf("expected context.Canceled error, got: %v", dropErr)
	}
}
