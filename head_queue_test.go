package headqueue

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/draganm/headqueue/sqlitestore"
	_ "github.com/mattn/go-sqlite3"
)

const schema = `
CREATE TABLE IF NOT EXISTS blocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    number BIGINT,
    hash BLOB NOT NULL,
    parent BLOB NOT NULL,
    block BLOB,
    receipts BLOB,
    call_traces BLOB,
    prestate_traces BLOB,
    keccak256_preimage_traces BLOB,
    state_access_traces BLOB
);
`

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := fmt.Sprintf("file:%s/test.db?_journal_mode=WAL", tmpDir)

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	_, err = db.Exec(schema)
	if err != nil {
		db.Close()
		t.Fatalf("failed to create schema: %v", err)
	}

	t.Cleanup(func() {
		db.Close()
	})

	return db
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

	hq := NewHeadQueue(db, maxBlocks)

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
	db := setupTestDB(t)
	hq := NewHeadQueue(db, 10)
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
	db := setupTestDB(t)
	hq := NewHeadQueue(db, 10)
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
	db := setupTestDB(t)
	hq := NewHeadQueue(db, 10)
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
	db := setupTestDB(t)
	hq := NewHeadQueue(db, 10)
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
	db := setupTestDB(t)
	hq := NewHeadQueue(db, 10)
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
	db := setupTestDB(t)
	hq := NewHeadQueue(db, 2)
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
	var wg sync.WaitGroup
	var enqueueErr error
	enqueueStarted := make(chan struct{})
	enqueueDone := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		close(enqueueStarted)
		block := createTestBlock(3)
		enqueueErr = hq.Enqueue(ctx, block)
		close(enqueueDone)
	}()

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

	wg.Wait()

	if enqueueErr != nil {
		t.Errorf("Enqueue returned error: %v", enqueueErr)
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
	db := setupTestDB(t)
	hq := NewHeadQueue(db, 1)
	ctx := context.Background()

	// Fill the queue
	block := createTestBlock(1)
	err := hq.Enqueue(ctx, block)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Try to enqueue with a cancellable context
	cancelCtx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	var enqueueErr error
	enqueueStarted := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		close(enqueueStarted)
		block := createTestBlock(2)
		enqueueErr = hq.Enqueue(cancelCtx, block)
	}()

	<-enqueueStarted

	// Give the goroutine time to start blocking
	time.Sleep(100 * time.Millisecond)

	// Cancel the context
	cancel()

	// Wait for enqueue to return
	wg.Wait()

	if enqueueErr == nil {
		t.Error("expected error from cancelled context, got nil")
	}
	if enqueueErr != context.Canceled {
		t.Errorf("expected context.Canceled error, got: %v", enqueueErr)
	}
}
