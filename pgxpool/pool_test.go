package pgxpool_test

import (
	"context"
	"testing"
	"time"

	"github.com/muckomdeead/pgx/pgconn"
	"github.com/muckomdeead/pgx/pgxpool"
)

func TestCanceledTxConnectionNotReturnedToIdlePool(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, nil)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("failed to acquire conn: %v", err)
	}

	txCtx, cancel := context.WithCancel(ctx)
	tx, err := conn.Begin(txCtx)
	if err != nil {
		t.Fatalf("failed to begin tx: %v", err)
	}

	// Cancel context during transaction execution
	cancel()
	_ = tx.Exec(txCtx, "SELECT 1")
	_ = tx.Rollback(txCtx)

	// Release connection
	conn.Release()

	stat := pool.Stat()
	if stat.IdleConns != 0 {
		t.Errorf("expected 0 idle conns after dirty release, got %d", stat.IdleConns)
	}

	// Next acquire should yield a fresh, idle connection
	conn2, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("failed to acquire conn2: %v", err)
	}
	defer conn2.Release()

	if conn2.Conn().PgConn().TxStatus() != pgconn.TxStatusIdle {
		t.Errorf("expected TxStatusIdle ('I'), got '%c'", conn2.Conn().PgConn().TxStatus())
	}
}

func TestDirtyTransactionStatusDestroysConnection(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, nil)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("failed to acquire conn: %v", err)
	}

	// Set status to in transaction without rollback/commit
	conn.Conn().PgConn().SetTxStatus(pgconn.TxStatusInTransaction)
	conn.Release()

	stat := pool.Stat()
	if stat.IdleConns != 0 {
		t.Errorf("expected 0 idle conns, got %d", stat.IdleConns)
	}
	if stat.TotalConns != 0 {
		t.Errorf("expected 0 total conns, got %d", stat.TotalConns)
	}
}

func TestConcurrentCancellationsPoolMetrics(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, nil)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()

	done := make(chan struct{})
	for i := 0; i < 20; i++ {
		go func() {
			defer func() {
				done <- struct{}{}
			}()
			c, err := pool.Acquire(ctx)
			if err != nil {
				return
			}
			reqCtx, cancel := context.WithTimeout(ctx, 1*time.Millisecond)
			defer cancel()
			tx, err := c.Begin(reqCtx)
			if err == nil {
				time.Sleep(2 * time.Millisecond)
				_ = tx.Rollback(reqCtx)
			}
			c.Release()
		}()
	}

	for i := 0; i < 20; i++ {
		<-done
	}

	stat := pool.Stat()
	if stat.AcquiredConns != 0 {
		t.Errorf("expected 0 acquired conns, got %d", stat.AcquiredConns)
	}
}
