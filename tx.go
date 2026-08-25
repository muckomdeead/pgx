package pgx

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/muckomdeead/pgx/pgconn"
)

var (
	ErrTxClosed = errors.New("tx is closed")
)

type Tx struct {
	conn   *Conn
	closed bool
	mu     sync.Mutex
}

func (tx *Tx) Conn() *Conn {
	return tx.conn
}

func (tx *Tx) Commit(ctx context.Context) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return ErrTxClosed
	}
	tx.closed = true

	if tx.conn.PgConn().TxStatus() == pgconn.TxStatusInFailedTransaction {
		return errors.New("cannot commit failed transaction")
	}

	if err := ctx.Err(); err != nil {
		tx.conn.PgConn().SetTxStatus(pgconn.TxStatusInFailedTransaction)
		return err
	}

	err := tx.conn.Exec(ctx, "COMMIT")
	if err != nil {
		tx.conn.PgConn().SetTxStatus(pgconn.TxStatusInFailedTransaction)
		return err
	}
	tx.conn.PgConn().SetTxStatus(pgconn.TxStatusIdle)
	return nil
}

func (tx *Tx) Rollback(ctx context.Context) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return ErrTxClosed
	}
	tx.closed = true

	status := tx.conn.PgConn().TxStatus()
	if status != pgconn.TxStatusInTransaction && status != pgconn.TxStatusInFailedTransaction {
		return nil
	}

	// If context is already canceled or deadline exceeded, attempt clean rollback using a fallback background context
	if ctx.Err() != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := tx.conn.Exec(cleanupCtx, "ROLLBACK")
		if err != nil {
			// Rollback failed or cannot be guaranteed; close connection so pool destroys it
			_ = tx.conn.Close(cleanupCtx)
			return ctx.Err()
		}
		tx.conn.PgConn().SetTxStatus(pgconn.TxStatusIdle)
		return ctx.Err()
	}

	err := tx.conn.Exec(ctx, "ROLLBACK")
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.conn.Close(cleanupCtx)
		return err
	}

	tx.conn.PgConn().SetTxStatus(pgconn.TxStatusIdle)
	return nil
}

func (tx *Tx) Exec(ctx context.Context, sql string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return ErrTxClosed
	}

	if err := ctx.Err(); err != nil {
		tx.conn.PgConn().SetTxStatus(pgconn.TxStatusInFailedTransaction)
		return err
	}

	err := tx.conn.Exec(ctx, sql)
	if err != nil {
		tx.conn.PgConn().SetTxStatus(pgconn.TxStatusInFailedTransaction)
		return err
	}
	return nil
}
