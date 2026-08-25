package pgx

import (
	"context"
	"errors"
	"sync"

	"github.com/muckomdeead/pgx/pgconn"
)

type Conn struct {
	pgConn *pgconn.PgConn
	mu     sync.Mutex
	closed bool
}

func NewConn(pgConn *pgconn.PgConn) *Conn {
	if pgConn == nil {
		pgConn = pgconn.NewPgConn(nil)
	}
	return &Conn{
		pgConn: pgConn,
	}
}

func (c *Conn) PgConn() *pgconn.PgConn {
	return c.pgConn
}

func (c *Conn) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed || (c.pgConn != nil && c.pgConn.IsClosed())
}

func (c *Conn) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.pgConn != nil {
		return c.pgConn.Close(ctx)
	}
	return nil
}

func (c *Conn) Exec(ctx context.Context, sql string) error {
	if c.IsClosed() {
		return errors.New("conn closed")
	}
	if err := ctx.Err(); err != nil {
		if c.pgConn != nil && c.pgConn.TxStatus() == pgconn.TxStatusInTransaction {
			c.pgConn.SetTxStatus(pgconn.TxStatusInFailedTransaction)
		}
		return err
	}
	if c.pgConn != nil {
		return c.pgConn.Exec(ctx, sql)
	}
	return nil
}

func (c *Conn) Begin(ctx context.Context) (*Tx, error) {
	if c.IsClosed() {
		return nil, errors.New("conn closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.pgConn != nil {
		c.pgConn.SetTxStatus(pgconn.TxStatusInTransaction)
	}
	return &Tx{
		conn: c,
	}, nil
}
