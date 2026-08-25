package pgxpool

import (
	"context"
	"sync"

	"github.com/muckomdeead/pgx"
	"github.com/muckomdeead/pgx/pgconn"
)

type Conn struct {
	pool         *Pool
	connResource *connResource
	released     bool
	mu           sync.Mutex
}

func (c *Conn) Conn() *pgx.Conn {
	if c.connResource == nil {
		return nil
	}
	return c.connResource.conn
}

func (c *Conn) Release() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.released || c.connResource == nil {
		return
	}
	c.released = true

	res := c.connResource
	c.connResource = nil

	// Health and transaction status check
	// If TxStatus != TxStatusIdle or connection is closed, destroy rather than returning to idle pool
	if res.conn.IsClosed() || res.conn.PgConn().TxStatus() != pgconn.TxStatusIdle {
		res.Destroy()
		return
	}

	res.Release()
}

func (c *Conn) Exec(ctx context.Context, sql string) error {
	if c.released || c.connResource == nil {
		return ErrConnClosed
	}
	return c.Conn().Exec(ctx, sql)
}

func (c *Conn) Begin(ctx context.Context) (*pgx.Tx, error) {
	if c.released || c.connResource == nil {
		return nil, ErrConnClosed
	}
	return c.Conn().Begin(ctx)
}
