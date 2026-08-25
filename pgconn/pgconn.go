package pgconn

import (
	"context"
	"errors"
	"net"
	"sync"
)

const (
	TxStatusIdle                = 'I'
	TxStatusInTransaction       = 'T'
	TxStatusInFailedTransaction = 'E'
)

type PgConn struct {
	mu       sync.Mutex
	conn     net.Conn
	txStatus byte
	closed   bool
}

func NewPgConn(conn net.Conn) *PgConn {
	return &PgConn{
		conn:     conn,
		txStatus: TxStatusIdle,
	}
}

func (c *PgConn) TxStatus() byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.txStatus
}

func (c *PgConn) SetTxStatus(status byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.txStatus = status
}

func (c *PgConn) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *PgConn) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *PgConn) Exec(ctx context.Context, sql string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("connection closed")
	}
	if err := ctx.Err(); err != nil {
		c.txStatus = TxStatusInFailedTransaction
		return err
	}
	return nil
}
