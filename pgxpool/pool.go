package pgxpool

import (
	"context"
	"errors"
	"sync"

	"github.com/muckomdeead/pgx"
	"github.com/muckomdeead/pgx/pgconn"
)

var (
	ErrPoolClosed = errors.New("pgxpool: pool is closed")
	ErrConnClosed = errors.New("pgxpool: connection is closed")
)

type Stat struct {
	AcquiredConns int64
	IdleConns     int64
	TotalConns    int64
}

type connResource struct {
	pool *Pool
	conn *pgx.Conn
}

func (cr *connResource) Release() {
	cr.pool.returnConn(cr)
}

func (cr *connResource) Destroy() {
	cr.pool.destroyConn(cr)
}

type Pool struct {
	mu          sync.Mutex
	idleConns   []*connResource
	allConns    map[*connResource]struct{}
	closed      bool
	connFactory func(ctx context.Context) (*pgx.Conn, error)
}

func New(ctx context.Context, connFactory func(ctx context.Context) (*pgx.Conn, error)) (*Pool, error) {
	if connFactory == nil {
		connFactory = func(ctx context.Context) (*pgx.Conn, error) {
			pgConn := pgconn.NewPgConn(nil)
			return pgx.NewConn(pgConn), nil
		}
	}
	return &Pool{
		idleConns:   make([]*connResource, 0),
		allConns:    make(map[*connResource]struct{}),
		connFactory: connFactory,
	}, nil
}

func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}

	// Retrieve idle conn
	for len(p.idleConns) > 0 {
		cr := p.idleConns[len(p.idleConns)-1]
		p.idleConns = p.idleConns[:len(p.idleConns)-1]

		// Double check idle conn is healthy and idle
		if cr.conn.IsClosed() || cr.conn.PgConn().TxStatus() != pgconn.TxStatusIdle {
			delete(p.allConns, cr)
			_ = cr.conn.Close(context.Background())
			continue
		}

		p.mu.Unlock()
		return &Conn{pool: p, connResource: cr}, nil
	}
	p.mu.Unlock()

	c, err := p.connFactory(ctx)
	if err != nil {
		return nil, err
	}

	cr := &connResource{
		pool: p,
		conn: c,
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = c.Close(context.Background())
		return nil, ErrPoolClosed
	}
	p.allConns[cr] = struct{}{}
	p.mu.Unlock()

	return &Conn{pool: p, connResource: cr}, nil
}

func (p *Pool) returnConn(cr *connResource) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		delete(p.allConns, cr)
		_ = cr.conn.Close(context.Background())
		return
	}

	if cr.conn.IsClosed() || cr.conn.PgConn().TxStatus() != pgconn.TxStatusIdle {
		delete(p.allConns, cr)
		_ = cr.conn.Close(context.Background())
		return
	}

	p.idleConns = append(p.idleConns, cr)
}

func (p *Pool) destroyConn(cr *connResource) {
	p.mu.Lock()
	delete(p.allConns, cr)
	p.mu.Unlock()

	_ = cr.conn.Close(context.Background())
}

func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true

	conns := make([]*connResource, 0, len(p.allConns))
	for cr := range p.allConns {
		conns = append(conns, cr)
	}
	p.idleConns = nil
	p.allConns = make(map[*connResource]struct{})
	p.mu.Unlock()

	for _, cr := range conns {
		_ = cr.conn.Close(context.Background())
	}
}

func (p *Pool) Stat() Stat {
	p.mu.Lock()
	defer p.mu.Unlock()

	idle := int64(len(p.idleConns))
	total := int64(len(p.allConns))
	acquired := total - idle
	return Stat{
		AcquiredConns: acquired,
		IdleConns:     idle,
		TotalConns:    total,
	}
}
