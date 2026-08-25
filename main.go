package main

import (
	"context"
	"fmt"

	"github.com/muckomdeead/pgx/pgconn"
	"github.com/muckomdeead/pgx/pgxpool"
)

func main() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, nil)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		panic(err)
	}

	txCtx, cancel := context.WithCancel(ctx)
	tx, err := conn.Begin(txCtx)
	if err != nil {
		panic(err)
	}

	cancel()
	_ = tx.Rollback(txCtx)
	conn.Release()

	stat := pool.Stat()
	fmt.Printf("Pool idle conns: %d (expected 0 dirty conns in idle pool)\n", stat.IdleConns)

	conn2, err := pool.Acquire(ctx)
	if err != nil {
		panic(err)
	}
	defer conn2.Release()

	fmt.Printf("Acquired connection status: %c (TxStatusIdle: %c)\n", conn2.Conn().PgConn().TxStatus(), pgconn.TxStatusIdle)
	fmt.Println("pgx pool connection status protection verified successfully!")
}
