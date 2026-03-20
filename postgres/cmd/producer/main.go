package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type config struct {
	DSN           string
	MessagePrefix string
}

func main() {
	cfg := config{
		DSN:           envOr("PG_DSN", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"),
		MessagePrefix: envOr("PG_MESSAGE_PREFIX", "wal_demo"),
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, cfg.DSN)
	if err != nil {
		log.Fatalf("connect db: %v", err)
	}
	defer pool.Close()

	if err := initSchema(ctx, pool); err != nil {
		log.Fatalf("init schema: %v", err)
	}

	orderID, err := recordOrder(ctx, pool)
	if err != nil {
		log.Fatalf("insert sample order: %v", err)
	}

	payload, err := json.Marshal(map[string]any{
		"order_id": orderID,
		"event":    "order_created",
		"sent_at":  time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		log.Fatalf("payload encode: %v", err)
	}

	if err := emitMessage(ctx, pool, cfg.MessagePrefix, payload); err != nil {
		log.Fatalf("emit logical message: %v", err)
	}

	fmt.Printf("recorded order %d and emitted logical message under prefix %q\n", orderID, cfg.MessagePrefix)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func initSchema(ctx context.Context, pool *pgxpool.Pool) error {
	const ddl = `
	CREATE TABLE IF NOT EXISTS orders (
		id BIGSERIAL PRIMARY KEY,
		total_cents INTEGER NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`
	_, err := pool.Exec(ctx, ddl)
	return err
}

func recordOrder(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	const dml = `INSERT INTO orders (total_cents) VALUES ($1) RETURNING id`
	var id int64
	amount := time.Now().Unix()%5000 + 100
	if err := pool.QueryRow(ctx, dml, amount).Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

func emitMessage(ctx context.Context, pool *pgxpool.Pool, prefix string, payload []byte) error {
	const sql = `SELECT pg_logical_emit_message($1, $2, $3)`
	// send as non-transactional so it reaches the WAL even if caller commits later
	_, err := pool.Exec(ctx, sql, false, prefix, payload)
	return err
}
