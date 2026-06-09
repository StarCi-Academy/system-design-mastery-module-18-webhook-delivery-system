package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/starci-academy/webhook-dlq-circuit/internal/api"
	"github.com/starci-academy/webhook-dlq-circuit/internal/circuit"
	"github.com/starci-academy/webhook-dlq-circuit/internal/delivery"
)

const schema = `
CREATE TABLE IF NOT EXISTS outbox_event (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  type TEXT NOT NULL,
  payload JSONB NOT NULL,
  target_url TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INT NOT NULL DEFAULT 0,
  last_error TEXT
);
CREATE TABLE IF NOT EXISTS dead_letter (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id UUID NOT NULL UNIQUE,
  type TEXT NOT NULL,
  target_url TEXT NOT NULL,
  payload JSONB NOT NULL,
  attempts INT NOT NULL,
  last_error TEXT NOT NULL,
  parked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  replayed_at TIMESTAMPTZ
);`

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, env("POSTGRES_URL", "postgres://webhook:webhook@postgres:5432/webhook"))
	if err != nil {
		log.Fatal(err)
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		log.Fatal(err)
	}

	opt, _ := redis.ParseURL(env("REDIS_URL", "redis://redis:6379"))
	rdb := redis.NewClient(opt)

	cb := circuit.New(rdb)
	go delivery.New(pool, cb).Run(ctx)

	h := api.New(pool, cb)
	port := env("PORT", "3000")
	log.Printf("emitter listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, h.Routes()))
}
