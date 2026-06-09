// Command emitter starts the transactional-outbox emitter HTTP server, runs the
// schema migration, and launches the poller + watchdog ticker goroutines.
package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/starci-academy/outbox-go/internal/config"
	"github.com/starci-academy/outbox-go/internal/emitter"
)

func main() {
	cfg := config.Load()
	ctx := context.Background()

	pool := connectPostgres(ctx, cfg.PostgresURL)
	defer pool.Close()

	if err := emitter.Migrate(ctx, pool); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	em := emitter.New(pool, cfg)
	em.StartLoops(ctx)

	r := chi.NewRouter()
	em.Routes(r)

	log.Printf("emitter listening on %s", cfg.Port)
	if err := http.ListenAndServe(":"+cfg.Port, r); err != nil {
		log.Fatalf("server: %v", err)
	}
}

func connectPostgres(ctx context.Context, url string) *pgxpool.Pool {
	for i := 0; i < 30; i++ {
		pool, err := pgxpool.New(ctx, url)
		if err == nil {
			if pingErr := pool.Ping(ctx); pingErr == nil {
				return pool
			}
			pool.Close()
		}
		time.Sleep(time.Second)
	}
	log.Fatalf("postgres not reachable after 30 attempts")
	return nil
}
