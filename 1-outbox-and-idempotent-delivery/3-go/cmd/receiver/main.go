// Command receiver starts the idempotent webhook receiver-mock HTTP server.
package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/redis/go-redis/v9"

	"github.com/starci-academy/outbox-go/internal/config"
	"github.com/starci-academy/outbox-go/internal/receiver"
)

func main() {
	cfg := config.Load()

	opt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		log.Fatalf("parse redis url: %v", err)
	}
	rdb := redis.NewClient(opt)

	failFirst := envInt("FAIL_FIRST_N_ATTEMPTS", 0)
	ttl := time.Duration(envInt("IDEM_TTL_SECONDS", 86400)) * time.Second

	rcv := receiver.New(rdb, failFirst, ttl)

	r := chi.NewRouter()
	r.Post("/api/hook", rcv.Hook)
	r.Get("/health", rcv.Health)

	log.Printf("receiver-mock listening on %s, FAIL_FIRST_N_ATTEMPTS=%d", cfg.Port, failFirst)
	if err := http.ListenAndServe(":"+cfg.Port, r); err != nil {
		log.Fatalf("server: %v", err)
	}
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
