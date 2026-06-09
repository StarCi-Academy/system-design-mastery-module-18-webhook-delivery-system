// Package receiver implements the idempotent webhook receiver-mock: it caches the
// first successful response with SET ... NX EX and replays it byte-identical, it
// rejects an Idempotency-Key reused with a different body (409), and it fails the
// first FAIL_FIRST_N_ATTEMPTS attempts per key to demonstrate emitter retries.
package receiver

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

// Receiver owns the Redis client and the demo-failure configuration.
type Receiver struct {
	rdb       *redis.Client
	failFirst int
	ttl       time.Duration
}

// New builds a Receiver.
func New(rdb *redis.Client, failFirst int, ttl time.Duration) *Receiver {
	return &Receiver{rdb: rdb, failFirst: failFirst, ttl: ttl}
}

// HookResult is the JSON body returned on a successful hook call.
type HookResult struct {
	Received             bool            `json:"received"`
	EventID              string          `json:"eventId"`
	IdempotencyKey       string          `json:"idempotencyKey"`
	SideEffectExecutedAt string          `json:"sideEffectExecutedAt"`
	BodyEcho             json.RawMessage `json:"bodyEcho"`
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Hook is the POST /api/hook handler.
func (s *Receiver) Hook(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rawBody, _ := io.ReadAll(r.Body)
	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey == "" {
		writeJSON(w, 400, map[string]any{"statusCode": 400, "message": "missing Idempotency-Key"})
		return
	}

	bodyHash := sha256Hex(rawBody)
	cacheKey := "hook:idem:" + idemKey + ":" + bodyHash
	seenHashKey := "hook:idem:hash:" + idemKey

	// Cache hit → replay the first response verbatim; the side-effect does NOT run again.
	if cached, err := s.rdb.Get(ctx, cacheKey).Result(); err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Idempotent-Replayed", "true")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, cached)
		return
	}

	// Same key + different body → 409 (body-hash conflict gate).
	if priorHash, err := s.rdb.Get(ctx, seenHashKey).Result(); err == nil && priorHash != bodyHash {
		writeJSON(w, 409, map[string]any{"statusCode": 409, "message": "Idempotency-Key reused with a different body"})
		return
	}

	// Demo retry: count attempts per key; fail the first N.
	attemptKey := "hook:attempt:" + idemKey
	attempt, _ := s.rdb.Incr(ctx, attemptKey).Result()
	s.rdb.Expire(ctx, attemptKey, s.ttl)
	if int(attempt) <= s.failFirst {
		writeJSON(w, 503, map[string]any{"statusCode": 503, "message": "simulated failure"})
		return
	}

	// Cache miss → run the side-effect exactly once and capture its result.
	eventID := idemKey
	if id := idFromBody(rawBody); id != "" {
		eventID = id
	}
	result := HookResult{
		Received:             true,
		EventID:              eventID,
		IdempotencyKey:       idemKey,
		SideEffectExecutedAt: time.Now().UTC().Format(time.RFC3339Nano),
		BodyEcho:             json.RawMessage(rawBody),
	}
	payload, _ := json.Marshal(result)

	// SetNX maps to SET ... NX EX: only the winner writes; a racing first-time
	// request reads the winner's value instead of double-running the side-effect.
	won, _ := s.rdb.SetNX(ctx, cacheKey, payload, s.ttl).Result()
	s.rdb.Set(ctx, seenHashKey, bodyHash, s.ttl)
	if !won {
		if winner, err := s.rdb.Get(ctx, cacheKey).Result(); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Idempotent-Replayed", "true")
			w.WriteHeader(200)
			_, _ = io.WriteString(w, winner)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_, _ = w.Write(payload)
}

func idFromBody(rawBody []byte) string {
	var m map[string]any
	if json.Unmarshal(rawBody, &m) != nil {
		return ""
	}
	if v, ok := m["id"].(string); ok {
		return v
	}
	return ""
}

// Health is a liveness probe.
func (s *Receiver) Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]any{"ok": true})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
