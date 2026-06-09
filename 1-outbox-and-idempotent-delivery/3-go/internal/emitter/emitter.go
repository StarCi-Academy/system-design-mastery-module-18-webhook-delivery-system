// Package emitter implements the transactional-outbox emitter: a publish API that
// writes the business change and the outbox row in one transaction, a ticker
// goroutine poller that claims pending rows with FOR UPDATE SKIP LOCKED, a worker
// pool that delivers with retry/backoff, and a watchdog that reclaims stuck rows.
package emitter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/starci-academy/outbox-go/internal/config"
)

const (
	pollInterval     = 500 * time.Millisecond
	watchdogInterval = 2 * time.Second
	batchSize        = 10
	maxAttempts      = 6
	workerPool       = 4
	inFlightTimeout  = 4 * time.Second
	proxyMaxAttempts = 12
)

// Emitter owns the database pool and HTTP client used to deliver events.
type Emitter struct {
	pool *pgxpool.Pool
	cfg  config.Config
	http *http.Client
	sem  chan struct{}
}

// New builds an Emitter with a bounded worker-pool semaphore.
func New(pool *pgxpool.Pool, cfg config.Config) *Emitter {
	return &Emitter{
		pool: pool,
		cfg:  cfg,
		http: &http.Client{Timeout: 5 * time.Second},
		sem:  make(chan struct{}, workerPool),
	}
}

// PublishInput is the POST /api/events/publish request body.
type PublishInput struct {
	Type      string          `json:"type"`
	TargetURL string          `json:"targetUrl"`
	Payload   json.RawMessage `json:"payload"`
}

// PublishResult is the 201 response body.
type PublishResult struct {
	EventID string `json:"eventId"`
	Status  string `json:"status"`
}

// OutboxEvent is a claimed row handed to the worker pool.
type OutboxEvent struct {
	ID        string
	TargetURL string
	Payload   []byte
	Attempts  int
}

// Migrate creates the outbox_event and orders tables if they do not exist.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	stmts := []string{
		`CREATE EXTENSION IF NOT EXISTS pgcrypto`,
		`CREATE TABLE IF NOT EXISTS orders (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			ref VARCHAR(128)
		)`,
		`CREATE TABLE IF NOT EXISTS outbox_event (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			type VARCHAR(128) NOT NULL,
			target_url VARCHAR(512) NOT NULL,
			payload JSONB NOT NULL,
			status VARCHAR(16) NOT NULL DEFAULT 'pending',
			attempts INT NOT NULL DEFAULT 0,
			last_error TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox_event(status, created_at)`,
	}
	for _, s := range stmts {
		if _, err := pool.Exec(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

// Publish writes the business change and the outbox row in ONE transaction.
// Either both are durable, or neither happened — there is no window where the
// order is committed but the webhook event is lost.
func (s *Emitter) Publish(ctx context.Context, in PublishInput) (PublishResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return PublishResult{}, err
	}
	defer tx.Rollback(ctx) // no-op once Commit succeeds; safe if we return early

	// Business change (here: an orders row) and the outbox row share the same txn.
	var orderRef *string
	if ref := orderRefFromPayload(in.Payload); ref != "" {
		orderRef = &ref
	}
	var orderID string
	if err := tx.QueryRow(ctx,
		`INSERT INTO orders (ref) VALUES ($1) RETURNING id`, orderRef,
	).Scan(&orderID); err != nil {
		return PublishResult{}, err
	}
	var eventID, status string
	// status starts as 'pending'; the poller picks up pending rows on the next tick.
	if err := tx.QueryRow(ctx,
		`INSERT INTO outbox_event (type, target_url, payload, status, attempts)
		 VALUES ($1, $2, $3, 'pending', 0) RETURNING id, status`,
		in.Type, in.TargetURL, string(in.Payload),
	).Scan(&eventID, &status); err != nil {
		return PublishResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return PublishResult{}, err
	}
	return PublishResult{EventID: eventID, Status: status}, nil
}

func orderRefFromPayload(payload json.RawMessage) string {
	var m map[string]any
	if json.Unmarshal(payload, &m) != nil {
		return ""
	}
	if v, ok := m["orderId"].(string); ok {
		return v
	}
	return ""
}

// claimPending claims a batch of pending rows. pgx supports the Postgres
// FOR UPDATE SKIP LOCKED clause natively: two pollers see disjoint batches
// instead of waiting on each other or double-claiming.
func (s *Emitter) claimPending(ctx context.Context, batch int) ([]OutboxEvent, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, target_url, payload, attempts FROM outbox_event
		WHERE status = 'pending' ORDER BY created_at ASC
		LIMIT $1 FOR UPDATE SKIP LOCKED`, batch)
	if err != nil {
		return nil, err
	}
	var events []OutboxEvent
	var ids []string
	for rows.Next() {
		var e OutboxEvent
		if err := rows.Scan(&e.ID, &e.TargetURL, &e.Payload, &e.Attempts); err != nil {
			rows.Close()
			return nil, err
		}
		events = append(events, e)
		ids = append(ids, e.ID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, tx.Commit(ctx)
	}
	// Flip to in_flight inside the SAME txn so the next tick never re-claims them.
	if _, err := tx.Exec(ctx,
		`UPDATE outbox_event SET status = 'in_flight', updated_at = now() WHERE id = ANY($1)`, ids,
	); err != nil {
		return nil, err
	}
	return events, tx.Commit(ctx)
}

// deliverOne sends a single event to its target with the Idempotency-Key set to
// the event id, then transitions the row based on the receiver's response.
func (s *Emitter) deliverOne(ctx context.Context, ev OutboxEvent) {
	status, _, err := s.postJSON(ctx, ev.TargetURL, ev.Payload, ev.ID)
	if err == nil && status >= 200 && status < 300 {
		if _, e := s.pool.Exec(ctx,
			`UPDATE outbox_event SET status = 'sent', last_error = NULL, updated_at = now() WHERE id = $1`,
			ev.ID); e != nil {
			log.Printf("mark sent error: %v", e)
		}
		return
	}
	msg := fmt.Sprintf("receiver HTTP %d", status)
	if err != nil {
		msg = err.Error()
	}
	nextStatus := "pending"
	if ev.Attempts+1 >= maxAttempts {
		nextStatus = "failed"
	}
	if _, e := s.pool.Exec(ctx,
		`UPDATE outbox_event SET status = $2, attempts = attempts + 1, last_error = $3, updated_at = now() WHERE id = $1`,
		ev.ID, nextStatus, msg); e != nil {
		log.Printf("mark retry error: %v", e)
	}
}

func (s *Emitter) postJSON(ctx context.Context, url string, body []byte, idemKey string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idemKey)
	resp, err := s.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out, nil
}

func (s *Emitter) pollOnce(ctx context.Context) {
	events, err := s.claimPending(ctx, batchSize)
	if err != nil {
		log.Printf("poll error: %v", err)
		return
	}
	for _, ev := range events {
		ev := ev
		s.sem <- struct{}{}
		go func() {
			defer func() {
				<-s.sem
				if r := recover(); r != nil {
					log.Printf("worker panic recovered: %v", r)
				}
			}()
			s.deliverOne(ctx, ev)
		}()
	}
}

// watchdog resets rows stuck in_flight (the emitter crashed mid-delivery) back to
// pending so the next poller tick re-claims them.
func (s *Emitter) watchdog(ctx context.Context) {
	if _, err := s.pool.Exec(ctx,
		`UPDATE outbox_event SET status = 'pending', updated_at = now()
		 WHERE status = 'in_flight' AND updated_at < now() - make_interval(secs => $1)`,
		inFlightTimeout.Seconds()); err != nil {
		log.Printf("watchdog error: %v", err)
	}
}

// StartLoops launches the poller and watchdog ticker goroutines.
func (s *Emitter) StartLoops(ctx context.Context) {
	go func() {
		t := time.NewTicker(pollInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.pollOnce(ctx)
			}
		}
	}()
	go func() {
		t := time.NewTicker(watchdogInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.watchdog(ctx)
			}
		}
	}()
}

// Routes registers the emitter HTTP handlers on the chi router.
func (s *Emitter) Routes(r chi.Router) {
	r.Post("/api/events/publish", s.handlePublish)
	r.Get("/api/events/{id}", s.handleGet)
	r.Post("/api/hook-proxy", s.handleHookProxy)
	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]any{"ok": true}) })
}

func (s *Emitter) handlePublish(w http.ResponseWriter, r *http.Request) {
	var in PublishInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeJSON(w, 400, map[string]any{"statusCode": 400, "message": "invalid JSON body"})
		return
	}
	out, err := s.Publish(r.Context(), in)
	if err != nil {
		writeJSON(w, 500, map[string]any{"statusCode": 500, "message": err.Error()})
		return
	}
	writeJSON(w, 201, out)
}

func (s *Emitter) handleGet(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var (
		status               string
		attempts             int
		lastError            *string
		createdAt, updatedAt time.Time
	)
	err := s.pool.QueryRow(r.Context(),
		`SELECT status, attempts, last_error, created_at, updated_at FROM outbox_event WHERE id = $1`, id,
	).Scan(&status, &attempts, &lastError, &createdAt, &updatedAt)
	if err == pgx.ErrNoRows {
		writeJSON(w, 404, map[string]any{"statusCode": 404, "message": fmt.Sprintf("Event %s not found", id)})
		return
	}
	if err != nil {
		writeJSON(w, 500, map[string]any{"statusCode": 500, "message": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{
		"id":        id,
		"status":    status,
		"attempts":  attempts,
		"lastError": lastError,
		"createdAt": createdAt.UTC().Format(time.RFC3339Nano),
		"updatedAt": updatedAt.UTC().Format(time.RFC3339Nano),
	})
}

// handleHookProxy forwards a hook call to receiver-mock with the client's
// Idempotency-Key, retrying on a non-2xx (the receiver fails the first N attempts)
// so the caller observes the first successful response and, on a replay, the same
// cached response byte-identical.
func (s *Emitter) handleHookProxy(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	idemKey := r.Header.Get("Idempotency-Key")
	var status int
	var out []byte
	for i := 0; i < proxyMaxAttempts; i++ {
		var err error
		status, out, err = s.postJSON(r.Context(), s.cfg.ReceiverURL, body, idemKey)
		if err == nil && (status < 500) {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(out)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
