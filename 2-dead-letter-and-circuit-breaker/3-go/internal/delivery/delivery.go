package delivery

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/starci-academy/webhook-dlq-circuit/internal/circuit"
)

// Service runs the outbox poller + delivery worker. The circuit guards every
// send; a blocked job is skipped (the Go analog of BullMQ DelayedError is asynq's
// SkipRetry / RetryAfter) so circuit-skipped jobs do not burn delivery attempts.
type Service struct {
	pool    *pgxpool.Pool
	circuit *circuit.Service
	client  *http.Client
}

const maxAttempts = 3

func New(pool *pgxpool.Pool, c *circuit.Service) *Service {
	return &Service{pool: pool, circuit: c, client: &http.Client{Timeout: 3 * time.Second}}
}

// Run starts the poller and sweeper loops until ctx is cancelled.
func (s *Service) Run(ctx context.Context) {
	pollTick := time.NewTicker(500 * time.Millisecond)
	sweepTick := time.NewTicker(2 * time.Second)
	defer pollTick.Stop()
	defer sweepTick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-pollTick.C:
			s.poll(ctx)
		case <-sweepTick.C:
			s.sweep(ctx)
		}
	}
}

type pending struct {
	id, typ, targetURL string
	payload            []byte
	attempts           int
}

func (s *Service) poll(ctx context.Context) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, type, target_url, payload, attempts FROM outbox_event WHERE status='pending' LIMIT 50`)
	if err != nil {
		return
	}
	var jobs []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.id, &p.typ, &p.targetURL, &p.payload, &p.attempts); err == nil {
			jobs = append(jobs, p)
		}
	}
	rows.Close()
	for _, j := range jobs {
		s.deliver(ctx, j)
	}
}

func (s *Service) deliver(ctx context.Context, j pending) {
	host := hostOf(j.targetURL)
	allow, _, isProbe := s.circuit.AllowRequest(ctx, host)
	if !allow {
		// Circuit open: skip without burning an attempt (RetryAfter analog of DelayedError).
		return
	}
	err := s.post(ctx, j.targetURL, j.payload)
	if err == nil {
		s.circuit.RecordSuccess(ctx, host)
		s.pool.Exec(ctx, `UPDATE outbox_event SET status='sent' WHERE id=$1`, j.id)
		return
	}
	s.circuit.RecordFailure(ctx, host, isProbe)
	attempts := j.attempts + 1
	s.pool.Exec(ctx, `UPDATE outbox_event SET attempts=$2, last_error=$3 WHERE id=$1`, j.id, attempts, err.Error())
	if attempts >= maxAttempts {
		s.pool.Exec(ctx, `UPDATE outbox_event SET status='failed' WHERE id=$1`, j.id)
		s.park(ctx, j, attempts, err.Error())
	}
}

func (s *Service) post(ctx context.Context, target string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytesReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return &httpError{resp.StatusCode}
	}
	return nil
}

func (s *Service) park(ctx context.Context, j pending, attempts int, lastErr string) {
	s.pool.Exec(ctx,
		`INSERT INTO dead_letter (event_id, type, target_url, payload, attempts, last_error)
		 VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (event_id) DO NOTHING`,
		j.id, j.typ, j.targetURL, j.payload, attempts, lastErr)
}

func (s *Service) sweep(ctx context.Context) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT target_url FROM outbox_event LIMIT 200`)
	if err != nil {
		return
	}
	var hosts []string
	for rows.Next() {
		var u string
		if rows.Scan(&u) == nil {
			hosts = append(hosts, hostOf(u))
		}
	}
	rows.Close()
	for _, h := range hosts {
		if s.circuit.GetState(ctx, h) == circuit.Open && s.circuit.OpenTTLMillis(ctx, h) < 0 {
			s.circuit.ForceHalfOpen(ctx, h)
		}
	}
}

func hostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return u.Host
}
