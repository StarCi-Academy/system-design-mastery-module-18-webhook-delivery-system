package circuit

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// State enumerates the three circuit states.
type State string

const (
	Closed   State = "closed"
	Open     State = "open"
	HalfOpen State = "half_open"
)

const (
	FailThreshold = 3
	OpenTTL       = 20 * time.Second
	ProbeTTL      = 5 * time.Second
	FailWindow    = 60 * time.Second
)

// Service is a per-endpoint circuit breaker backed by Redis. State is shared
// across worker replicas through three keys per host (state/fail/probe) plus an
// openUntil stamp the sweeper reads to flip an expired open circuit to half_open.
type Service struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Service { return &Service{rdb: rdb} }

func (s *Service) stateKey(h string) string     { return "circuit:state:" + h }
func (s *Service) failKey(h string) string      { return "circuit:fail:" + h }
func (s *Service) probeKey(h string) string     { return "circuit:probe:" + h }
func (s *Service) openUntilKey(h string) string { return "circuit:openUntil:" + h }

// GetState returns the current state for a host (absent key == closed).
func (s *Service) GetState(ctx context.Context, host string) State {
	v, _ := s.rdb.Get(ctx, s.stateKey(host)).Result()
	switch State(v) {
	case Open:
		return Open
	case HalfOpen:
		return HalfOpen
	default:
		return Closed
	}
}

// AllowRequest decides whether a delivery may proceed. half_open admits exactly
// one probe via SET NX so concurrent workers cannot stampede the recovering host.
func (s *Service) AllowRequest(ctx context.Context, host string) (allow bool, state State, isProbe bool) {
	state = s.GetState(ctx, host)
	switch state {
	case Closed:
		return true, state, false
	case HalfOpen:
		ok, _ := s.rdb.SetNX(ctx, s.probeKey(host), "1", ProbeTTL).Result()
		return ok, state, ok
	default:
		return false, state, false
	}
}

func (s *Service) trip(ctx context.Context, host string) {
	s.rdb.Set(ctx, s.stateKey(host), string(Open), 0)
	until := time.Now().Add(OpenTTL).UnixMilli()
	s.rdb.Set(ctx, s.openUntilKey(host), strconv.FormatInt(until, 10), 0)
}

// RecordSuccess closes the circuit and clears every key for the host.
func (s *Service) RecordSuccess(ctx context.Context, host string) {
	s.rdb.Del(ctx, s.stateKey(host), s.failKey(host), s.probeKey(host), s.openUntilKey(host))
}

// RecordFailure bumps the consecutive counter; reaching the threshold trips open.
// A failed half_open probe re-opens immediately.
func (s *Service) RecordFailure(ctx context.Context, host string, isProbe bool) {
	if isProbe {
		s.trip(ctx, host)
		s.rdb.Del(ctx, s.probeKey(host))
		return
	}
	fails, _ := s.rdb.Incr(ctx, s.failKey(host)).Result()
	s.rdb.Expire(ctx, s.failKey(host), FailWindow)
	if fails >= FailThreshold {
		s.trip(ctx, host)
	}
}

// ForceHalfOpen is the sweeper hook for an expired open circuit.
func (s *Service) ForceHalfOpen(ctx context.Context, host string) {
	s.rdb.Set(ctx, s.stateKey(host), string(HalfOpen), 0)
	s.rdb.Del(ctx, s.openUntilKey(host))
}

// OpenTTLMillis returns the milliseconds left in the cool-down (negative once elapsed).
func (s *Service) OpenTTLMillis(ctx context.Context, host string) int64 {
	v, err := s.rdb.Get(ctx, s.openUntilKey(host)).Result()
	if err != nil {
		return -1
	}
	until, _ := strconv.ParseInt(v, 10, 64)
	return until - time.Now().UnixMilli()
}

func (s *Service) FailCount(ctx context.Context, host string) int {
	v, _ := s.rdb.Get(ctx, s.failKey(host)).Int()
	return v
}

// Status is the JSON shape returned by GET /api/circuit/status.
type Status struct {
	Host          string `json:"host"`
	State         string `json:"state"`
	FailCount     int    `json:"failCount"`
	FailThreshold int    `json:"failThreshold"`
	OpenTTLSec    int    `json:"openTtlSec"`
}

func (s *Service) Status(ctx context.Context, host string) Status {
	ttl := s.OpenTTLMillis(ctx, host)
	sec := 0
	if ttl > 0 {
		sec = int(ttl / 1000)
	}
	return Status{
		Host:          host,
		State:         string(s.GetState(ctx, host)),
		FailCount:     s.FailCount(ctx, host),
		FailThreshold: FailThreshold,
		OpenTTLSec:    sec,
	}
}
