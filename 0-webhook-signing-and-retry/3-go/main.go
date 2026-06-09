package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// AttemptRecord is one delivery attempt of an event.
type AttemptRecord struct {
	Attempt        int   `json:"attempt"`
	PlannedDelayMs int64 `json:"plannedDelayMs"`
	Status         int   `json:"status"`
}

// EventRecord is the stored state of a published event.
type EventRecord struct {
	EventID     string          `json:"eventId"`
	Type        string          `json:"type"`
	TargetURL   string          `json:"targetUrl"`
	Payload     json.RawMessage `json:"payload"`
	FinalStatus string          `json:"finalStatus"`
	Attempts    []AttemptRecord `json:"attempts"`
}

// Service holds the emitter state and dependencies.
type Service struct {
	secret      string
	maxAttempts int
	client      *http.Client
	store       map[string]*EventRecord
	mu          sync.Mutex
}

func newService(secret string, maxAttempts int) *Service {
	return &Service{
		secret:      secret,
		maxAttempts: maxAttempts,
		client:      &http.Client{Timeout: 5 * time.Second},
		store:       make(map[string]*EventRecord),
	}
}

func newEventID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s-%s-%s-%s-%s", hex.EncodeToString(b[0:4]), hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]), hex.EncodeToString(b[8:10]), hex.EncodeToString(b[10:16]))
}

func (s *Service) record(eventID string, a AttemptRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.store[eventID]; ok {
		r.Attempts = append(r.Attempts, a)
	}
}

func (s *Service) setFinal(eventID, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.store[eventID]; ok {
		r.FinalStatus = status
	}
}

func (s *Service) deliverWithRetry(eventID, targetURL, body string) {
	for attempt := 1; attempt <= s.maxAttempts; attempt++ {
		// Re-sign with a FRESH timestamp every attempt so a late retry stays in-window.
		ts := time.Now().Unix()
		header := BuildSignatureHeader(ts, Sign(s.secret, body, ts))
		var plannedDelayMs int64
		if attempt > 1 {
			plannedDelayMs = backoffMs[attempt-2] + jitter(backoffMs[attempt-2], eventID, attempt)
			time.Sleep(time.Duration(plannedDelayMs) * time.Millisecond)
		}
		req, _ := http.NewRequest(http.MethodPost, targetURL, strings.NewReader(body))
		req.Header.Set("X-Webhook-Id", eventID)
		req.Header.Set("X-Webhook-Timestamp", strconv.FormatInt(ts, 10))
		req.Header.Set("X-Webhook-Signature", header)
		req.Header.Set("Content-Type", "application/json")
		status := 0
		if resp, err := s.client.Do(req); err == nil {
			status = resp.StatusCode
			resp.Body.Close()
		}
		s.record(eventID, AttemptRecord{Attempt: attempt, PlannedDelayMs: plannedDelayMs, Status: status})
		if status >= 200 && status < 300 {
			s.setFinal(eventID, "delivered")
			return
		}
	}
	s.setFinal(eventID, "failed")
}

type publishDTO struct {
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	TargetURL string          `json:"targetUrl"`
}

func (s *Service) handlePublish(w http.ResponseWriter, r *http.Request) {
	var dto publishDTO
	if err := json.NewDecoder(r.Body).Decode(&dto); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "invalid body", "statusCode": 400})
		return
	}
	eventID := newEventID()
	rec := &EventRecord{
		EventID:     eventID,
		Type:        dto.Type,
		TargetURL:   dto.TargetURL,
		Payload:     dto.Payload,
		FinalStatus: "pending",
		Attempts:    []AttemptRecord{},
	}
	s.mu.Lock()
	s.store[eventID] = rec
	s.mu.Unlock()
	// Fire-and-forget: delivery runs off the request path.
	go s.deliverWithRetry(eventID, dto.TargetURL, string(dto.Payload))
	writeJSON(w, http.StatusCreated, rec)
}

func (s *Service) handleGet(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimPrefix(r.URL.Path, "/api/events/")
	s.mu.Lock()
	rec, ok := s.store[eventID]
	s.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"message": "event not found", "statusCode": 404})
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func handleHook(secret string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		header := r.Header.Get("X-Webhook-Signature")
		if err := Verify(secret, string(raw), header); err != nil {
			writeJSON(w, http.StatusUnauthorized, map[string]any{
				"message": err.Error(), "error": "Unauthorized", "statusCode": 401,
			})
			return
		}
		// Chaos hook: force a specific status code AFTER a valid signature.
		if fail := r.URL.Query().Get("fail"); fail != "" {
			code, err := strconv.Atoi(fail)
			if err != nil {
				code = 503
			}
			writeJSON(w, code, map[string]any{"message": "forced failure", "statusCode": code})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func main() {
	role := os.Getenv("ROLE")
	if role == "" {
		role = "emitter"
	}
	secret := os.Getenv("WEBHOOK_SECRET")
	if secret == "" {
		secret = "shared-secret-demo"
	}
	port := envInt("PORT", 3000)

	mux := http.NewServeMux()
	if role == "receiver" {
		mux.HandleFunc("/api/hook", handleHook(secret))
	} else {
		svc := newService(secret, envInt("MAX_ATTEMPTS", 3))
		mux.HandleFunc("/api/events/publish", svc.handlePublish)
		mux.HandleFunc("/api/events/", svc.handleGet)
	}
	addr := fmt.Sprintf("0.0.0.0:%d", port)
	log.Printf("%s listening on :%d", role, port)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
