package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/starci-academy/webhook-dlq-circuit/internal/circuit"
)

// Handler exposes the HTTP contract shared by all four language tracks.
type Handler struct {
	pool    *pgxpool.Pool
	circuit *circuit.Service
}

func New(pool *pgxpool.Pool, c *circuit.Service) *Handler { return &Handler{pool: pool, circuit: c} }

func (h *Handler) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/events/publish", h.publish)
	mux.HandleFunc("/api/dlq", h.listDLQ)
	mux.HandleFunc("/api/dlq/", h.replay) // /api/dlq/:id/replay
	mux.HandleFunc("/api/circuit/status", h.circuitStatus)
	return mux
}

type publishReq struct {
	Type      string                 `json:"type"`
	Payload   map[string]interface{} `json:"payload"`
	TargetURL string                 `json:"targetUrl"`
}

func (h *Handler) publish(w http.ResponseWriter, r *http.Request) {
	var req publishReq
	json.NewDecoder(r.Body).Decode(&req)
	payload, _ := json.Marshal(req.Payload)
	var id string
	h.pool.QueryRow(r.Context(),
		`INSERT INTO outbox_event (type, payload, target_url, status) VALUES ($1,$2,$3,'pending') RETURNING id`,
		req.Type, payload, req.TargetURL).Scan(&id)
	writeJSON(w, http.StatusOK, map[string]string{"eventId": id, "status": "pending"})
}

func (h *Handler) listDLQ(w http.ResponseWriter, r *http.Request) {
	rows, _ := h.pool.Query(r.Context(),
		`SELECT id, event_id, type, target_url, payload, attempts, last_error, parked_at, replayed_at
		 FROM dead_letter ORDER BY parked_at DESC`)
	defer rows.Close()
	out := []map[string]interface{}{}
	for rows.Next() {
		var id, eid, typ, target, lastErr string
		var payload []byte
		var attempts int
		var parked time.Time
		var replayed *time.Time
		rows.Scan(&id, &eid, &typ, &target, &payload, &attempts, &lastErr, &parked, &replayed)
		var p map[string]interface{}
		json.Unmarshal(payload, &p)
		out = append(out, map[string]interface{}{
			"id": id, "eventId": eid, "type": typ, "targetUrl": target, "payload": p,
			"attempts": attempts, "lastError": lastErr, "parkedAt": parked, "replayedAt": replayed,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) replay(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/dlq/"), "/")
	if len(parts) < 2 || parts[1] != "replay" {
		http.NotFound(w, r)
		return
	}
	id := parts[0]
	var eventID string
	err := h.pool.QueryRow(r.Context(), `SELECT event_id FROM dead_letter WHERE id=$1`, id).Scan(&eventID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"replayedDlq": id, "status": "not_found", "note": "no such DLQ row"})
		return
	}
	h.pool.Exec(r.Context(), `UPDATE dead_letter SET replayed_at=now() WHERE id=$1`, id)
	h.pool.Exec(r.Context(),
		`UPDATE outbox_event SET status='pending', attempts=0, last_error=NULL WHERE id=$1`, eventID)
	writeJSON(w, http.StatusOK, map[string]string{
		"replayedDlq": id, "eventId": eventID, "status": "requeued",
		"note": "Outbox row reset to pending; poller will re-enqueue within ~500ms.",
	})
}

func (h *Handler) circuitStatus(w http.ResponseWriter, r *http.Request) {
	host := r.URL.Query().Get("host")
	writeJSON(w, http.StatusOK, h.circuit.Status(context.Background(), host))
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
