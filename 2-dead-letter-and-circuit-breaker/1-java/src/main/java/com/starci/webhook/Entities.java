package com.starci.webhook;

import jakarta.persistence.*;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/** outbox_event: durable hot path; status moves pending -> sent | failed | back to pending on replay. */
@Entity
@Table(name = "outbox_event")
class OutboxEvent {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    UUID id;
    String type;
    @Column(columnDefinition = "jsonb")
    @org.hibernate.annotations.JdbcTypeCode(org.hibernate.type.SqlTypes.JSON)
    Map<String, Object> payload;
    @Column(name = "target_url")
    String targetUrl;
    String status = "pending";
    int attempts = 0;
    @Column(name = "last_error")
    String lastError;

    // Public getters so Jackson serializes every field; without them the GET
    // endpoints emit empty objects ([{}]) because the fields are package-private.
    public UUID getId() { return id; }
    public String getType() { return type; }
    public Map<String, Object> getPayload() { return payload; }
    public String getTargetUrl() { return targetUrl; }
    public String getStatus() { return status; }
    public int getAttempts() { return attempts; }
    public String getLastError() { return lastError; }
}

/** dead_letter: operator-facing slow path; monitorable, alertable, replayable. */
@Entity
@Table(name = "dead_letter")
class DeadLetter {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    UUID id;
    @Column(name = "event_id", unique = true)
    UUID eventId;
    String type;
    @Column(name = "target_url")
    String targetUrl;
    @Column(columnDefinition = "jsonb")
    @org.hibernate.annotations.JdbcTypeCode(org.hibernate.type.SqlTypes.JSON)
    Map<String, Object> payload;
    int attempts;
    @Column(name = "last_error")
    String lastError;
    @Column(name = "parked_at")
    OffsetDateTime parkedAt = OffsetDateTime.now();
    @Column(name = "replayed_at")
    OffsetDateTime replayedAt;

    // Public getters so GET /api/dlq serializes the full row instead of [{}].
    public UUID getId() { return id; }
    public UUID getEventId() { return eventId; }
    public String getType() { return type; }
    public String getTargetUrl() { return targetUrl; }
    public Map<String, Object> getPayload() { return payload; }
    public int getAttempts() { return attempts; }
    public String getLastError() { return lastError; }
    public OffsetDateTime getParkedAt() { return parkedAt; }
    public OffsetDateTime getReplayedAt() { return replayedAt; }
}
