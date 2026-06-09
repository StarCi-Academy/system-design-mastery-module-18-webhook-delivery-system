package com.starci.webhook;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/** DTO + storage records for the emitter. */
public final class Records {

    private Records() {
    }

    /** Inbound publish request. */
    public record PublishDto(String type, JsonNode payload, String targetUrl) {
    }

    /** One delivery attempt. */
    public static final class AttemptRecord {
        public int attempt;
        public long plannedDelayMs;
        public int status;

        public AttemptRecord(int attempt, long plannedDelayMs, int status) {
            this.attempt = attempt;
            this.plannedDelayMs = plannedDelayMs;
            this.status = status;
        }

        public int getAttempt() {
            return attempt;
        }

        public long getPlannedDelayMs() {
            return plannedDelayMs;
        }

        public int getStatus() {
            return status;
        }
    }

    /** Stored state of a published event. */
    public static final class EventRecord {
        public String eventId;
        public String type;
        public String targetUrl;
        public JsonNode payload;
        public String finalStatus = "pending";
        public List<AttemptRecord> attempts = new ArrayList<>();

        public String getEventId() {
            return eventId;
        }

        public String getType() {
            return type;
        }

        public String getTargetUrl() {
            return targetUrl;
        }

        public JsonNode getPayload() {
            return payload;
        }

        public String getFinalStatus() {
            return finalStatus;
        }

        public List<AttemptRecord> getAttempts() {
            return attempts;
        }
    }
}
