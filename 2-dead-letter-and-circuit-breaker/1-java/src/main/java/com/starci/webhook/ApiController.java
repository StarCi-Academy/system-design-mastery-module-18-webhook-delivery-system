package com.starci.webhook;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api")
public class ApiController {
    private final OutboxRepository outbox;
    private final DeadLetterRepository dlq;
    private final CircuitService circuit;

    ApiController(OutboxRepository outbox, DeadLetterRepository dlq, CircuitService circuit) {
        this.outbox = outbox;
        this.dlq = dlq;
        this.circuit = circuit;
    }

    @PostMapping("/events/publish")
    public Map<String, Object> publish(@RequestBody PublishDto dto) {
        OutboxEvent ev = new OutboxEvent();
        ev.type = dto.type();
        ev.payload = dto.payload();
        ev.targetUrl = dto.targetUrl();
        ev.status = "pending";
        outbox.save(ev);
        return Map.of("eventId", ev.id.toString(), "status", "pending");
    }

    @GetMapping("/dlq")
    public List<DeadLetter> listDlq() {
        return dlq.findAllByOrderByParkedAtDesc();
    }

    @PostMapping("/dlq/{id}/replay")
    public Map<String, Object> replay(@PathVariable UUID id) {
        return dlq.findById(id).map(row -> {
            row.replayedAt = OffsetDateTime.now();
            dlq.save(row);
            outbox.findById(row.eventId).ifPresent(ev -> {
                ev.status = "pending";
                ev.attempts = 0;
                ev.lastError = null;
                outbox.save(ev);
            });
            return Map.<String, Object>of(
                    "replayedDlq", id.toString(),
                    "eventId", row.eventId.toString(),
                    "status", "requeued",
                    "note", "Outbox row reset to pending; poller will re-enqueue within ~500ms.");
        }).orElseThrow(() -> new ResponseStatusException(HttpStatus.BAD_REQUEST, "no such DLQ row"));
    }

    @GetMapping("/circuit/status")
    public Map<String, Object> circuitStatus(@RequestParam String host) {
        long ttl = circuit.openTtlMillis(host);
        return Map.of(
                "host", host,
                "state", circuit.getState(host),
                "failCount", circuit.failCount(host),
                "failThreshold", CircuitService.FAIL_THRESHOLD,
                "openTtlSec", ttl > 0 ? (int) (ttl / 1000) : 0);
    }

    record PublishDto(String type, Map<String, Object> payload, String targetUrl) {}

    @SuppressWarnings("unused")
    private static String hostOf(String url) {
        URI u = URI.create(url);
        return u.getHost();
    }
}
