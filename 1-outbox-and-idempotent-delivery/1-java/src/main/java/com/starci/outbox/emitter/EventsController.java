package com.starci.outbox.emitter;

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

/**
 * The emitter HTTP surface: publish, read-back, and a hook-proxy that forwards to
 * receiver-mock (used by the idempotency-replay flow from the host).
 */
@RestController
@Profile("emitter")
public class EventsController {

    private static final int PROXY_MAX_ATTEMPTS = 12;

    private final EmitterService emitterService;
    private final RestTemplate restTemplate;
    private final String receiverUrl;

    public EventsController(EmitterService emitterService, RestTemplate restTemplate,
                            @Value("${app.receiver-url}") String receiverUrl) {
        this.emitterService = emitterService;
        this.restTemplate = restTemplate;
        this.receiverUrl = receiverUrl;
    }

    @PostMapping("/api/events/publish")
    public ResponseEntity<PublishResult> publish(@RequestBody PublishInput input) {
        return ResponseEntity.status(201).body(emitterService.publish(input));
    }

    @GetMapping("/api/events/{id}")
    public ResponseEntity<?> get(@PathVariable String id) {
        return emitterService.getEvent(id)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(404)
                        .body(Map.of("statusCode", 404, "message", "Event " + id + " not found")));
    }

    /**
     * Forward a hook call to receiver-mock with the client's Idempotency-Key,
     * retrying on a 5xx (the receiver fails the first N attempts) so the caller
     * sees the first successful response and, on a replay, the same cached one.
     */
    @PostMapping("/api/hook-proxy")
    public ResponseEntity<String> hookProxy(@RequestBody(required = false) String body,
                                            @RequestHeader(value = "Idempotency-Key", required = false) String idemKey) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (idemKey != null) headers.set("Idempotency-Key", idemKey);
        HttpEntity<String> req = new HttpEntity<>(body == null ? "" : body, headers);

        for (int i = 0; i < PROXY_MAX_ATTEMPTS; i++) {
            try {
                ResponseEntity<String> resp = restTemplate.postForEntity(receiverUrl, req, String.class);
                return ResponseEntity.status(resp.getStatusCode())
                        .contentType(MediaType.APPLICATION_JSON).body(resp.getBody());
            } catch (HttpStatusCodeException ex) {
                int sc = ex.getStatusCode().value();
                if (sc < 500) {
                    return ResponseEntity.status(sc)
                            .contentType(MediaType.APPLICATION_JSON).body(ex.getResponseBodyAsString());
                }
                // 5xx (simulated failure) — fall through to backoff and retry.
            } catch (Exception ignored) {
                // transient — retry
            }
            try {
                Thread.sleep(300);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return ResponseEntity.status(502).contentType(MediaType.APPLICATION_JSON).body("{\"statusCode\":502}");
    }
}
