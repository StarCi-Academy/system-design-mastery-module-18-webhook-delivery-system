package com.starci.outbox.emitter;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

/**
 * The background poller: every 500ms it claims a batch of pending rows (via the
 * lock-claiming method on EmitterService), delivers each on a small executor with
 * retry/backoff, and every couple of seconds a watchdog resets rows stuck
 * in_flight so a crash never loses an event.
 */
@Service
@Profile("emitter")
public class PollerService {

    private static final Logger log = LoggerFactory.getLogger(PollerService.class);
    private static final int BATCH = 10;
    private static final int MAX_ATTEMPTS = 6;
    private static final Duration IN_FLIGHT_TIMEOUT = Duration.ofSeconds(4);

    private final EmitterService emitterService;
    private final OutboxRepository outboxRepository;
    private final RestTemplate restTemplate;
    private final ExecutorService executor;
    private final ObjectMapper objectMapper;

    public PollerService(EmitterService emitterService, OutboxRepository outboxRepository,
                         RestTemplate restTemplate, ExecutorService executor, ObjectMapper objectMapper) {
        this.emitterService = emitterService;
        this.outboxRepository = outboxRepository;
        this.restTemplate = restTemplate;
        this.executor = executor;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelay = 500)
    public void poll() {
        try {
            List<OutboxEvent> rows = emitterService.claimBatch(BATCH);
            for (OutboxEvent ev : rows) {
                executor.submit(() -> deliverOne(ev));
            }
        } catch (Exception e) {
            log.error("poll error: {}", e.getMessage());
        }
    }

    private void deliverOne(OutboxEvent ev) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("Idempotency-Key", ev.getId().toString());
            HttpEntity<String> req = new HttpEntity<>(objectMapper.writeValueAsString(ev.getPayload()), headers);
            ResponseEntity<String> resp = restTemplate.postForEntity(ev.getTargetUrl(), req, String.class);
            if (resp.getStatusCode().is2xxSuccessful()) {
                outboxRepository.markSent(ev.getId(), Instant.now());
            } else {
                throw new RuntimeException("receiver HTTP " + resp.getStatusCode().value());
            }
        } catch (Exception err) {
            String next = ev.getAttempts() + 1 >= MAX_ATTEMPTS ? "failed" : "pending";
            outboxRepository.markRetry(ev.getId(), next, err.getMessage(), Instant.now());
        }
    }

    @Scheduled(fixedDelay = 2000)
    public void watchdog() {
        try {
            Instant now = Instant.now();
            outboxRepository.resetStuck(now, now.minus(IN_FLIGHT_TIMEOUT));
        } catch (Exception e) {
            log.error("watchdog error: {}", e.getMessage());
        }
    }
}
