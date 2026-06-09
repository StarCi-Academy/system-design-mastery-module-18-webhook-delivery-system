package com.starci.webhook;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * Outbox poller + delivery worker + circuit sweeper. The circuit guards every
 * send; a circuit-blocked job is skipped without burning an attempt — the JVM
 * analog of BullMQ's DelayedError soft-skip.
 */
@Service
public class DeliveryService {
    private static final int MAX_ATTEMPTS = 3;

    private final OutboxRepository outbox;
    private final DeadLetterRepository dlq;
    private final CircuitService circuit;
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3)).build();

    DeliveryService(OutboxRepository outbox, DeadLetterRepository dlq, CircuitService circuit) {
        this.outbox = outbox;
        this.dlq = dlq;
        this.circuit = circuit;
    }

    @Scheduled(fixedDelay = 500)
    public void poll() {
        for (OutboxEvent ev : outbox.findTop50ByStatus("pending")) {
            ev.status = "delivering";
            outbox.save(ev);
            deliver(ev);
        }
    }

    @Transactional
    void deliver(OutboxEvent ev) {
        String host = URI.create(ev.targetUrl).getHost() + portSuffix(ev.targetUrl);
        CircuitService.Gate gate = circuit.allowRequest(host);
        if (!gate.allow()) {
            ev.status = "pending"; // reschedule without burning an attempt
            outbox.save(ev);
            return;
        }
        try {
            send(ev);
            circuit.recordSuccess(host);
            ev.status = "sent";
            outbox.save(ev);
        } catch (Exception e) {
            circuit.recordFailure(host, gate.isProbe());
            ev.attempts += 1;
            // e.getMessage() is null for some IOException subtypes (e.g. ConnectException);
            // fall back to the class name so last_error is never empty in dead_letter.
            ev.lastError = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            if (ev.attempts >= MAX_ATTEMPTS) {
                ev.status = "failed";
                park(ev);
            } else {
                ev.status = "pending";
            }
            outbox.save(ev);
        }
    }

    private void send(OutboxEvent ev) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(ev.targetUrl))
                .timeout(Duration.ofSeconds(3))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(ev.payload.toString()))
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 400) throw new RuntimeException("receiver responded " + resp.statusCode());
    }

    private void park(OutboxEvent ev) {
        if (dlq.findByEventId(ev.id).isPresent()) return;
        DeadLetter d = new DeadLetter();
        d.eventId = ev.id;
        d.type = ev.type;
        d.targetUrl = ev.targetUrl;
        d.payload = ev.payload;
        d.attempts = ev.attempts;
        d.lastError = ev.lastError;
        dlq.save(d);
    }

    @Scheduled(fixedDelay = 2000)
    public void sweep() {
        Set<String> hosts = new HashSet<>();
        for (OutboxEvent ev : outbox.findAll()) {
            hosts.add(URI.create(ev.targetUrl).getHost() + portSuffix(ev.targetUrl));
        }
        for (String host : hosts) {
            if ("open".equals(circuit.getState(host)) && circuit.openTtlMillis(host) < 0) {
                circuit.forceHalfOpen(host);
            }
        }
    }

    private String portSuffix(String url) {
        int port = URI.create(url).getPort();
        return port < 0 ? "" : ":" + port;
    }
}
