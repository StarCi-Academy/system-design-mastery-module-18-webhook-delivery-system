package com.starci.webhook;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/** Emitter: stores events and delivers them with retry off the request path. */
@Service
@ConditionalOnProperty(name = "ROLE", havingValue = "emitter", matchIfMissing = true)
public class EmitterService {

    private static final long[] BACKOFF_MS = {1000, 3000}; // base delay before attempt 2 and 3

    private final String secret;
    private final int maxAttempts;
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private final Map<String, Records.EventRecord> store = new ConcurrentHashMap<>();
    private final ObjectMapper mapper = new ObjectMapper();

    public EmitterService(
            @Value("${WEBHOOK_SECRET:shared-secret-demo}") String secret,
            @Value("${MAX_ATTEMPTS:3}") int maxAttempts) {
        this.secret = secret;
        this.maxAttempts = maxAttempts;
    }

    public Records.EventRecord publish(Records.PublishDto dto) throws Exception {
        Records.EventRecord record = new Records.EventRecord();
        record.eventId = UUID.randomUUID().toString();
        record.type = dto.type();
        record.targetUrl = dto.targetUrl();
        record.payload = dto.payload();
        store.put(record.eventId, record);
        String body = mapper.writeValueAsString(dto.payload());
        // Fire-and-forget: delivery runs off the request path.
        pool.submit(() -> deliverWithRetry(record, body));
        return record;
    }

    public Records.EventRecord get(String eventId) {
        return store.get(eventId);
    }

    private long jitter(long base, String eventId, int attempt) throws Exception {
        byte[] h = WebhookSigner.macBytes("jitter", eventId + ":" + attempt);
        long u = ((long) (h[0] & 0xff) << 24) | ((h[1] & 0xff) << 16) | ((h[2] & 0xff) << 8) | (h[3] & 0xff);
        double f = ((double) u / 0xffffffffL) * 0.4 - 0.2; // factor in [-0.2, +0.2]
        return Math.max(0, Math.round(base * (1 + f)));
    }

    private void deliverWithRetry(Records.EventRecord record, String body) {
        try {
            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                // Re-sign with a FRESH timestamp every attempt so a late retry stays in-window.
                long ts = Instant.now().getEpochSecond();
                String header = WebhookSigner.buildSignatureHeader(ts, WebhookSigner.sign(secret, body, ts));
                long plannedDelayMs = attempt == 1
                        ? 0
                        : BACKOFF_MS[attempt - 2] + jitter(BACKOFF_MS[attempt - 2], record.eventId, attempt);
                if (plannedDelayMs > 0) {
                    Thread.sleep(plannedDelayMs);
                }
                int status = 0;
                try {
                    HttpRequest req = HttpRequest.newBuilder(URI.create(record.targetUrl))
                            .header("X-Webhook-Id", record.eventId)
                            .header("X-Webhook-Timestamp", String.valueOf(ts))
                            .header("X-Webhook-Signature", header)
                            .header("Content-Type", "application/json")
                            .timeout(Duration.ofSeconds(5))
                            .POST(HttpRequest.BodyPublishers.ofString(body))
                            .build();
                    HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
                    status = res.statusCode();
                } catch (Exception ex) {
                    status = 0;
                }
                record.attempts.add(new Records.AttemptRecord(attempt, plannedDelayMs, status));
                if (status >= 200 && status < 300) {
                    record.finalStatus = "delivered";
                    return;
                }
            }
            record.finalStatus = "failed";
        } catch (Exception ex) {
            record.finalStatus = "failed";
        }
    }
}
