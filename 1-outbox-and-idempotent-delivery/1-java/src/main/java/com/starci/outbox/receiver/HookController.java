package com.starci.outbox.receiver;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/**
 * receiver-mock: an idempotent webhook receiver. It caches the first successful
 * response with SET ... NX EX and replays it byte-identical, rejects a key reused
 * with a different body (409), and fails the first N attempts per key to demo retry.
 */
@RestController
@Profile("receiver")
public class HookController {

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final int failFirst;
    private final long ttlSeconds;

    public HookController(StringRedisTemplate redis, ObjectMapper objectMapper,
                          @Value("${app.fail-first-n-attempts}") int failFirst,
                          @Value("${app.idem-ttl-seconds}") long ttlSeconds) {
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.failFirst = failFirst;
        this.ttlSeconds = ttlSeconds;
    }

    @PostMapping("/api/hook")
    public ResponseEntity<String> hook(@RequestBody(required = false) String rawBody,
                                       @RequestHeader(value = "Idempotency-Key", required = false) String idemKey) throws Exception {
        if (rawBody == null) rawBody = "";
        if (idemKey == null || idemKey.isEmpty()) {
            return json(400, "{\"statusCode\":400,\"message\":\"missing Idempotency-Key\"}", false);
        }

        String bodyHash = sha256(rawBody);
        String cacheKey = "hook:idem:" + idemKey + ":" + bodyHash;
        String seenHashKey = "hook:idem:hash:" + idemKey;

        // Cache hit → replay the first response verbatim; the side-effect does NOT run again.
        String cached = redis.opsForValue().get(cacheKey);
        if (cached != null) {
            return json(200, cached, true);
        }

        // Same key + different body → 409 (body-hash conflict gate).
        String priorHash = redis.opsForValue().get(seenHashKey);
        if (priorHash != null && !priorHash.equals(bodyHash)) {
            return json(409, "{\"statusCode\":409,\"message\":\"Idempotency-Key reused with a different body\"}", false);
        }

        // Demo retry: count attempts per key; fail the first N.
        String attemptKey = "hook:attempt:" + idemKey;
        Long attempt = redis.opsForValue().increment(attemptKey);
        redis.expire(attemptKey, Duration.ofSeconds(ttlSeconds));
        if (attempt != null && attempt <= failFirst) {
            return json(503, "{\"statusCode\":503,\"message\":\"simulated failure\"}", false);
        }

        // Cache miss → run the side-effect exactly once and capture its result.
        JsonNode bodyEcho = tryParse(rawBody);
        String eventId = (bodyEcho != null && bodyEcho.hasNonNull("id")) ? bodyEcho.get("id").asText() : idemKey;
        HookResult result = new HookResult(true, eventId, idemKey, Instant.now().toString(), bodyEcho);
        String payload = objectMapper.writeValueAsString(result);

        // SET ... NX EX: only the winner writes; a racing first-time request reads the
        // winner's value instead of double-running the side-effect.
        Boolean won = redis.opsForValue().setIfAbsent(cacheKey, payload, ttlSeconds, TimeUnit.SECONDS);
        redis.opsForValue().set(seenHashKey, bodyHash, ttlSeconds, TimeUnit.SECONDS);
        if (Boolean.FALSE.equals(won)) {
            String winner = redis.opsForValue().get(cacheKey);
            if (winner != null) return json(200, winner, true);
        }
        return json(200, payload, false);
    }

    private ResponseEntity<String> json(int status, String body, boolean replayed) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (replayed) headers.set("Idempotent-Replayed", "true");
        return ResponseEntity.status(status).headers(headers).body(body);
    }

    private JsonNode tryParse(String raw) {
        try {
            return objectMapper.readTree(raw);
        } catch (Exception e) {
            return null;
        }
    }

    private static String sha256(String s) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] digest = md.digest(s.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
