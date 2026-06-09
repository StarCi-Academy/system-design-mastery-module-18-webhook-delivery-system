package com.starci.webhook;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import java.time.Duration;

/**
 * Per-endpoint circuit breaker backed by Redis so state is shared across replicas.
 * Mirrors the Resilience4j three-state model (closed/open/half_open) but persists
 * transitions in Redis keys, which a single-process Resilience4j registry cannot do.
 */
@Service
public class CircuitService {
    public static final int FAIL_THRESHOLD = 3;
    public static final int OPEN_TTL_SEC = 20;
    public static final int PROBE_TTL_SEC = 5;
    public static final int FAIL_WINDOW_SEC = 60;

    private final StringRedisTemplate redis;

    CircuitService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    private String stateKey(String h) { return "circuit:state:" + h; }
    private String failKey(String h) { return "circuit:fail:" + h; }
    private String probeKey(String h) { return "circuit:probe:" + h; }
    private String openUntilKey(String h) { return "circuit:openUntil:" + h; }

    public String getState(String host) {
        String v = redis.opsForValue().get(stateKey(host));
        if ("open".equals(v)) return "open";
        if ("half_open".equals(v)) return "half_open";
        return "closed";
    }

    /** Returns true if the job may proceed; half_open admits one probe via SET NX. */
    public Gate allowRequest(String host) {
        String state = getState(host);
        if ("closed".equals(state)) return new Gate(true, state, false);
        if ("half_open".equals(state)) {
            Boolean won = redis.opsForValue()
                    .setIfAbsent(probeKey(host), "1", Duration.ofSeconds(PROBE_TTL_SEC));
            boolean ok = Boolean.TRUE.equals(won);
            return new Gate(ok, state, ok);
        }
        return new Gate(false, state, false);
    }

    private void trip(String host) {
        redis.opsForValue().set(stateKey(host), "open");
        long until = System.currentTimeMillis() + OPEN_TTL_SEC * 1000L;
        redis.opsForValue().set(openUntilKey(host), Long.toString(until));
    }

    public void recordSuccess(String host) {
        redis.delete(java.util.List.of(stateKey(host), failKey(host), probeKey(host), openUntilKey(host)));
    }

    public void recordFailure(String host, boolean isProbe) {
        if (isProbe) {
            trip(host);
            redis.delete(probeKey(host));
            return;
        }
        Long fails = redis.opsForValue().increment(failKey(host));
        redis.expire(failKey(host), Duration.ofSeconds(FAIL_WINDOW_SEC));
        if (fails != null && fails >= FAIL_THRESHOLD) {
            trip(host);
        }
    }

    public void forceHalfOpen(String host) {
        redis.opsForValue().set(stateKey(host), "half_open");
        redis.delete(openUntilKey(host));
    }

    public long openTtlMillis(String host) {
        String v = redis.opsForValue().get(openUntilKey(host));
        if (v == null) return -1;
        return Long.parseLong(v) - System.currentTimeMillis();
    }

    public int failCount(String host) {
        String v = redis.opsForValue().get(failKey(host));
        return v == null ? 0 : Integer.parseInt(v);
    }

    public record Gate(boolean allow, String state, boolean isProbe) {}
}
