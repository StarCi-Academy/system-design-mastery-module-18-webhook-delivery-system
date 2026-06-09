import { Injectable, OnModuleDestroy } from "@nestjs/common"
import Redis from "ioredis"

export type CircuitState = "closed" | "open" | "half_open"

export const FAIL_THRESHOLD = 3
export const OPEN_TTL_SEC = 20
export const PROBE_TTL_SEC = 5
export const FAIL_WINDOW_SEC = 60

/**
 * Per-endpoint circuit breaker backed by Redis. Three keys per host:
 *   circuit:state:<host>  -> "open" | "half_open" (absent = closed)
 *   circuit:fail:<host>   -> consecutive failure counter (TTL FAIL_WINDOW_SEC)
 *   circuit:probe:<host>  -> single-probe lock for the half_open state
 * State is kept in Redis (not in-process) so every worker replica shares one view.
 */
@Injectable()
export class CircuitService implements OnModuleDestroy {
    private readonly redis: Redis

    constructor() {
        this.redis = new Redis(process.env.REDIS_URL ?? "redis://redis:6379")
    }

    private stateKey(host: string) {
        return `circuit:state:${host}`
    }
    private failKey(host: string) {
        return `circuit:fail:${host}`
    }
    private probeKey(host: string) {
        return `circuit:probe:${host}`
    }
    private openUntilKey(host: string) {
        return `circuit:openUntil:${host}`
    }

    /** Trip the host to open and stamp when its cool-down ends (read by the sweeper). */
    private async trip(host: string): Promise<void> {
        await this.redis.set(this.stateKey(host), "open")
        await this.redis.set(this.openUntilKey(host), String(Date.now() + OPEN_TTL_SEC * 1000))
    }

    async getState(host: string): Promise<CircuitState> {
        const raw = await this.redis.get(this.stateKey(host))
        if (raw === "open") return "open"
        if (raw === "half_open") return "half_open"
        return "closed"
    }

    /**
     * Decide whether a job for this host may proceed.
     * closed: always allow. open: always block. half_open: allow exactly one
     * probe via SET NX so concurrent workers cannot stampede the recovering host.
     */
    async allowRequest(
        host: string,
    ): Promise<{ allow: boolean; state: CircuitState; isProbe: boolean }> {
        const state = await this.getState(host)
        if (state === "closed") return { allow: true, state, isProbe: false }
        if (state === "half_open") {
            const ok = await this.redis.set(this.probeKey(host), "1", "EX", PROBE_TTL_SEC, "NX")
            return { allow: ok === "OK", state, isProbe: ok === "OK" }
        }
        return { allow: false, state, isProbe: false }
    }

    /** Success closes the circuit and clears every key for the host. */
    async recordSuccess(host: string): Promise<void> {
        await this.redis.del(
            this.stateKey(host),
            this.failKey(host),
            this.probeKey(host),
            this.openUntilKey(host),
        )
    }

    /**
     * A failure bumps the consecutive counter. Once it reaches the threshold the
     * host trips to open with a cool-down window. A failed half_open probe re-opens.
     */
    async recordFailure(host: string, isProbe: boolean): Promise<void> {
        if (isProbe) {
            await this.trip(host)
            await this.redis.del(this.probeKey(host))
            return
        }
        const res = await this.redis
            .multi()
            .incr(this.failKey(host))
            .expire(this.failKey(host), FAIL_WINDOW_SEC)
            .exec()
        const fails = Number(res?.[0]?.[1] ?? 0)
        if (fails >= FAIL_THRESHOLD) {
            await this.trip(host)
        }
    }

    /** Sweeper hook: flip an expired open circuit to half_open for one probe. */
    async forceHalfOpen(host: string): Promise<void> {
        await this.redis.set(this.stateKey(host), "half_open")
        await this.redis.del(this.openUntilKey(host))
    }

    async failCount(host: string): Promise<number> {
        return Number((await this.redis.get(this.failKey(host))) ?? 0)
    }

    /** Milliseconds remaining in the open cool-down; negative once it has elapsed. */
    async openTtlMs(host: string): Promise<number> {
        const until = await this.redis.get(this.openUntilKey(host))
        if (!until) return -1
        return Number(until) - Date.now()
    }

    /** Whole seconds remaining in the open cool-down (for the status endpoint). */
    async openTtl(host: string): Promise<number> {
        return Math.ceil((await this.openTtlMs(host)) / 1000)
    }

    async onModuleDestroy(): Promise<void> {
        await this.redis.quit()
    }
}
