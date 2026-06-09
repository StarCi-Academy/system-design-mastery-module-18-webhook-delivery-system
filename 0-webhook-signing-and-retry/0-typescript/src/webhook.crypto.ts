import { createHmac, timingSafeEqual } from "node:crypto"

/**
 * Sign the canonical string "<timestamp>.<rawBody>" with HMAC-SHA256.
 * The timestamp is part of the signed input, so an attacker cannot push the
 * timestamp into the valid window without also recomputing the HMAC.
 */
export function sign(secret: string, rawBody: string, timestamp: number): string {
    return createHmac("sha256", secret).update(`${timestamp}.${rawBody}`).digest("hex")
}

/** Build the wire header: t=<unix>,v1=<hex>. */
export function buildSignatureHeader(timestamp: number, signature: string): string {
    return `t=${timestamp},v1=${signature}`
}

/** Parse a "t=...,v1=..." header into its parts. */
export function parseSignatureHeader(header: string): { t: number; v1: string } | null {
    const parts = header.split(",")
    const tPart = parts.find((p) => p.startsWith("t="))
    const vPart = parts.find((p) => p.startsWith("v1="))
    if (tPart === undefined || vPart === undefined) {
        return null
    }
    const t = Number.parseInt(tPart.slice(2), 10)
    if (Number.isNaN(t)) {
        return null
    }
    return { t, v1: vPart.slice(3) }
}

/** Constant-time hex comparison guarding length first. */
export function constantTimeHexEqual(expected: string, actual: string): boolean {
    if (expected.length !== actual.length) {
        return false
    }
    try {
        return timingSafeEqual(Buffer.from(expected, "hex"), Buffer.from(actual, "hex"))
    } catch {
        return false
    }
}

/**
 * Stable jitter derived from HMAC(eventId, attempt) so test runs are reproducible.
 * Returns a delay of base * (1 + f) with f in [-0.2, +0.2].
 */
export function jitter(base: number, eventId: string, attempt: number): number {
    const h = createHmac("sha256", "jitter").update(`${eventId}:${attempt}`).digest()
    const f = (h.readUInt32BE(0) / 0xffffffff) * 0.4 - 0.2
    return Math.max(0, Math.round(base * (1 + f)))
}
