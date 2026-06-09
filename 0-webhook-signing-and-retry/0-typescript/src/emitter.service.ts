import { Injectable } from "@nestjs/common"
import { ConfigService } from "@nestjs/config"
import axios from "axios"
import { v4 as uuidv4 } from "uuid"
import { buildSignatureHeader, jitter, sign } from "./webhook.crypto"

interface AttemptRecord {
    attempt: number
    plannedDelayMs: number
    status: number
}

interface EventRecord {
    eventId: string
    type: string
    targetUrl: string
    payload: unknown
    finalStatus: "pending" | "delivered" | "failed"
    attempts: AttemptRecord[]
}

@Injectable()
export class EmitterService {
    private readonly secret: string
    private readonly maxAttempts: number
    private readonly backoffMs: number[] = [1000, 3000]
    private readonly store = new Map<string, EventRecord>()

    public constructor(config: ConfigService) {
        this.secret = config.get<string>("WEBHOOK_SECRET", "shared-secret-demo")
        this.maxAttempts = Number(config.get<string>("MAX_ATTEMPTS", "3"))
    }

    public publish(input: { type: string; payload: unknown; targetUrl: string }): EventRecord {
        const eventId = uuidv4()
        const record: EventRecord = {
            eventId,
            type: input.type,
            targetUrl: input.targetUrl,
            payload: input.payload,
            finalStatus: "pending",
            attempts: [],
        }
        this.store.set(eventId, record)
        // Fire-and-forget: delivery runs off the request path.
        void this.deliverWithRetry(record)
        return record
    }

    public get(eventId: string): EventRecord | undefined {
        return this.store.get(eventId)
    }

    private async deliverWithRetry(record: EventRecord): Promise<void> {
        const body = JSON.stringify(record.payload)
        for (let attempt = 1; attempt <= this.maxAttempts; attempt++) {
            const ts = Math.floor(Date.now() / 1000)
            const signature = sign(this.secret, body, ts)
            const header = buildSignatureHeader(ts, signature)
            const plannedDelayMs =
                attempt === 1
                    ? 0
                    : this.backoffMs[attempt - 2] + jitter(this.backoffMs[attempt - 2], record.eventId, attempt)
            if (plannedDelayMs > 0) {
                await new Promise((r) => setTimeout(r, plannedDelayMs))
            }
            let status = 0
            try {
                const res = await axios.post(record.targetUrl, body, {
                    headers: {
                        "X-Webhook-Id": record.eventId,
                        "X-Webhook-Timestamp": String(ts),
                        "X-Webhook-Signature": header,
                        "Content-Type": "application/json",
                    },
                    validateStatus: () => true,
                    timeout: 5000,
                })
                status = res.status
            } catch {
                status = 0
            }
            record.attempts.push({ attempt, plannedDelayMs, status })
            if (status >= 200 && status < 300) {
                record.finalStatus = "delivered"
                return
            }
        }
        record.finalStatus = "failed"
    }
}
