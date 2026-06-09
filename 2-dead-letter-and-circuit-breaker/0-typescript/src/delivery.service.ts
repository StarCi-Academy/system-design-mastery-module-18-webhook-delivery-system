import { Injectable, OnModuleDestroy, OnModuleInit } from "@nestjs/common"
import { InjectRepository } from "@nestjs/typeorm"
import { ConnectionOptions, DelayedError, Job, Queue, Worker } from "bullmq"
import { Repository } from "typeorm"
import { CircuitService } from "./circuit.service"
import { DeadLetter, OutboxEvent } from "./entities"

const QUEUE_NAME = "webhook-delivery"
const MAX_ATTEMPTS = 3

interface DeliverData {
    outboxId: string
    type: string
    targetUrl: string
    payload: Record<string, unknown>
}

/** Parse REDIS_URL into a BullMQ connection options object. */
function redisConnection(): ConnectionOptions {
    const url = new URL(process.env.REDIS_URL ?? "redis://redis:6379")
    return { host: url.hostname, port: Number(url.port || 6379) }
}

/**
 * Owns the BullMQ queue + worker that delivers webhooks. The worker consults the
 * circuit breaker before every send; when the circuit blocks a job it reschedules
 * the job and throws DelayedError so BullMQ does NOT burn a retry attempt.
 */
@Injectable()
export class DeliveryService implements OnModuleInit, OnModuleDestroy {
    private readonly connection: ConnectionOptions = redisConnection()
    private queue!: Queue<DeliverData>
    private worker!: Worker<DeliverData>

    constructor(
        @InjectRepository(OutboxEvent) private readonly outboxRepo: Repository<OutboxEvent>,
        @InjectRepository(DeadLetter) private readonly dlqRepo: Repository<DeadLetter>,
        private readonly circuit: CircuitService,
    ) {}

    async onModuleInit(): Promise<void> {
        this.queue = new Queue<DeliverData>(QUEUE_NAME, { connection: this.connection })
        this.worker = new Worker<DeliverData>(QUEUE_NAME, this.process.bind(this), {
            connection: this.connection,
            concurrency: 4,
        })
        this.worker.on("failed", () => {
            // Failures are handled inline in process(); this hook avoids unhandled rejections.
        })
    }

    /** Enqueue one delivery job for a pending outbox row. */
    async enqueue(event: OutboxEvent): Promise<void> {
        await this.queue.add(
            "deliver",
            {
                outboxId: event.id,
                type: event.type,
                targetUrl: event.targetUrl,
                payload: event.payload,
            },
            { attempts: MAX_ATTEMPTS, backoff: { type: "fixed", delay: 1000 } },
        )
    }

    /**
     * The delivery worker. Gate on the circuit first: a blocked job is rescheduled
     * via moveToDelayed + DelayedError (no attempt burned). Otherwise POST to the
     * receiver; success closes the circuit, failure records it and, when attempts
     * are exhausted, parks the event into the dead_letter table.
     */
    private async process(job: Job<DeliverData>, token?: string): Promise<void> {
        const host = new URL(job.data.targetUrl).host
        const gate = await this.circuit.allowRequest(host)
        if (!gate.allow) {
            await job.moveToDelayed(Date.now() + 5000, token)
            throw new DelayedError()
        }

        try {
            await this.deliver(job.data)
            await this.circuit.recordSuccess(host)
            await this.outboxRepo.update({ id: job.data.outboxId }, { status: "sent" })
        } catch (err) {
            const message = err instanceof Error ? err.message : String(err)
            await this.circuit.recordFailure(host, gate.isProbe)

            const attemptsMade = (job.attemptsMade ?? 0) + 1
            const isFinal = attemptsMade >= (job.opts.attempts ?? MAX_ATTEMPTS)
            await this.outboxRepo.update(
                { id: job.data.outboxId },
                { attempts: attemptsMade, lastError: message },
            )

            if (isFinal) {
                await this.outboxRepo.update({ id: job.data.outboxId }, { status: "failed" })
                await this.park(job.data, attemptsMade, message)
            }
            throw err
        }
    }

    /** Real HTTP POST to the receiver. A dead host throws ENOTFOUND. */
    private async deliver(data: DeliverData): Promise<void> {
        const res = await fetch(data.targetUrl, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(data.payload),
        })
        if (!res.ok) throw new Error(`receiver responded ${res.status}`)
    }

    /** Park a terminal failure into dead_letter (idempotent per eventId). */
    private async park(data: DeliverData, attempts: number, lastError: string): Promise<void> {
        const existing = await this.dlqRepo.findOne({ where: { eventId: data.outboxId } })
        if (existing) return
        await this.dlqRepo.save(
            this.dlqRepo.create({
                eventId: data.outboxId,
                type: data.type,
                targetUrl: data.targetUrl,
                payload: data.payload,
                attempts,
                lastError,
                replayedAt: null,
            }),
        )
    }

    async onModuleDestroy(): Promise<void> {
        await this.worker?.close()
        await this.queue?.close()
    }
}
