import { BadRequestException, Body, Controller, Get, Param, Post, Query } from "@nestjs/common"
import { InjectRepository } from "@nestjs/typeorm"
import { Repository } from "typeorm"
import { CircuitService, FAIL_THRESHOLD, OPEN_TTL_SEC } from "./circuit.service"
import { DeadLetter, OutboxEvent } from "./entities"

interface PublishDto {
    type: string
    payload: Record<string, unknown>
    targetUrl: string
}

@Controller("api")
export class ApiController {
    constructor(
        @InjectRepository(OutboxEvent) private readonly outboxRepo: Repository<OutboxEvent>,
        @InjectRepository(DeadLetter) private readonly dlqRepo: Repository<DeadLetter>,
        private readonly circuit: CircuitService,
    ) {}

    /** Publish writes one durable pending outbox row; the poller takes it from there. */
    @Post("events/publish")
    async publish(@Body() dto: PublishDto): Promise<{ eventId: string; status: string }> {
        const row = await this.outboxRepo.save(
            this.outboxRepo.create({
                type: dto.type,
                payload: dto.payload,
                targetUrl: dto.targetUrl,
                status: "pending",
            }),
        )
        return { eventId: row.id, status: "pending" }
    }

    /** List parked dead-letter rows, newest first. */
    @Get("dlq")
    async listDlq(): Promise<DeadLetter[]> {
        return this.dlqRepo.find({ order: { parkedAt: "DESC" } })
    }

    /**
     * Manual replay: stamp replayedAt, then flip the original outbox row back to
     * pending so the poller re-enqueues it. The DLQ row stays for audit.
     */
    @Post("dlq/:id/replay")
    async replay(
        @Param("id") id: string,
    ): Promise<{ replayedDlq: string; eventId: string; status: string; note: string }> {
        const row = await this.dlqRepo.findOne({ where: { id } })
        if (!row) {
            throw new BadRequestException("no such DLQ row")
        }
        await this.dlqRepo.update({ id }, { replayedAt: new Date() })
        await this.outboxRepo.update(
            { id: row.eventId },
            { status: "pending", attempts: 0, lastError: null },
        )
        return {
            replayedDlq: id,
            eventId: row.eventId,
            status: "requeued",
            note: "Outbox row reset to pending; poller will re-enqueue within ~500ms.",
        }
    }

    /** Inspect the circuit state for one host. */
    @Get("circuit/status")
    async circuitStatus(@Query("host") host: string): Promise<{
        host: string
        state: string
        failCount: number
        failThreshold: number
        openTtlSec: number
    }> {
        return {
            host,
            state: await this.circuit.getState(host),
            failCount: await this.circuit.failCount(host),
            failThreshold: FAIL_THRESHOLD,
            openTtlSec: OPEN_TTL_SEC,
        }
    }
}
