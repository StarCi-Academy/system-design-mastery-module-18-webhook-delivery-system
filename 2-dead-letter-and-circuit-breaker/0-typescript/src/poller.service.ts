import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from "@nestjs/common"
import { InjectRepository } from "@nestjs/typeorm"
import { Repository } from "typeorm"
import { CircuitService } from "./circuit.service"
import { DeliveryService } from "./delivery.service"
import { OutboxEvent } from "./entities"

/**
 * Outbox poller: claims pending rows and enqueues delivery jobs. Also runs the
 * circuit sweeper, which flips expired open circuits to half_open — the single
 * place where the state machine self-advances toward recovery.
 */
@Injectable()
export class PollerService implements OnModuleInit, OnModuleDestroy {
    private readonly logger = new Logger("PollerService")
    private pollTimer?: NodeJS.Timeout
    private sweepTimer?: NodeJS.Timeout

    constructor(
        @InjectRepository(OutboxEvent) private readonly outboxRepo: Repository<OutboxEvent>,
        private readonly delivery: DeliveryService,
        private readonly circuit: CircuitService,
    ) {}

    onModuleInit(): void {
        this.pollTimer = setInterval(() => void this.poll(), 500)
        this.sweepTimer = setInterval(() => void this.sweep(), 2000)
    }

    private async poll(): Promise<void> {
        const pending = await this.outboxRepo.find({ where: { status: "pending" }, take: 50 })
        for (const ev of pending) {
            await this.outboxRepo.update({ id: ev.id }, { status: "delivering" })
            await this.delivery.enqueue(ev)
        }
    }

    /**
     * The circuit sweeper. We track hosts known to be open; once Redis reports
     * their open key has expired (ttl < 0), we flip them to half_open so the next
     * job becomes the single recovery probe.
     */
    private async sweep(): Promise<void> {
        const hosts = new Set<string>()
        for (const ev of await this.outboxRepo.find({ take: 200 })) {
            try {
                hosts.add(new URL(ev.targetUrl).host)
            } catch {
                // ignore malformed URLs
            }
        }
        for (const host of hosts) {
            const state = await this.circuit.getState(host)
            if (state === "open" && (await this.circuit.openTtlMs(host)) < 0) {
                await this.circuit.forceHalfOpen(host)
                this.logger.log(`circuit ${host} -> HALF_OPEN`)
            }
        }
    }

    onModuleDestroy(): void {
        if (this.pollTimer) clearInterval(this.pollTimer)
        if (this.sweepTimer) clearInterval(this.sweepTimer)
    }
}
