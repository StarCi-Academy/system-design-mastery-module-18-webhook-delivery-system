import { Column, Entity, PrimaryGeneratedColumn } from "typeorm"

/**
 * outbox_event is the durable hot path: a publish writes one row here, the poller
 * picks pending rows and enqueues a delivery job. status moves
 * pending -> failed (terminal) or back to pending on a manual replay.
 */
@Entity({ name: "outbox_event" })
export class OutboxEvent {
    @PrimaryGeneratedColumn("uuid")
    id: string

    @Column({ type: "text" })
    type: string

    @Column({ type: "jsonb" })
    payload: Record<string, unknown>

    @Column({ name: "target_url", type: "text" })
    targetUrl: string

    // pending | delivering | sent | failed
    @Column({ type: "text", default: "pending" })
    status: string

    @Column({ type: "int", default: 0 })
    attempts: number

    @Column({ name: "last_error", type: "text", nullable: true })
    lastError: string | null
}

/**
 * dead_letter is the operator-facing slow path: a terminal delivery failure parks
 * a forensic copy here. It is monitorable, alertable, and replayable independently
 * of the outbox hot path.
 */
@Entity({ name: "dead_letter" })
export class DeadLetter {
    @PrimaryGeneratedColumn("uuid")
    id: string

    @Column({ name: "event_id", type: "uuid" })
    eventId: string

    @Column({ type: "text" })
    type: string

    @Column({ name: "target_url", type: "text" })
    targetUrl: string

    @Column({ type: "jsonb" })
    payload: Record<string, unknown>

    @Column({ type: "int" })
    attempts: number

    @Column({ name: "last_error", type: "text" })
    lastError: string

    @Column({ name: "parked_at", type: "timestamptz", default: () => "now()" })
    parkedAt: Date

    @Column({ name: "replayed_at", type: "timestamptz", nullable: true })
    replayedAt: Date | null
}
