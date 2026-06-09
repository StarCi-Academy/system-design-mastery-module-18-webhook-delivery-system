import { Module, OnModuleInit } from "@nestjs/common"
import { ConfigModule } from "@nestjs/config"
import { InjectDataSource, TypeOrmModule } from "@nestjs/typeorm"
import { DataSource } from "typeorm"
import { ApiController } from "./api.controller"
import { CircuitService } from "./circuit.service"
import { DeliveryService } from "./delivery.service"
import { DeadLetter, OutboxEvent } from "./entities"
import { PollerService } from "./poller.service"

/**
 * Explicit schema authority, identical to the Go/C# tracks of this lesson.
 *
 * Owning the DDL here (instead of TypeORM `synchronize: true`) keeps the
 * Postgres schema deterministic across all four language tracks and avoids
 * silent drift between the entity classes and the live tables.
 */
const SCHEMA_DDL = `
CREATE TABLE IF NOT EXISTS outbox_event (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  type TEXT NOT NULL,
  payload JSONB NOT NULL,
  target_url TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INT NOT NULL DEFAULT 0,
  last_error TEXT
);
CREATE TABLE IF NOT EXISTS dead_letter (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id UUID NOT NULL UNIQUE,
  type TEXT NOT NULL,
  target_url TEXT NOT NULL,
  payload JSONB NOT NULL,
  attempts INT NOT NULL,
  last_error TEXT NOT NULL,
  parked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  replayed_at TIMESTAMPTZ
);`

@Module({
    imports: [
        ConfigModule.forRoot({ isGlobal: true }),
        TypeOrmModule.forRoot({
            type: "postgres",
            url: process.env.POSTGRES_URL ?? "postgres://webhook:webhook@postgres:5432/webhook",
            entities: [OutboxEvent, DeadLetter],
            // Schema authority is the DDL above (run in onModuleInit), not the ORM.
            synchronize: false,
        }),
        TypeOrmModule.forFeature([OutboxEvent, DeadLetter]),
    ],
    controllers: [ApiController],
    providers: [CircuitService, DeliveryService, PollerService],
})
export class AppModule implements OnModuleInit {
    constructor(@InjectDataSource() private readonly dataSource: DataSource) {}

    /**
     * Creates the outbox/dead-letter tables before the poller starts, so the
     * app boots cleanly against an empty database without ORM auto-sync.
     */
    async onModuleInit(): Promise<void> {
        await this.dataSource.query(SCHEMA_DDL)
    }
}
