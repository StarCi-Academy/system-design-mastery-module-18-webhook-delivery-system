import { DynamicModule, Module } from "@nestjs/common"
import { ConfigModule } from "@nestjs/config"
import { EmitterController } from "./emitter.controller"
import { EmitterService } from "./emitter.service"
import { ReceiverController } from "./receiver.controller"

@Module({})
export class AppModule {
    /**
     * The same image runs as either the emitter or the receiver-mock depending on
     * the ROLE env var, so the compose stack ships one Dockerfile for both services.
     */
    public static forRole(role: string): DynamicModule {
        const isReceiver = role === "receiver"
        return {
            module: AppModule,
            imports: [ConfigModule.forRoot({ isGlobal: true })],
            controllers: isReceiver ? [ReceiverController] : [EmitterController],
            providers: isReceiver ? [] : [EmitterService],
        }
    }
}
