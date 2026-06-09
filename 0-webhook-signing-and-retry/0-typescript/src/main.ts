import { NestFactory } from "@nestjs/core"
import { AppModule } from "./app.module"

async function bootstrap(): Promise<void> {
    const role = process.env.ROLE ?? "emitter"
    // rawBody: true so the receiver can verify the HMAC over the exact wire bytes.
    const app = await NestFactory.create(AppModule.forRole(role), { rawBody: true })
    const port = Number(process.env.PORT ?? 3000)
    await app.listen(port, "0.0.0.0")
    // eslint-disable-next-line no-console
    console.log(`${role} listening on :${port}`)
}

void bootstrap()
