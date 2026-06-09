import { NestFactory } from "@nestjs/core"
import { AppModule } from "./app.module"

/**
 * Boots the emitter HTTP server inside the Docker container.
 *
 * @returns A promise that resolves once the server is accepting connections.
 */
async function bootstrap(): Promise<void> {
    const app = await NestFactory.create(AppModule)
    const port = Number(process.env.PORT ?? 3000)
    // Bind 0.0.0.0 so the host can reach the container. The NestJS default
    // (127.0.0.1) only listens on the container loopback, which makes host
    // curl/Invoke-RestMethod fail with connection refused.
    await app.listen(port, "0.0.0.0")
    // eslint-disable-next-line no-console
    console.log(`emitter listening on :${port}`)
}

void bootstrap()
