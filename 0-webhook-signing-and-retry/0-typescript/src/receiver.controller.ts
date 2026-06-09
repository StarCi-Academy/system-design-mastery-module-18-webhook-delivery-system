import { Controller, HttpCode, HttpException, Post, Query, Req, UnauthorizedException } from "@nestjs/common"
import { ConfigService } from "@nestjs/config"
import type { Request } from "express"
import { constantTimeHexEqual, parseSignatureHeader, sign } from "./webhook.crypto"

const WINDOW_SECONDS = 300

/** Forces a specific status code AFTER a valid signature, for chaos testing. */
class ForcedFailure extends HttpException {
    public constructor(status: number) {
        super({ message: "forced failure", statusCode: status }, status)
    }
}

@Controller("api/hook")
export class ReceiverController {
    private readonly secret: string

    public constructor(config: ConfigService) {
        this.secret = config.get<string>("WEBHOOK_SECRET", "shared-secret-demo")
    }

    @Post()
    @HttpCode(200)
    public hook(@Req() req: Request, @Query("fail") fail?: string): unknown {
        // rawBody is populated because the app is bootstrapped with rawBody: true.
        const rawBody = (req as Request & { rawBody?: Buffer }).rawBody?.toString("utf8") ?? ""
        const header = req.header("X-Webhook-Signature") ?? ""
        const parsed = parseSignatureHeader(header)
        if (parsed === null) {
            throw new UnauthorizedException("signature mismatch")
        }
        // 1) Window check FIRST: reject stale/future timestamps before comparing signatures.
        const age = Math.abs(Math.floor(Date.now() / 1000) - parsed.t)
        if (age > WINDOW_SECONDS) {
            throw new UnauthorizedException(`timestamp outside +-${WINDOW_SECONDS}s window (age=${age}s)`)
        }
        // 2) Constant-time signature comparison.
        const expected = sign(this.secret, rawBody, parsed.t)
        if (!constantTimeHexEqual(expected, parsed.v1)) {
            throw new UnauthorizedException("signature mismatch")
        }
        // Chaos hook: force a specific status code AFTER a valid signature.
        if (fail !== undefined) {
            const code = Number.parseInt(fail, 10)
            throw new ForcedFailure(Number.isNaN(code) ? 503 : code)
        }
        return { ok: true }
    }
}
