import { Body, Controller, Get, NotFoundException, Param, Post } from "@nestjs/common"
import { EmitterService } from "./emitter.service"

interface PublishDto {
    type: string
    payload: unknown
    targetUrl: string
}

@Controller("api/events")
export class EmitterController {
    public constructor(private readonly emitter: EmitterService) {}

    @Post("publish")
    public publish(@Body() body: PublishDto): unknown {
        return this.emitter.publish({ type: body.type, payload: body.payload, targetUrl: body.targetUrl })
    }

    @Get(":eventId")
    public get(@Param("eventId") eventId: string): unknown {
        const record = this.emitter.get(eventId)
        if (record === undefined) {
            throw new NotFoundException(`event ${eventId} not found`)
        }
        return record
    }
}
