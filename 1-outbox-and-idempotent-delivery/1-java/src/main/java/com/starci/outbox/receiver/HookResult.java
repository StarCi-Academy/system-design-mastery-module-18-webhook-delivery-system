package com.starci.outbox.receiver;

import com.fasterxml.jackson.databind.JsonNode;

/** receiver-mock successful hook response body. */
public record HookResult(
        boolean received,
        String eventId,
        String idempotencyKey,
        String sideEffectExecutedAt,
        JsonNode bodyEcho) {
}
