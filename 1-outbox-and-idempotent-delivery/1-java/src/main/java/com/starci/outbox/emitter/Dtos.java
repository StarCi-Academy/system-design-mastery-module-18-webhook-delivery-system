package com.starci.outbox.emitter;

import com.fasterxml.jackson.databind.JsonNode;

/** POST /api/events/publish request body. */
record PublishInput(String type, String targetUrl, JsonNode payload) { }

/** 201 response body. */
record PublishResult(String eventId, String status) { }

/** GET /api/events/:id 200 response body. */
record EventDto(String id, String status, int attempts, String lastError, String createdAt, String updatedAt) { }
