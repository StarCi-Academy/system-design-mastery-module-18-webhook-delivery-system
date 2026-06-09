package com.starci.outbox.emitter;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;

import org.springframework.context.annotation.Profile;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Handles the synchronous publish API, the single-event read, and the
 * lock-claiming step of the poller. Claiming lives here (not in the poller) so
 * the {@code @Transactional} proxy applies instead of being bypassed by
 * self-invocation.
 */
@Service
@Profile("emitter")
public class EmitterService {

    private final OutboxRepository outboxRepository;
    private final OrderRepository orderRepository;

    public EmitterService(OutboxRepository outboxRepository, OrderRepository orderRepository) {
        this.outboxRepository = outboxRepository;
        this.orderRepository = orderRepository;
    }

    /**
     * Publish an event by writing the business change and the outbox row in ONE
     * transaction. Either both are durable, or neither happened — there is no
     * window where the order is committed but the webhook event is lost.
     */
    @Transactional
    public PublishResult publish(PublishInput input) {
        // Business change (here: an orders row) and the outbox row share the same txn.
        String orderRef = null;
        JsonNode payload = input.payload();
        if (payload != null && payload.hasNonNull("orderId")) {
            orderRef = payload.get("orderId").asText();
        }
        orderRepository.save(new OrderEntity(orderRef));

        OutboxEvent event = new OutboxEvent();
        event.setType(input.type());
        event.setTargetUrl(input.targetUrl());
        event.setPayload(payload);
        event.setStatus("pending"); // poller picks up pending rows on the next tick
        event.setAttempts(0);
        outboxRepository.save(event);
        return new PublishResult(event.getId().toString(), event.getStatus());
    }

    /**
     * Claim a batch of pending rows and flip them to in_flight inside the SAME
     * transaction (dirty checking flushes the update before commit) so the next
     * tick never re-claims them.
     */
    @Transactional
    public List<OutboxEvent> claimBatch(int batch) {
        List<OutboxEvent> rows = outboxRepository.claimPending(PageRequest.of(0, batch));
        Instant now = Instant.now();
        rows.forEach(r -> {
            r.setStatus("in_flight");
            r.setUpdatedAt(now);
        });
        return rows;
    }

    public Optional<EventDto> getEvent(String id) {
        UUID uuid;
        try {
            uuid = UUID.fromString(id);
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
        return outboxRepository.findById(uuid).map(e -> new EventDto(
                e.getId().toString(),
                e.getStatus(),
                e.getAttempts(),
                e.getLastError(),
                e.getCreatedAt().toString(),
                e.getUpdatedAt().toString()));
    }
}
