package com.starci.webhook;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {
    List<OutboxEvent> findTop50ByStatus(String status);
}

interface DeadLetterRepository extends JpaRepository<DeadLetter, UUID> {
    List<DeadLetter> findAllByOrderByParkedAtDesc();
    Optional<DeadLetter> findByEventId(UUID eventId);
}
