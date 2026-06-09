package com.starci.outbox.emitter;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {

    /**
     * Claim a batch of pending rows. PESSIMISTIC_WRITE plus the hint
     * jakarta.persistence.lock.timeout = -2 makes Hibernate emit Postgres
     * {@code FOR UPDATE SKIP LOCKED}: two pollers see disjoint batches.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints({ @QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2") })
    @Query("SELECT e FROM OutboxEvent e WHERE e.status = 'pending' ORDER BY e.createdAt ASC")
    List<OutboxEvent> claimPending(Pageable pageable);

    @Modifying
    @Transactional
    @Query("UPDATE OutboxEvent e SET e.status = 'sent', e.lastError = null, e.updatedAt = :now WHERE e.id = :id")
    void markSent(@Param("id") UUID id, @Param("now") Instant now);

    @Modifying
    @Transactional
    @Query("UPDATE OutboxEvent e SET e.status = :status, e.attempts = e.attempts + 1, e.lastError = :err, e.updatedAt = :now WHERE e.id = :id")
    void markRetry(@Param("id") UUID id, @Param("status") String status, @Param("err") String err, @Param("now") Instant now);

    /** Watchdog: reset rows stuck in_flight (the emitter crashed mid-delivery). */
    @Modifying
    @Transactional
    @Query("UPDATE OutboxEvent e SET e.status = 'pending', e.updatedAt = :now WHERE e.status = 'in_flight' AND e.updatedAt < :threshold")
    int resetStuck(@Param("now") Instant now, @Param("threshold") Instant threshold);
}
