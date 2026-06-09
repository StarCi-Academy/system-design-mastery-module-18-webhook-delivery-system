package com.starci.outbox.emitter;

import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** A business stand-in row written in the same transaction as the outbox row. */
@Entity
@Table(name = "orders")
public class OrderEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "ref")
    private String ref;

    public OrderEntity() { }

    public OrderEntity(String ref) { this.ref = ref; }

    public UUID getId() { return id; }
    public String getRef() { return ref; }
}
