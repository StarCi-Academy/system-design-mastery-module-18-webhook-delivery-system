package com.starci.outbox;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Single Spring Boot application running in one of two roles selected by the
 * active profile: {@code emitter} (publish API + outbox poller) or
 * {@code receiver} (idempotent webhook receiver-mock).
 */
@SpringBootApplication
public class OutboxApplication {
    public static void main(String[] args) {
        SpringApplication.run(OutboxApplication.class, args);
    }
}
