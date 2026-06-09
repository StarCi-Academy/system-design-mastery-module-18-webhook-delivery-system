package com.starci.outbox.emitter;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.client.RestTemplate;

/** Emitter-only beans: HTTP client, the send executor, scheduling, and the schema bootstrap. */
@Configuration
@Profile("emitter")
@EnableScheduling
public class EmitterConfig {

    private static final Logger log = LoggerFactory.getLogger(EmitterConfig.class);

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Bean(destroyMethod = "shutdown")
    public ExecutorService deliveryExecutor() {
        return Executors.newFixedThreadPool(4);
    }

    /** Create the schema if it does not exist (idempotent), retrying until Postgres is up. */
    @Bean
    public CommandLineRunner schemaInitializer(JdbcTemplate jdbc) {
        return args -> {
            for (int i = 0; i < 30; i++) {
                try {
                    jdbc.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto");
                    jdbc.execute("""
                        CREATE TABLE IF NOT EXISTS orders (
                            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            ref VARCHAR(128)
                        )""");
                    jdbc.execute("""
                        CREATE TABLE IF NOT EXISTS outbox_event (
                            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            type VARCHAR(128) NOT NULL,
                            target_url VARCHAR(512) NOT NULL,
                            payload JSONB NOT NULL,
                            status VARCHAR(16) NOT NULL DEFAULT 'pending',
                            attempts INT NOT NULL DEFAULT 0,
                            last_error TEXT,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                        )""");
                    jdbc.execute("CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox_event(status, created_at)");
                    return;
                } catch (Exception e) {
                    log.warn("schema init attempt {} failed: {}", i + 1, e.getMessage());
                    Thread.sleep(1000);
                }
            }
            throw new IllegalStateException("postgres not reachable after 30 attempts");
        };
    }
}
