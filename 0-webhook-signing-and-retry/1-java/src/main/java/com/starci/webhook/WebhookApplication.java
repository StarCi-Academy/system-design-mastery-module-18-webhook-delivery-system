package com.starci.webhook;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Entry point. The same jar runs as emitter or receiver based on the ROLE env var. */
@SpringBootApplication
public class WebhookApplication {
    public static void main(String[] args) {
        String role = System.getenv().getOrDefault("ROLE", "emitter");
        System.out.println(role + " starting");
        SpringApplication.run(WebhookApplication.class, args);
    }
}
