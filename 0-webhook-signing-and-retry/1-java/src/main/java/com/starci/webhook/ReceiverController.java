package com.starci.webhook;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** Receiver-mock HTTP surface; only registered when ROLE == receiver. */
@RestController
@RequestMapping("/api/hook")
@ConditionalOnProperty(name = "ROLE", havingValue = "receiver")
public class ReceiverController {

    private final String secret;

    public ReceiverController(@Value("${WEBHOOK_SECRET:shared-secret-demo}") String secret) {
        this.secret = secret;
    }

    @PostMapping
    public ResponseEntity<?> hook(
            @RequestBody(required = false) String rawBody,
            @RequestParam(name = "fail", required = false) String fail,
            HttpServletRequest request) throws Exception {
        String body = rawBody == null ? "" : rawBody;
        String header = request.getHeader("X-Webhook-Signature");
        if (header == null) {
            header = "";
        }
        try {
            WebhookVerifier.verify(secret, body, header);
        } catch (WebhookException ex) {
            return ResponseEntity.status(401)
                    .body(Map.of("message", ex.getMessage(), "error", "Unauthorized", "statusCode", 401));
        }
        // Chaos hook: force a specific status code AFTER a valid signature.
        if (fail != null && !fail.isEmpty()) {
            int code;
            try {
                code = Integer.parseInt(fail);
            } catch (NumberFormatException ex) {
                code = 503;
            }
            return ResponseEntity.status(code)
                    .body(Map.of("message", "forced failure", "statusCode", code));
        }
        return ResponseEntity.ok(Map.of("ok", true));
    }
}
