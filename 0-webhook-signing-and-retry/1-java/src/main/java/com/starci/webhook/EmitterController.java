package com.starci.webhook;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** Emitter HTTP surface; only registered when ROLE != receiver. */
@RestController
@RequestMapping("/api/events")
@ConditionalOnProperty(name = "ROLE", havingValue = "emitter", matchIfMissing = true)
public class EmitterController {

    private final EmitterService emitter;

    public EmitterController(EmitterService emitter) {
        this.emitter = emitter;
    }

    @PostMapping("/publish")
    public ResponseEntity<Records.EventRecord> publish(@RequestBody Records.PublishDto body) throws Exception {
        return ResponseEntity.status(HttpStatus.CREATED).body(emitter.publish(body));
    }

    @GetMapping("/{eventId}")
    public ResponseEntity<?> get(@PathVariable String eventId) {
        Records.EventRecord record = emitter.get(eventId);
        if (record == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("message", "event " + eventId + " not found", "statusCode", 404));
        }
        return ResponseEntity.ok(record);
    }
}
