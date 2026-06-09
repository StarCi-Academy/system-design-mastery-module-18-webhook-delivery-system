package com.starci.webhook;

/** Raised when verification fails; mapped to HTTP 401 by the receiver. */
public class WebhookException extends RuntimeException {
    public WebhookException(String message) {
        super(message);
    }
}
