package com.starci.webhook;

import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;

/** Receiver-side verification: window check first, then constant-time compare. */
public final class WebhookVerifier {

    private static final long WINDOW_SECONDS = 300; // plus-minus 5 minutes

    private WebhookVerifier() {
    }

    /**
     * Verify the envelope. Order matters: reject an out-of-window timestamp BEFORE
     * comparing signatures, so a captured-but-stale payload is dropped even if its
     * signature was once valid.
     */
    public static void verify(String secret, String rawBody, String header) throws Exception {
        ParsedHeader p = ParsedHeader.parse(header);
        long age = Math.abs(Instant.now().getEpochSecond() - p.t());
        if (age > WINDOW_SECONDS) {
            throw new WebhookException("timestamp outside +-" + WINDOW_SECONDS + "s window (age=" + age + "s)");
        }
        String expected = WebhookSigner.sign(secret, rawBody, p.t());
        byte[] a = HexFormat.of().parseHex(expected);
        byte[] b = safeParseHex(p.v1());
        // Constant-time compare: same duration regardless of which byte differs.
        if (a.length != b.length || !MessageDigest.isEqual(a, b)) {
            throw new WebhookException("signature mismatch");
        }
    }

    private static byte[] safeParseHex(String hex) {
        try {
            return HexFormat.of().parseHex(hex);
        } catch (IllegalArgumentException ex) {
            return new byte[0];
        }
    }

    /** Parsed "t=...,v1=..." header. */
    public record ParsedHeader(long t, String v1) {
        public static ParsedHeader parse(String header) {
            String tStr = null;
            String v1 = null;
            for (String part : header.split(",")) {
                if (part.startsWith("t=")) {
                    tStr = part.substring(2);
                } else if (part.startsWith("v1=")) {
                    v1 = part.substring(3);
                }
            }
            if (tStr == null || v1 == null) {
                throw new WebhookException("signature mismatch");
            }
            try {
                return new ParsedHeader(Long.parseLong(tStr), v1);
            } catch (NumberFormatException ex) {
                throw new WebhookException("signature mismatch");
            }
        }
    }
}
