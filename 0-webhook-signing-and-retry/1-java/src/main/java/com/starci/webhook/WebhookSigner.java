package com.starci.webhook;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

/** HMAC-SHA256 signing of the canonical "&lt;timestamp&gt;.&lt;rawBody&gt;" envelope. */
public final class WebhookSigner {

    private WebhookSigner() {
    }

    /**
     * Sign the canonical string "&lt;timestamp&gt;.&lt;rawBody&gt;" with HMAC-SHA256.
     * The timestamp is part of the signed input (not just a sibling header), so an
     * attacker cannot push the timestamp into the valid window without also
     * recomputing the HMAC, which needs the secret. Mac is NOT thread-safe, so a
     * fresh instance is created per call.
     */
    public static String sign(String secret, String rawBody, long timestamp) throws Exception {
        return HexFormat.of().formatHex(macBytes(secret, timestamp + "." + rawBody));
    }

    /** Raw HMAC-SHA256 bytes of the given message under the given key. */
    public static byte[] macBytes(String key, String message) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return mac.doFinal(message.getBytes(StandardCharsets.UTF_8));
    }

    /** The wire header packs both the timestamp and the signature: t=&lt;unix&gt;,v1=&lt;hex&gt;. */
    public static String buildSignatureHeader(long timestamp, String signature) {
        return "t=" + timestamp + ",v1=" + signature;
    }
}
