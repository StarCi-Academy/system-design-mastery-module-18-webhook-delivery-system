package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

const windowSeconds int64 = 300 // plus-minus 5 minutes

// Sign produces HMAC-SHA256 over the canonical string "<timestamp>.<rawBody>".
// The timestamp is part of the signed input (not just a sibling header), so an
// attacker cannot push the timestamp into the valid window without also
// recomputing the HMAC, which requires the secret.
func Sign(secret, rawBody string, timestamp int64) string {
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%d.%s", timestamp, rawBody)
	return hex.EncodeToString(mac.Sum(nil))
}

// BuildSignatureHeader packs both the timestamp and signature: t=<unix>,v1=<hex>.
func BuildSignatureHeader(timestamp int64, signature string) string {
	return fmt.Sprintf("t=%d,v1=%s", timestamp, signature)
}

// parseHeader splits a "t=...,v1=..." header into its parts.
func parseHeader(header string) (int64, string, error) {
	var tStr, v1 string
	for _, part := range strings.Split(header, ",") {
		if strings.HasPrefix(part, "t=") {
			tStr = part[2:]
		} else if strings.HasPrefix(part, "v1=") {
			v1 = part[3:]
		}
	}
	if tStr == "" || v1 == "" {
		return 0, "", fmt.Errorf("signature mismatch")
	}
	t, err := strconv.ParseInt(tStr, 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("signature mismatch")
	}
	return t, v1, nil
}

// Verify checks the envelope. Order matters: reject an out-of-window timestamp
// BEFORE comparing signatures, so a captured-but-stale payload is dropped even
// if its signature was once valid.
func Verify(secret, rawBody, header string) error {
	t, v1, err := parseHeader(header)
	if err != nil {
		return err
	}
	age := abs(time.Now().Unix() - t)
	if age > windowSeconds {
		return fmt.Errorf("timestamp outside +-%ds window (age=%ds)", windowSeconds, age)
	}
	expected := Sign(secret, rawBody, t)
	// hmac.Equal is the constant-time comparator: same duration regardless of
	// which byte differs. Never use bytes.Equal or == for secrets.
	if !hmac.Equal([]byte(expected), []byte(v1)) {
		return fmt.Errorf("signature mismatch")
	}
	return nil
}

var backoffMs = []int64{1000, 3000} // base delay before attempt 2 and 3

// jitter derives a stable factor from HMAC(eventId, attempt) so test runs are
// reproducible. Returns base * (1 + f) with f in [-0.2, +0.2].
func jitter(base int64, eventID string, attempt int) int64 {
	mac := hmac.New(sha256.New, []byte("jitter"))
	fmt.Fprintf(mac, "%s:%d", eventID, attempt)
	h := mac.Sum(nil)
	u := uint32(h[0])<<24 | uint32(h[1])<<16 | uint32(h[2])<<8 | uint32(h[3])
	f := float64(u)/float64(^uint32(0))*0.4 - 0.2
	d := int64(math.Round(float64(base) * (1 + f)))
	if d < 0 {
		return 0
	}
	return d
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
