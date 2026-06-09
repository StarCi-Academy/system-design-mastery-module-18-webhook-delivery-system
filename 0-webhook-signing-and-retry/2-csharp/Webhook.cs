using System.Security.Cryptography;
using System.Text;

namespace WebhookSigning;

/// <summary>Raised when verification fails; mapped to HTTP 401 by the receiver.</summary>
public sealed class WebhookException(string message) : Exception(message);

public static class WebhookSigner
{
    /// <summary>
    /// Sign the canonical string "&lt;timestamp&gt;.&lt;rawBody&gt;" with HMAC-SHA256.
    /// The timestamp is part of the signed input (not just a sibling header), so an
    /// attacker cannot push the timestamp into the valid window without also
    /// recomputing the HMAC, which needs the secret.
    /// </summary>
    public static string Sign(string secret, string rawBody, long timestamp)
    {
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(secret));
        byte[] raw = hmac.ComputeHash(Encoding.UTF8.GetBytes($"{timestamp}.{rawBody}"));
        return Convert.ToHexString(raw).ToLowerInvariant();
    }

    // The wire header packs both the timestamp and the signature: t=<unix>,v1=<hex>.
    public static string BuildSignatureHeader(long timestamp, string signature)
        => $"t={timestamp},v1={signature}";
}

public static class WebhookVerifier
{
    private const long WindowSeconds = 300; // plus-minus 5 minutes

    /// <summary>
    /// Verify the envelope. Order matters: reject an out-of-window timestamp BEFORE
    /// comparing signatures, so a captured-but-stale payload is dropped even if its
    /// signature was once valid.
    /// </summary>
    public static void Verify(string secret, string rawBody, string header)
    {
        var (t, v1) = ParseHeader(header);
        long age = Math.Abs(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - t);
        if (age > WindowSeconds)
        {
            throw new WebhookException($"timestamp outside +-{WindowSeconds}s window (age={age}s)");
        }
        string expected = WebhookSigner.Sign(secret, rawBody, t);
        byte[] a = Convert.FromHexString(expected);
        byte[] b = SafeFromHex(v1);
        // Constant-time compare: same duration regardless of which byte differs.
        if (a.Length != b.Length || !CryptographicOperations.FixedTimeEquals(a, b))
        {
            throw new WebhookException("signature mismatch");
        }
    }

    private static (long t, string v1) ParseHeader(string header)
    {
        string? tStr = null, v1 = null;
        foreach (string part in header.Split(','))
        {
            if (part.StartsWith("t=", StringComparison.Ordinal)) tStr = part[2..];
            else if (part.StartsWith("v1=", StringComparison.Ordinal)) v1 = part[3..];
        }
        if (tStr is null || v1 is null || !long.TryParse(tStr, out long t))
        {
            throw new WebhookException("signature mismatch");
        }
        return (t, v1);
    }

    private static byte[] SafeFromHex(string hex)
    {
        try { return Convert.FromHexString(hex); }
        catch { return []; }
    }
}
