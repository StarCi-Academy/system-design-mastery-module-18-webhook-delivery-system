using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace WebhookSigning;

public sealed class AttemptRecord
{
    [JsonPropertyName("attempt")] public int Attempt { get; set; }
    [JsonPropertyName("plannedDelayMs")] public long PlannedDelayMs { get; set; }
    [JsonPropertyName("status")] public int Status { get; set; }
}

public sealed class EventRecord
{
    [JsonPropertyName("eventId")] public string EventId { get; set; } = "";
    [JsonPropertyName("type")] public string Type { get; set; } = "";
    [JsonPropertyName("targetUrl")] public string TargetUrl { get; set; } = "";
    [JsonPropertyName("payload")] public JsonElement Payload { get; set; }
    [JsonPropertyName("finalStatus")] public string FinalStatus { get; set; } = "pending";
    [JsonPropertyName("attempts")] public List<AttemptRecord> Attempts { get; } = [];
}

public sealed class PublishDto
{
    [JsonPropertyName("type")] public string Type { get; set; } = "";
    [JsonPropertyName("payload")] public JsonElement Payload { get; set; }
    [JsonPropertyName("targetUrl")] public string TargetUrl { get; set; } = "";
}

/// <summary>Emitter: stores events and delivers them with retry off the request path.</summary>
public sealed class EmitterService
{
    private static readonly long[] BackoffMs = [1000, 3000]; // base delay before attempt 2 and 3
    private readonly string _secret;
    private readonly int _maxAttempts;
    private readonly HttpClient _httpClient = new() { Timeout = TimeSpan.FromSeconds(5) };
    private readonly ConcurrentDictionary<string, EventRecord> _store = new();

    public EmitterService(IConfiguration config)
    {
        _secret = config["WEBHOOK_SECRET"] ?? "shared-secret-demo";
        _maxAttempts = int.TryParse(config["MAX_ATTEMPTS"], out int m) ? m : 3;
    }

    public EventRecord Publish(PublishDto dto)
    {
        var record = new EventRecord
        {
            EventId = Guid.NewGuid().ToString(),
            Type = dto.Type,
            TargetUrl = dto.TargetUrl,
            Payload = dto.Payload,
            FinalStatus = "pending",
        };
        _store[record.EventId] = record;
        string body = dto.Payload.GetRawText();
        // Fire-and-forget: delivery runs off the request path.
        _ = Task.Run(() => DeliverWithRetryAsync(record, body));
        return record;
    }

    public EventRecord? Get(string eventId) => _store.GetValueOrDefault(eventId);

    private long Jitter(long baseMs, string eventId, int attempt)
    {
        using var hmac = new System.Security.Cryptography.HMACSHA256(Encoding.UTF8.GetBytes("jitter"));
        byte[] h = hmac.ComputeHash(Encoding.UTF8.GetBytes($"{eventId}:{attempt}"));
        uint u = (uint)((h[0] << 24) | (h[1] << 16) | (h[2] << 8) | h[3]);
        double f = ((double)u / uint.MaxValue) * 0.4 - 0.2; // factor in [-0.2, +0.2]
        return Math.Max(0, (long)Math.Round(baseMs * (1 + f)));
    }

    private async Task DeliverWithRetryAsync(EventRecord record, string body)
    {
        for (int attempt = 1; attempt <= _maxAttempts; attempt++)
        {
            // Re-sign with a FRESH timestamp every attempt so a late retry stays in-window.
            long ts = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            string header = WebhookSigner.BuildSignatureHeader(ts, WebhookSigner.Sign(_secret, body, ts));
            long plannedDelayMs = attempt == 1
                ? 0
                : BackoffMs[attempt - 2] + Jitter(BackoffMs[attempt - 2], record.EventId, attempt);
            if (plannedDelayMs > 0)
            {
                await Task.Delay((int)plannedDelayMs);
            }
            int status = 0;
            try
            {
                using var req = new HttpRequestMessage(HttpMethod.Post, record.TargetUrl)
                {
                    Content = new StringContent(body, Encoding.UTF8, "application/json"),
                };
                req.Headers.TryAddWithoutValidation("X-Webhook-Id", record.EventId);
                req.Headers.TryAddWithoutValidation("X-Webhook-Timestamp", ts.ToString());
                req.Headers.TryAddWithoutValidation("X-Webhook-Signature", header);
                HttpResponseMessage res = await _httpClient.SendAsync(req);
                status = (int)res.StatusCode;
            }
            catch
            {
                status = 0;
            }
            record.Attempts.Add(new AttemptRecord { Attempt = attempt, PlannedDelayMs = plannedDelayMs, Status = status });
            if (status is >= 200 and < 300)
            {
                record.FinalStatus = "delivered";
                return;
            }
        }
        record.FinalStatus = "failed";
    }
}
