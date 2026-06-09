using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Mvc;
using StackExchange.Redis;

namespace Outbox.Controllers;

/// <summary>receiver-mock: an idempotent webhook receiver. It caches the first
/// successful response with SET ... NX EX and replays it byte-identical, rejects a
/// key reused with a different body (409), and fails the first N attempts per key.</summary>
[ApiController]
public class HookController : ControllerBase
{
    private readonly IConnectionMultiplexer _redis;
    private readonly int _failFirst;
    private readonly TimeSpan _ttl;

    public HookController(IConnectionMultiplexer redis, IConfiguration config)
    {
        _redis = redis;
        _failFirst = config.GetValue("FailFirstNAttempts", 0);
        _ttl = TimeSpan.FromSeconds(config.GetValue("IdemTtlSeconds", 86400));
    }

    [HttpPost("/api/hook")]
    public async Task<IActionResult> Hook()
    {
        var db = _redis.GetDatabase();
        using var reader = new StreamReader(Request.Body);
        var rawBody = await reader.ReadToEndAsync();
        var idemKey = Request.Headers["Idempotency-Key"].ToString();
        if (string.IsNullOrEmpty(idemKey))
            return StatusCode(400, new { statusCode = 400, message = "missing Idempotency-Key" });

        var bodyHash = Sha256(rawBody);
        var cacheKey = $"hook:idem:{idemKey}:{bodyHash}";
        var seenHashKey = $"hook:idem:hash:{idemKey}";

        // Cache hit → replay the first response verbatim; the side-effect does NOT run again.
        var cached = await db.StringGetAsync(cacheKey);
        if (cached.HasValue)
            return Replay(cached!);

        // Same key + different body → 409 (body-hash conflict gate).
        var priorHash = await db.StringGetAsync(seenHashKey);
        if (priorHash.HasValue && priorHash != bodyHash)
            return StatusCode(409, new { statusCode = 409, message = "Idempotency-Key reused with a different body" });

        // Demo retry: count attempts per key; fail the first N.
        var attemptKey = $"hook:attempt:{idemKey}";
        var attempt = await db.StringIncrementAsync(attemptKey);
        await db.KeyExpireAsync(attemptKey, _ttl);
        if (attempt <= _failFirst)
            return StatusCode(503, new { statusCode = 503, message = "simulated failure" });

        // Cache miss → run the side-effect exactly once and capture its result.
        var bodyEcho = TryParse(rawBody);
        var eventId = bodyEcho?["id"]?.GetValue<string>() ?? idemKey;
        var result = new HookResult(true, eventId, idemKey, DateTime.UtcNow.ToString("o"), bodyEcho);
        var payload = JsonSerializer.Serialize(result, Json.Options);

        // SET ... NX EX: only the winner writes; a racing first-time request reads the
        // winner's value instead of double-running the side-effect.
        var won = await db.StringSetAsync(cacheKey, payload, _ttl, When.NotExists);
        await db.StringSetAsync(seenHashKey, bodyHash, _ttl);
        if (!won)
        {
            var winner = await db.StringGetAsync(cacheKey);
            if (winner.HasValue) return Replay(winner!);
        }
        return new ContentResult { StatusCode = 200, Content = payload, ContentType = "application/json" };
    }

    private ContentResult Replay(string body)
    {
        Response.Headers["Idempotent-Replayed"] = "true";
        return new ContentResult { StatusCode = 200, Content = body, ContentType = "application/json" };
    }

    private static JsonNode? TryParse(string raw)
    {
        try { return JsonNode.Parse(raw); }
        catch { return null; }
    }

    private static string Sha256(string s)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(s));
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }
}
