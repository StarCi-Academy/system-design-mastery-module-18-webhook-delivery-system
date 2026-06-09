using StackExchange.Redis;

namespace WebhookDlqCircuit;

/// <summary>
/// Per-endpoint circuit breaker backed by Redis. This mirrors Polly's three-state
/// breaker but persists state in Redis so every worker replica shares one view —
/// something an in-process Polly ResiliencePipelineRegistry cannot do alone.
/// </summary>
public class CircuitService
{
    public const int FailThreshold = 3;
    public const int OpenTtlSec = 20;
    public const int ProbeTtlSec = 5;
    public const int FailWindowSec = 60;

    private readonly IDatabase _db;

    public CircuitService(IConnectionMultiplexer redis) => _db = redis.GetDatabase();

    private static string StateKey(string h) => $"circuit:state:{h}";
    private static string FailKey(string h) => $"circuit:fail:{h}";
    private static string ProbeKey(string h) => $"circuit:probe:{h}";
    private static string OpenUntilKey(string h) => $"circuit:openUntil:{h}";

    public async Task<string> GetStateAsync(string host)
    {
        var v = await _db.StringGetAsync(StateKey(host));
        return v == "open" ? "open" : v == "half_open" ? "half_open" : "closed";
    }

    public record Gate(bool Allow, string State, bool IsProbe);

    /// <summary>half_open admits exactly one probe via SET NX so workers cannot stampede.</summary>
    public async Task<Gate> AllowRequestAsync(string host)
    {
        var state = await GetStateAsync(host);
        if (state == "closed") return new Gate(true, state, false);
        if (state == "half_open")
        {
            var won = await _db.StringSetAsync(ProbeKey(host), "1",
                TimeSpan.FromSeconds(ProbeTtlSec), When.NotExists);
            return new Gate(won, state, won);
        }
        return new Gate(false, state, false);
    }

    private async Task TripAsync(string host)
    {
        await _db.StringSetAsync(StateKey(host), "open");
        var until = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + OpenTtlSec * 1000L;
        await _db.StringSetAsync(OpenUntilKey(host), until.ToString());
    }

    public async Task RecordSuccessAsync(string host) =>
        await _db.KeyDeleteAsync(new RedisKey[]
            { StateKey(host), FailKey(host), ProbeKey(host), OpenUntilKey(host) });

    public async Task RecordFailureAsync(string host, bool isProbe)
    {
        if (isProbe)
        {
            await TripAsync(host);
            await _db.KeyDeleteAsync(ProbeKey(host));
            return;
        }
        var fails = await _db.StringIncrementAsync(FailKey(host));
        await _db.KeyExpireAsync(FailKey(host), TimeSpan.FromSeconds(FailWindowSec));
        if (fails >= FailThreshold) await TripAsync(host);
    }

    public async Task ForceHalfOpenAsync(string host)
    {
        await _db.StringSetAsync(StateKey(host), "half_open");
        await _db.KeyDeleteAsync(OpenUntilKey(host));
    }

    public async Task<long> OpenTtlMillisAsync(string host)
    {
        var v = await _db.StringGetAsync(OpenUntilKey(host));
        if (v.IsNullOrEmpty) return -1;
        return long.Parse(v!) - DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    public async Task<int> FailCountAsync(string host)
    {
        var v = await _db.StringGetAsync(FailKey(host));
        return v.IsNullOrEmpty ? 0 : (int)v;
    }
}
