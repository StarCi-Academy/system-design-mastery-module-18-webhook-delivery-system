using System.Text.Json;
using Npgsql;
using StackExchange.Redis;
using WebhookDlqCircuit;

var builder = WebApplication.CreateBuilder(args);

string Env(string k, string def) => Environment.GetEnvironmentVariable(k) is { Length: > 0 } v ? v : def;

var pgUrl = Env("POSTGRES_URL", "postgres://webhook:webhook@postgres:5432/webhook");
var pgConn = NpgsqlConnectionStringBuilderFromUrl(pgUrl);
var db = new Db(pgConn);
await db.InitAsync();

var redis = await ConnectionMultiplexer.ConnectAsync(Env("REDIS_URL", "redis://redis:6379").Replace("redis://", ""));

builder.Services.AddSingleton(db);
builder.Services.AddSingleton<IConnectionMultiplexer>(redis);
builder.Services.AddSingleton<CircuitService>();
builder.Services.AddHostedService<Worker>();

var app = builder.Build();
var circuit = app.Services.GetRequiredService<CircuitService>();

// POST /api/events/publish -> { eventId, status }
app.MapPost("/api/events/publish", async (PublishDto dto) =>
{
    Guid id;
    await using var cmd = db.Source.CreateCommand(
        "INSERT INTO outbox_event (type, payload, target_url, status) VALUES ($1,$2::jsonb,$3,'pending') RETURNING id");
    cmd.Parameters.AddWithValue(dto.Type);
    cmd.Parameters.AddWithValue(JsonSerializer.Serialize(dto.Payload));
    cmd.Parameters.AddWithValue(dto.TargetUrl);
    id = (Guid)(await cmd.ExecuteScalarAsync())!;
    return Results.Ok(new { eventId = id.ToString(), status = "pending" });
});

// GET /api/dlq -> array of parked rows
app.MapGet("/api/dlq", async () =>
{
    var rows = new List<object>();
    await using var cmd = db.Source.CreateCommand(
        @"SELECT id, event_id, type, target_url, payload, attempts, last_error, parked_at, replayed_at
          FROM dead_letter ORDER BY parked_at DESC");
    await using var r = await cmd.ExecuteReaderAsync();
    while (await r.ReadAsync())
    {
        rows.Add(new
        {
            id = r.GetGuid(0).ToString(),
            eventId = r.GetGuid(1).ToString(),
            type = r.GetString(2),
            targetUrl = r.GetString(3),
            payload = JsonSerializer.Deserialize<object>(r.GetString(4)),
            attempts = r.GetInt32(5),
            lastError = r.GetString(6),
            parkedAt = r.GetDateTime(7),
            replayedAt = r.IsDBNull(8) ? (DateTime?)null : r.GetDateTime(8)
        });
    }
    return Results.Ok(rows);
});

// POST /api/dlq/{id}/replay -> requeue
app.MapPost("/api/dlq/{id}/replay", async (Guid id) =>
{
    Guid? eventId = null;
    await using (var cmd = db.Source.CreateCommand("SELECT event_id FROM dead_letter WHERE id=$1"))
    {
        cmd.Parameters.AddWithValue(id);
        var res = await cmd.ExecuteScalarAsync();
        if (res is Guid g) eventId = g;
    }
    if (eventId is null)
        return Results.BadRequest(new { replayedDlq = id.ToString(), status = "not_found", note = "no such DLQ row" });

    await using (var c1 = db.Source.CreateCommand("UPDATE dead_letter SET replayed_at=now() WHERE id=$1"))
    {
        c1.Parameters.AddWithValue(id);
        await c1.ExecuteNonQueryAsync();
    }
    await using (var c2 = db.Source.CreateCommand(
        "UPDATE outbox_event SET status='pending', attempts=0, last_error=NULL WHERE id=$1"))
    {
        c2.Parameters.AddWithValue(eventId.Value);
        await c2.ExecuteNonQueryAsync();
    }
    return Results.Ok(new
    {
        replayedDlq = id.ToString(),
        eventId = eventId.Value.ToString(),
        status = "requeued",
        note = "Outbox row reset to pending; poller will re-enqueue within ~500ms."
    });
});

// GET /api/circuit/status?host=...
app.MapGet("/api/circuit/status", async (string host) =>
{
    var ttl = await circuit.OpenTtlMillisAsync(host);
    return Results.Ok(new
    {
        host,
        state = await circuit.GetStateAsync(host),
        failCount = await circuit.FailCountAsync(host),
        failThreshold = CircuitService.FailThreshold,
        openTtlSec = ttl > 0 ? (int)(ttl / 1000) : 0
    });
});

app.Run($"http://0.0.0.0:{Env("PORT", "3000")}");

// Convert a postgres://user:pass@host:port/db URL into an Npgsql connection string.
static string NpgsqlConnectionStringBuilderFromUrl(string url)
{
    var u = new Uri(url);
    var userInfo = u.UserInfo.Split(':');
    return new NpgsqlConnectionStringBuilder
    {
        Host = u.Host,
        Port = u.Port > 0 ? u.Port : 5432,
        Username = userInfo[0],
        Password = userInfo.Length > 1 ? userInfo[1] : "",
        Database = u.AbsolutePath.TrimStart('/')
    }.ConnectionString;
}

record PublishDto(string Type, Dictionary<string, object> Payload, string TargetUrl);
