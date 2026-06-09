using System.Text;
using Microsoft.EntityFrameworkCore;
using Outbox.Data;

namespace Outbox.Services;

/// <summary>The background poller: every 500ms it claims a batch of pending rows
/// with FOR UPDATE SKIP LOCKED, delivers each with retry/backoff, and every couple
/// of seconds a watchdog resets rows stuck in_flight so a crash never loses them.</summary>
public class PollerHostedService : BackgroundService
{
    private const int BatchSize = 10;
    private const int MaxAttempts = 6;
    private const int WorkerPool = 4;
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan InFlightTimeout = TimeSpan.FromSeconds(4);

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IHttpClientFactory _httpFactory;
    private readonly ILogger<PollerHostedService> _log;
    private readonly SemaphoreSlim _sem = new(WorkerPool);

    public PollerHostedService(IServiceScopeFactory scopeFactory, IHttpClientFactory httpFactory, ILogger<PollerHostedService> log)
    {
        _scopeFactory = scopeFactory;
        _httpFactory = httpFactory;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var tick = 0;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await PollOnceAsync(ct);
                if (tick % 4 == 0) await WatchdogAsync(ct); // every ~2s
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "poll loop error");
            }
            tick++;
            await Task.Delay(PollInterval, ct);
        }
    }

    /// <summary>Claim a batch of pending rows. EF Core has no first-class SKIP LOCKED,
    /// so we run raw SQL: FOR UPDATE SKIP LOCKED lets two pollers see disjoint batches
    /// instead of waiting on each other or double-claiming.</summary>
    private async Task<List<OutboxEvent>> ClaimPendingAsync(AppDbContext db, int batch)
    {
        await using var tx = await db.Database.BeginTransactionAsync();
        var rows = await db.OutboxEvents
            .FromSqlInterpolated($@"
                SELECT * FROM outbox_event
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT {batch} FOR UPDATE SKIP LOCKED")
            .AsNoTracking()
            .ToListAsync();
        if (rows.Count == 0) { await tx.CommitAsync(); return rows; }

        // Flip to in_flight inside the SAME txn so the next tick never re-claims them.
        var ids = rows.Select(r => r.Id).ToArray();
        await db.Database.ExecuteSqlInterpolatedAsync(
            $"UPDATE outbox_event SET status = 'in_flight', updated_at = now() WHERE id = ANY({ids})");
        await tx.CommitAsync();
        return rows;
    }

    private async Task PollOnceAsync(CancellationToken ct)
    {
        using var scope = _scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
        var rows = await ClaimPendingAsync(db, BatchSize);
        if (rows.Count == 0) return;
        await Task.WhenAll(rows.Select(r => DeliverOneAsync(r, ct)));
    }

    private async Task DeliverOneAsync(OutboxEvent ev, CancellationToken ct)
    {
        await _sem.WaitAsync(ct);
        try
        {
            var (ok, status, error) = await PostAsync(ev.TargetUrl, ev.Payload, ev.Id.ToString(), ct);
            using var scope = _scopeFactory.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
            if (ok)
            {
                await db.Database.ExecuteSqlInterpolatedAsync(
                    $"UPDATE outbox_event SET status = 'sent', last_error = NULL, updated_at = now() WHERE id = {ev.Id}");
            }
            else
            {
                var next = ev.Attempts + 1 >= MaxAttempts ? "failed" : "pending";
                await db.Database.ExecuteSqlInterpolatedAsync(
                    $"UPDATE outbox_event SET status = {next}, attempts = attempts + 1, last_error = {error}, updated_at = now() WHERE id = {ev.Id}");
            }
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "deliver error for {Id}", ev.Id);
        }
        finally
        {
            _sem.Release();
        }
    }

    private async Task<(bool ok, int status, string error)> PostAsync(string url, string payload, string idemKey, CancellationToken ct)
    {
        try
        {
            var client = _httpFactory.CreateClient();
            using var req = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = new StringContent(payload, Encoding.UTF8, "application/json"),
            };
            req.Headers.TryAddWithoutValidation("Idempotency-Key", idemKey);
            using var resp = await client.SendAsync(req, ct);
            var code = (int)resp.StatusCode;
            return (code is >= 200 and < 300, code, $"receiver HTTP {code}");
        }
        catch (Exception ex)
        {
            return (false, 0, ex.Message);
        }
    }

    /// <summary>Reset rows stuck in_flight (the emitter crashed mid-delivery) back to
    /// pending so the next poller tick re-claims them.</summary>
    private async Task WatchdogAsync(CancellationToken ct)
    {
        using var scope = _scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
        var secs = InFlightTimeout.TotalSeconds;
        await db.Database.ExecuteSqlInterpolatedAsync($@"
            UPDATE outbox_event SET status = 'pending', updated_at = now()
            WHERE status = 'in_flight' AND updated_at < now() - make_interval(secs => {secs})");
    }
}
