using System.Text;
using Npgsql;

namespace WebhookDlqCircuit;

/// <summary>
/// Outbox poller + delivery worker + circuit sweeper. A circuit-blocked job is
/// rescheduled without burning an attempt — the .NET analog of the BullMQ
/// DelayedError soft-skip (and of catching Polly's BrokenCircuitException).
/// </summary>
public class Worker : BackgroundService
{
    private const int MaxAttempts = 3;
    private readonly Db _db;
    private readonly CircuitService _circuit;
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(3) };

    public Worker(Db db, CircuitService circuit)
    {
        _db = db;
        _circuit = circuit;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var lastSweep = DateTime.UtcNow;
        while (!ct.IsCancellationRequested)
        {
            await PollAsync();
            if ((DateTime.UtcNow - lastSweep).TotalSeconds >= 2)
            {
                await SweepAsync();
                lastSweep = DateTime.UtcNow;
            }
            await Task.Delay(500, ct);
        }
    }

    private async Task PollAsync()
    {
        var src = _db.Source;
        var rows = new List<(Guid id, string type, string url, string payload, int attempts)>();
        await using (var cmd = src.CreateCommand(
            "SELECT id, type, target_url, payload, attempts FROM outbox_event WHERE status='pending' LIMIT 50"))
        await using (var r = await cmd.ExecuteReaderAsync())
        {
            while (await r.ReadAsync())
                rows.Add((r.GetGuid(0), r.GetString(1), r.GetString(2), r.GetString(3), r.GetInt32(4)));
        }
        foreach (var row in rows) await DeliverAsync(row.id, row.type, row.url, row.payload, row.attempts);
    }

    private async Task DeliverAsync(Guid id, string type, string url, string payload, int attempts)
    {
        var host = new Uri(url).Authority;
        var gate = await _circuit.AllowRequestAsync(host);
        if (!gate.Allow) return; // circuit open: skip without burning an attempt

        try
        {
            await SendAsync(url, payload);
            await _circuit.RecordSuccessAsync(host);
            await Exec("UPDATE outbox_event SET status='sent' WHERE id=$1", id);
        }
        catch (Exception e)
        {
            await _circuit.RecordFailureAsync(host, gate.IsProbe);
            var next = attempts + 1;
            await Exec("UPDATE outbox_event SET attempts=$2, last_error=$3 WHERE id=$1", id, next, e.Message);
            if (next >= MaxAttempts)
            {
                await Exec("UPDATE outbox_event SET status='failed' WHERE id=$1", id);
                await ParkAsync(id, type, url, payload, next, e.Message);
            }
        }
    }

    private async Task SendAsync(string url, string payload)
    {
        using var content = new StringContent(payload, Encoding.UTF8, "application/json");
        var resp = await _http.PostAsync(url, content);
        if (!resp.IsSuccessStatusCode) throw new Exception($"receiver responded {(int)resp.StatusCode}");
    }

    private async Task ParkAsync(Guid eventId, string type, string url, string payload, int attempts, string err)
    {
        await Exec(
            @"INSERT INTO dead_letter (event_id, type, target_url, payload, attempts, last_error)
              VALUES ($1,$2,$3,$4::jsonb,$5,$6) ON CONFLICT (event_id) DO NOTHING",
            eventId, type, url, payload, attempts, err);
    }

    private async Task SweepAsync()
    {
        var hosts = new HashSet<string>();
        await using (var cmd = _db.Source.CreateCommand("SELECT DISTINCT target_url FROM outbox_event LIMIT 200"))
        await using (var r = await cmd.ExecuteReaderAsync())
        {
            while (await r.ReadAsync()) hosts.Add(new Uri(r.GetString(0)).Authority);
        }
        foreach (var host in hosts)
        {
            if (await _circuit.GetStateAsync(host) == "open" && await _circuit.OpenTtlMillisAsync(host) < 0)
                await _circuit.ForceHalfOpenAsync(host);
        }
    }

    private async Task Exec(string sql, params object[] args)
    {
        await using var cmd = _db.Source.CreateCommand(sql);
        foreach (var a in args) cmd.Parameters.AddWithValue(a);
        await cmd.ExecuteNonQueryAsync();
    }
}
