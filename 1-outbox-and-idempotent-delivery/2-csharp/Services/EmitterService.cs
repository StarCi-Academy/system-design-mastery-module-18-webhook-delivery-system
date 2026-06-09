using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Outbox.Data;

namespace Outbox.Services;

/// <summary>Handles the synchronous publish API and the single-event read used by
/// the controller. The background delivery loop lives in PollerHostedService.</summary>
public class EmitterService
{
    private readonly AppDbContext _db;

    public EmitterService(AppDbContext db) => _db = db;

    /// <summary>Publish an event by writing the business change and the outbox row in
    /// ONE transaction. Either both are durable, or neither happened — there is no
    /// window where the order is committed but the webhook event is lost.</summary>
    public async Task<PublishResult> PublishAsync(PublishInput input)
    {
        await using var tx = await _db.Database.BeginTransactionAsync();

        // Business change (here: an orders row) and the outbox row share the same txn.
        string? orderRef = null;
        if (input.Payload.ValueKind == JsonValueKind.Object &&
            input.Payload.TryGetProperty("orderId", out var oid) &&
            oid.ValueKind == JsonValueKind.String)
        {
            orderRef = oid.GetString();
        }
        var order = new Order { Ref = orderRef };
        _db.Orders.Add(order);
        await _db.SaveChangesAsync();

        var ev = new OutboxEvent
        {
            Type = input.Type,
            TargetUrl = input.TargetUrl,
            Payload = input.Payload.GetRawText(),
            Status = "pending", // poller picks up pending rows on the next tick
            Attempts = 0,
        };
        _db.OutboxEvents.Add(ev);
        await _db.SaveChangesAsync();
        await tx.CommitAsync();
        return new PublishResult(ev.Id, ev.Status);
    }

    /// <summary>Read one event back for GET /api/events/:id, or null if unknown.</summary>
    public async Task<EventDto?> GetEventAsync(string id)
    {
        if (!Guid.TryParse(id, out var gid)) return null;
        var ev = await _db.OutboxEvents
            .FromSqlInterpolated($"SELECT * FROM outbox_event WHERE id = {gid}")
            .AsNoTracking()
            .FirstOrDefaultAsync();
        if (ev is null) return null;
        return new EventDto(
            ev.Id.ToString(),
            ev.Status,
            ev.Attempts,
            ev.LastError,
            ev.CreatedAt.ToUniversalTime().ToString("o"),
            ev.UpdatedAt.ToUniversalTime().ToString("o"));
    }
}
