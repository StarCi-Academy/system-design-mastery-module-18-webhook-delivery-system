using System.Text.Json;
using System.Text.Json.Nodes;

namespace Outbox;

/// <summary>An outbox row: the durable source of truth for one event to deliver.</summary>
public class OutboxEvent
{
    public Guid Id { get; set; }
    public string Type { get; set; } = "";
    public string TargetUrl { get; set; } = "";
    public string Payload { get; set; } = "{}";
    public string Status { get; set; } = "pending";
    public int Attempts { get; set; }
    public string? LastError { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }
}

/// <summary>A business stand-in row written in the same transaction as the outbox row.</summary>
public class Order
{
    public Guid Id { get; set; }
    public string? Ref { get; set; }
}

/// <summary>POST /api/events/publish request body.</summary>
public record PublishInput(string Type, string TargetUrl, JsonElement Payload);

/// <summary>201 response body.</summary>
public record PublishResult(Guid EventId, string Status);

/// <summary>GET /api/events/:id 200 response body.</summary>
public record EventDto(string Id, string Status, int Attempts, string? LastError, string CreatedAt, string UpdatedAt);

/// <summary>receiver-mock successful hook response body.</summary>
public record HookResult(bool Received, string EventId, string IdempotencyKey, string SideEffectExecutedAt, JsonNode? BodyEcho);
