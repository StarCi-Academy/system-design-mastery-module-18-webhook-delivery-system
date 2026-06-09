using System.Text.Json;

namespace Outbox;

/// <summary>Shared camelCase JSON options so the receiver's cached response and
/// the API responses use the same field casing as the contract.</summary>
public static class Json
{
    public static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };
}
