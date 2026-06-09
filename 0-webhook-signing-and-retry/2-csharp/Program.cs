using WebhookSigning;

var builder = WebApplication.CreateBuilder(args);
builder.Configuration.AddEnvironmentVariables();

string role = Environment.GetEnvironmentVariable("ROLE") ?? "emitter";
int port = int.TryParse(Environment.GetEnvironmentVariable("PORT"), out int p) ? p : 3000;
string secret = builder.Configuration["WEBHOOK_SECRET"] ?? "shared-secret-demo";

bool isReceiver = role == "receiver";
if (!isReceiver)
{
    builder.Services.AddSingleton<EmitterService>();
}

builder.WebHost.UseUrls($"http://0.0.0.0:{port}");
var app = builder.Build();

if (isReceiver)
{
    // Receiver-mock: verify the HMAC over the exact wire bytes, then optionally force a status.
    app.MapPost("/api/hook", async (HttpContext ctx) =>
    {
        using var reader = new StreamReader(ctx.Request.Body);
        string rawBody = await reader.ReadToEndAsync();
        string header = ctx.Request.Headers["X-Webhook-Signature"].ToString();
        try
        {
            WebhookVerifier.Verify(secret, rawBody, header);
        }
        catch (WebhookException ex)
        {
            return Results.Json(
                new { message = ex.Message, error = "Unauthorized", statusCode = 401 },
                statusCode: 401);
        }
        // Chaos hook: force a specific status code AFTER a valid signature.
        string? fail = ctx.Request.Query["fail"];
        if (!string.IsNullOrEmpty(fail))
        {
            int code = int.TryParse(fail, out int c) ? c : 503;
            return Results.Json(new { message = "forced failure", statusCode = code }, statusCode: code);
        }
        return Results.Json(new { ok = true });
    });
}
else
{
    var emitter = app.Services.GetRequiredService<EmitterService>();
    app.MapPost("/api/events/publish", (PublishDto dto) => Results.Json(emitter.Publish(dto), statusCode: 201));
    app.MapGet("/api/events/{eventId}", (string eventId) =>
    {
        EventRecord? rec = emitter.Get(eventId);
        return rec is null
            ? Results.Json(new { message = $"event {eventId} not found", statusCode = 404 }, statusCode: 404)
            : Results.Json(rec);
    });
}

Console.WriteLine($"{role} listening on :{port}");
app.Run();
