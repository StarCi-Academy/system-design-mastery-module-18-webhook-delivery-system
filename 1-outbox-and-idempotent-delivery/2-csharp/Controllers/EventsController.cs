using System.Text;
using Microsoft.AspNetCore.Mvc;
using Outbox.Services;

namespace Outbox.Controllers;

/// <summary>The emitter HTTP surface: publish, read-back, and a hook-proxy that
/// forwards to receiver-mock (used by the idempotency-replay flow from the host).</summary>
[ApiController]
public class EventsController : ControllerBase
{
    private const int ProxyMaxAttempts = 12;

    private readonly EmitterService _emitter;
    private readonly IHttpClientFactory _httpFactory;
    private readonly string _receiverUrl;

    public EventsController(EmitterService emitter, IHttpClientFactory httpFactory, IConfiguration config)
    {
        _emitter = emitter;
        _httpFactory = httpFactory;
        _receiverUrl = config["ReceiverUrl"] ?? "http://receiver-mock:3000/api/hook";
    }

    [HttpPost("/api/events/publish")]
    public async Task<IActionResult> Publish([FromBody] PublishInput input)
    {
        var result = await _emitter.PublishAsync(input);
        return StatusCode(201, result);
    }

    [HttpGet("/api/events/{id}")]
    public async Task<IActionResult> Get(string id)
    {
        var ev = await _emitter.GetEventAsync(id);
        if (ev is null)
            return NotFound(new { statusCode = 404, message = $"Event {id} not found" });
        return Ok(ev);
    }

    /// <summary>Forward a hook call to receiver-mock with the client's Idempotency-Key,
    /// retrying on a non-2xx (the receiver fails the first N attempts) so the caller
    /// sees the first successful response and, on a replay, the same cached response.</summary>
    [HttpPost("/api/hook-proxy")]
    public async Task<IActionResult> HookProxy()
    {
        using var reader = new StreamReader(Request.Body);
        var body = await reader.ReadToEndAsync();
        var idemKey = Request.Headers["Idempotency-Key"].ToString();

        var status = 502;
        var content = "{}";
        var client = _httpFactory.CreateClient();
        for (var i = 0; i < ProxyMaxAttempts; i++)
        {
            try
            {
                using var req = new HttpRequestMessage(HttpMethod.Post, _receiverUrl)
                {
                    Content = new StringContent(body, Encoding.UTF8, "application/json"),
                };
                req.Headers.TryAddWithoutValidation("Idempotency-Key", idemKey);
                using var resp = await client.SendAsync(req);
                status = (int)resp.StatusCode;
                content = await resp.Content.ReadAsStringAsync();
                if (status < 500) break;
            }
            catch
            {
                // transient — fall through to backoff and retry
            }
            await Task.Delay(300);
        }
        return new ContentResult { StatusCode = status, Content = content, ContentType = "application/json" };
    }
}
