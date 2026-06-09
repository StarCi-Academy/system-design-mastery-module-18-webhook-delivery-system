using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Outbox.Data;
using Outbox.Services;
using StackExchange.Redis;

// One binary, two roles. Default = emitter (publish API + poller). The receiver-mock
// runs the same image with `dotnet Outbox.dll receiver` (see compose command override).
var role = args.Length > 0 ? args[0] : Environment.GetEnvironmentVariable("ROLE") ?? "emitter";
var port = Environment.GetEnvironmentVariable("PORT") ?? "3000";

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://0.0.0.0:{port}");
builder.Services
    .AddControllers()
    .AddJsonOptions(o => o.JsonSerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase);

if (role == "receiver")
{
    var redisConn = builder.Configuration.GetConnectionString("Redis") ?? "redis:6379";
    builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConn));
}
else
{
    var pg = builder.Configuration.GetConnectionString("Postgres")
             ?? "Host=postgres;Port=5432;Username=app;Password=app;Database=app";
    builder.Services.AddDbContext<AppDbContext>(o => o.UseNpgsql(pg));
    builder.Services.AddScoped<EmitterService>();
    builder.Services.AddHttpClient();
    builder.Services.AddHostedService<PollerHostedService>();
}

var app = builder.Build();
app.MapControllers();

if (role != "receiver")
    await MigrateWithRetryAsync(app);

Console.WriteLine($"{role} listening on {port}");
app.Run();

static async Task MigrateWithRetryAsync(WebApplication app)
{
    for (var i = 0; i < 30; i++)
    {
        try
        {
            using var scope = app.Services.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
            await db.MigrateSchemaAsync();
            return;
        }
        catch
        {
            await Task.Delay(1000);
        }
    }
    throw new Exception("postgres not reachable after 30 attempts");
}
