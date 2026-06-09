using Npgsql;

namespace WebhookDlqCircuit;

/// <summary>Thin Npgsql data layer for the outbox_event and dead_letter tables.</summary>
public class Db
{
    private readonly NpgsqlDataSource _src;

    public Db(string connString) => _src = NpgsqlDataSource.Create(connString);

    public async Task InitAsync()
    {
        const string ddl = @"
CREATE TABLE IF NOT EXISTS outbox_event (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  type TEXT NOT NULL,
  payload JSONB NOT NULL,
  target_url TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INT NOT NULL DEFAULT 0,
  last_error TEXT
);
CREATE TABLE IF NOT EXISTS dead_letter (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id UUID NOT NULL UNIQUE,
  type TEXT NOT NULL,
  target_url TEXT NOT NULL,
  payload JSONB NOT NULL,
  attempts INT NOT NULL,
  last_error TEXT NOT NULL,
  parked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  replayed_at TIMESTAMPTZ
);";
        await using var cmd = _src.CreateCommand(ddl);
        await cmd.ExecuteNonQueryAsync();
    }

    public NpgsqlDataSource Source => _src;
}
