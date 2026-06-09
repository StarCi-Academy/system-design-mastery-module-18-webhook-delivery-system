using Microsoft.EntityFrameworkCore;

namespace Outbox.Data;

/// <summary>EF Core context mapping the outbox_event and orders tables. Most reads
/// use raw SQL (FromSqlInterpolated) so the SKIP LOCKED clause is explicit.</summary>
public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    public DbSet<OutboxEvent> OutboxEvents => Set<OutboxEvent>();
    public DbSet<Order> Orders => Set<Order>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<OutboxEvent>(e =>
        {
            e.ToTable("outbox_event");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id").HasDefaultValueSql("gen_random_uuid()").ValueGeneratedOnAdd();
            e.Property(x => x.Type).HasColumnName("type");
            e.Property(x => x.TargetUrl).HasColumnName("target_url");
            e.Property(x => x.Payload).HasColumnName("payload").HasColumnType("jsonb");
            e.Property(x => x.Status).HasColumnName("status");
            e.Property(x => x.Attempts).HasColumnName("attempts");
            e.Property(x => x.LastError).HasColumnName("last_error");
            e.Property(x => x.CreatedAt).HasColumnName("created_at").HasDefaultValueSql("now()").ValueGeneratedOnAdd();
            e.Property(x => x.UpdatedAt).HasColumnName("updated_at").HasDefaultValueSql("now()").ValueGeneratedOnAdd();
        });

        b.Entity<Order>(e =>
        {
            e.ToTable("orders");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id").HasDefaultValueSql("gen_random_uuid()").ValueGeneratedOnAdd();
            e.Property(x => x.Ref).HasColumnName("ref");
        });
    }

    /// <summary>Create the schema if it does not exist (idempotent, no EF migrations).</summary>
    public async Task MigrateSchemaAsync()
    {
        await Database.ExecuteSqlRawAsync("CREATE EXTENSION IF NOT EXISTS pgcrypto");
        await Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS orders (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                ref VARCHAR(128)
            )");
        await Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS outbox_event (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                type VARCHAR(128) NOT NULL,
                target_url VARCHAR(512) NOT NULL,
                payload JSONB NOT NULL,
                status VARCHAR(16) NOT NULL DEFAULT 'pending',
                attempts INT NOT NULL DEFAULT 0,
                last_error TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )");
        await Database.ExecuteSqlRawAsync(
            "CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox_event(status, created_at)");
    }
}
