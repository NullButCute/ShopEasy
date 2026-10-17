using Microsoft.EntityFrameworkCore;
using ShopEasy.OrderService.Idempotency;
using ShopEasy.OrderService.ReadModels;
using ShopEasy.OrderService.Services;

namespace ShopEasy.OrderService.Data;

public enum SagaStatus
{
    Started,
    ReservingInventory,
    ProcessingPayment,
    Completing,
    Completed,
    ReleasingInventory,
    Cancelled,
    Failed,
    // Cancelling an order that was already paid: the refund is out, then the stock is put back
    RefundingPayment,
    RestockingInventory
}

public class OrderSagaState
{
    public Guid Id { get; set; }
    public Guid OrderId { get; set; }
    public SagaStatus Status { get; set; }
    public string UserId { get; set; } = default!;
    public string UserEmail { get; set; } = default!;
    public decimal TotalAmount { get; set; }
    public string ItemsJson { get; set; } = default!; // serialized items
    public string? FailureReason { get; set; }
    public int RetryCount { get; set; }

    // Correlation id of the request that placed the order, taken from the message that started
    // the saga. The orchestrator and the timeout service run without a request and use it to
    // keep the commands they publish in the same trace.
    public string? CorrelationId { get; set; }

    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }
}


public enum OrderStatus
{
    Pending,
    Confirmed,
    PaymentFailed,
    Shipped,
    Delivered,
    Cancelled
}

public class OutboxEntry
{
    public Guid Id { get; set; }
    public string EventType { get; set; } = default!;
    public string Payload { get; set; } = default!;
    public DateTime CreatedAt { get; set; }
    public DateTime? DeliveredAt { get; set; }
    public int RetryCount { get; set; }

    // Correlation id of the HTTP request that placed the order. The event is published later by
    // a background worker that has no request, so the id is stored with the entry and put back
    // on the message (see CorrelationContext).
    public string? CorrelationId { get; set; }
}

// What is remembered about a POST /api/orders that carried an Idempotency-Key header, so that a
// retry of the same request returns the original response instead of placing a second order.
// Written in the same transaction as the order, so the two exist together or not at all.
public class IdempotencyRecord
{
    public const string KeyIndexName = "IX_IdempotencyRecords_UserId_Key";

    public Guid Id { get; set; }

    // Keys are scoped to the caller: two users may use the same key without seeing each other
    public string UserId { get; set; } = default!;
    public string Key { get; set; } = default!;

    // SHA-256 of the meaningful parts of the request (see OrderRequestFingerprint). The same key
    // with a different request is an error, not a replay.
    public string RequestHash { get; set; } = default!;

    public Guid OrderId { get; set; }

    // The response that was sent the first time, replayed as it was
    public int StatusCode { get; set; }
    public string ResponseBody { get; set; } = default!;

    public DateTime CreatedAt { get; set; }
}

public class Order
{
    public Guid Id { get; set; }
    public string UserId { get; set; } = default!;
    public string UserEmail { get; set; } = default!;
    public OrderStatus Status { get; set; } = OrderStatus.Pending;
    public decimal TotalAmount { get; set; }
    public string? Notes { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }
    public List<OrderItem> Items { get; set; } = new();
}

public class OrderItem
{
    public Guid Id { get; set; }
    public Guid OrderId { get; set; }
    public Guid ProductId { get; set; }
    public string ProductName { get; set; } = default!;
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
    public decimal LineTotal => Quantity * UnitPrice;
}

public class OrderDbContext : DbContext
{
    public OrderDbContext(DbContextOptions<OrderDbContext> options) : base(options) { }

    public DbSet<Order> Orders => Set<Order>();
    public DbSet<OrderItem> OrderItems => Set<OrderItem>();
    public DbSet<OutboxEntry> OutboxEntries => Set<OutboxEntry>();
    public DbSet<OrderSagaState> SagaStates => Set<OrderSagaState>();
    public DbSet<OrderSummary> OrderSummaries => Set<OrderSummary>();
    public DbSet<IdempotencyRecord> IdempotencyRecords => Set<IdempotencyRecord>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OrderSummary>(e =>
        {
            e.HasKey(o => o.OrderId);
            e.HasIndex(o => o.UserId);       // fast lookup by user
            e.HasIndex(o => o.StatusCode);   // fast filter by status
            e.HasIndex(o => o.PlacedAt);     // fast sort by date
            e.Property(o => o.ItemsSummary).HasMaxLength(500);
        });

        modelBuilder.Entity<Order>(e =>
        {
            e.HasKey(o => o.Id);
            e.Property(o => o.TotalAmount).HasColumnType("decimal(18,2)");
            e.HasMany(o => o.Items).WithOne().HasForeignKey(i => i.OrderId);
        });

        modelBuilder.Entity<OrderItem>(e =>
        {
            e.HasKey(i => i.Id);
            e.Property(i => i.UnitPrice).HasColumnType("decimal(18,2)");
        });

        modelBuilder.Entity<OutboxEntry>(e =>
        {
            e.HasKey(o => o.Id);
            e.HasIndex(o => o.DeliveredAt); // fast query for pending
        });

        modelBuilder.Entity<IdempotencyRecord>(e =>
        {
            e.HasKey(r => r.Id);
            e.Property(r => r.UserId).HasMaxLength(128);
            e.Property(r => r.Key).HasMaxLength(IdempotencyKey.MaxLength);
            e.Property(r => r.RequestHash).HasMaxLength(64);
            // The unique index is what makes two concurrent requests with the same key safe:
            // only one of them can commit its order.
            e.HasIndex(r => new { r.UserId, r.Key }).IsUnique().HasDatabaseName(IdempotencyRecord.KeyIndexName);
            e.HasIndex(r => r.CreatedAt); // retention clean-up
        });

        modelBuilder.Entity<OrderSagaState>(e =>
        {
            e.HasKey(s => s.Id);
            e.HasIndex(s => s.OrderId).IsUnique();
            e.HasIndex(s => s.Status);
            e.Property(s => s.TotalAmount).HasColumnType("decimal(18,2)");
        });
    }
}