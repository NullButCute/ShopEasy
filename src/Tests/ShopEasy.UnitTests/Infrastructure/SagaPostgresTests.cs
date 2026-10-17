using System.Collections.Concurrent;
using FluentAssertions;
using MassTransit;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using ShopEasy.OrderService.Data;
using ShopEasy.OrderService.ReadModels;
using ShopEasy.OrderService.Sagas;
using ShopEasy.OrderService.Services;
using ShopEasy.Shared.Contracts;
using Xunit;

namespace ShopEasy.UnitTests.Infrastructure;

// The saga row locking (PostgresSagaLocking: SELECT ... FOR UPDATE) against a real PostgreSQL.
// Several actors change a saga — event consumers, the timeout service, order cancellation —
// and the InMemory provider cannot run the locking query, so the unit tests use a stand-in.
// Here the REAL locking is raced, with every actor on its own connection, the way separate
// service instances would run. See InfrastructureFacts.cs.
public class SagaPostgresTests
{
    private const string Owner = "user-1";

    private static async Task<DbContextOptions<OrderDbContext>> CreateOrderDatabaseAsync(
        PostgresTestDatabase database)
    {
        var options = new DbContextOptionsBuilder<OrderDbContext>()
            .UseNpgsql(database.ConnectionString).Options;
        await using var db = new OrderDbContext(options);
        await db.Database.MigrateAsync();
        return options;
    }

    private static async Task<Guid> SeedAsync(
        DbContextOptions<OrderDbContext> options, SagaStatus status, DateTime? updatedAt = null)
    {
        var orderId = Guid.NewGuid();
        var now = DateTime.UtcNow;
        await using var db = new OrderDbContext(options);
        db.Orders.Add(new Order
        {
            Id = orderId, UserId = Owner, UserEmail = "u@test.com",
            Status = OrderStatus.Pending, TotalAmount = 100m, CreatedAt = now, UpdatedAt = now
        });
        db.OrderSummaries.Add(new OrderSummary
        {
            OrderId = orderId, UserId = Owner, UserEmail = "u@test.com",
            StatusName = "Pending", StatusCode = (int)OrderStatus.Pending, TotalAmount = 100m,
            AmountDisplay = "$100.00", ItemCount = 1, ItemsSummary = "Widget x1",
            PlacedAt = now, UpdatedAt = now
        });
        db.SagaStates.Add(new OrderSagaState
        {
            Id = Guid.NewGuid(), OrderId = orderId, Status = status,
            UserId = Owner, UserEmail = "u@test.com", TotalAmount = 100m, ItemsJson = "[]",
            CreatedAt = updatedAt ?? now, UpdatedAt = updatedAt ?? now
        });
        await db.SaveChangesAsync();
        return orderId;
    }

    // Starts every action at the same moment, each on its own thread
    private static async Task RunTogetherAsync(params Func<Task>[] actions)
    {
        var go = new TaskCompletionSource();
        var running = actions.Select(action => Task.Run(async () =>
        {
            await go.Task;
            await action();
        })).ToArray();
        await Task.Delay(50);           // let every task reach the starting line
        go.SetResult();
        await Task.WhenAll(running);
    }

    // What an actor published, collected across all actors of one race
    private sealed class Outbox
    {
        public ConcurrentBag<object> Messages { get; } = [];

        public IPublishEndpoint NewEndpoint()
        {
            var endpoint = Substitute.For<IPublishEndpoint>();
            endpoint.Publish(Arg.Any<ProcessPaymentCommand>(), Arg.Any<CancellationToken>())
                .Returns(call => Record(call.Arg<ProcessPaymentCommand>()));
            endpoint.Publish(Arg.Any<CommitInventoryCommand>(), Arg.Any<CancellationToken>())
                .Returns(call => Record(call.Arg<CommitInventoryCommand>()));
            endpoint.Publish(Arg.Any<ReleaseInventoryCommand>(), Arg.Any<CancellationToken>())
                .Returns(call => Record(call.Arg<ReleaseInventoryCommand>()));
            endpoint.Publish(Arg.Any<RefundPaymentCommand>(), Arg.Any<CancellationToken>())
                .Returns(call => Record(call.Arg<RefundPaymentCommand>()));
            return endpoint;
        }

        private Task Record(object message)
        {
            Messages.Add(message);
            return Task.CompletedTask;
        }

        public int Count<T>() => Messages.OfType<T>().Count();
    }

    // ── The lock itself ──────────────────────────────────────────────────────

    // Read the saga, think for a moment, write it back. This is every handler's shape.
    private static async Task IncrementRetryCountAsync(
        DbContextOptions<OrderDbContext> options, Guid orderId, ISagaLocking locking)
    {
        await using var db = new OrderDbContext(options);
        await using var transaction = await db.Database.BeginTransactionAsync();

        var saga = await locking.LockByOrderIdAsync(db, orderId);
        var seen = saga!.RetryCount;
        await Task.Delay(30);                       // widen the window between read and write
        saga.RetryCount = seen + 1;
        await db.SaveChangesAsync();

        await transaction.CommitAsync();
    }

    // Reads without a lock — what the code would do if the locking were missing
    private sealed class NoLocking : ISagaLocking
    {
        public Task<OrderSagaState?> LockByOrderIdAsync(
            OrderDbContext db, Guid orderId, CancellationToken ct = default)
            => db.SagaStates.FirstOrDefaultAsync(s => s.OrderId == orderId, ct);
    }

    [RequiresPostgresFact]
    public async Task ConcurrentTransitions_AreSerializedByTheRowLock_NoUpdateIsLost()
    {
        await using var database = await PostgresTestDatabase.CreateAsync("sagalock");
        var options = await CreateOrderDatabaseAsync(database);
        var orderId = await SeedAsync(options, SagaStatus.ProcessingPayment);
        var locking = new PostgresSagaLocking();

        await RunTogetherAsync(Enumerable.Range(0, 12)
            .Select(_ => (Func<Task>)(() => IncrementRetryCountAsync(options, orderId, locking)))
            .ToArray());

        await using var check = new OrderDbContext(options);
        (await check.SagaStates.AsNoTracking().SingleAsync(s => s.OrderId == orderId))
            .RetryCount.Should().Be(12);
    }

    // Proves the test above can fail: without the lock the same code loses updates.
    // (Twelve actors all read the same value and write the same result back.)
    [RequiresPostgresFact]
    public async Task Control_WithoutTheRowLock_ConcurrentUpdatesAreLost()
    {
        await using var database = await PostgresTestDatabase.CreateAsync("sagacontrol");
        var options = await CreateOrderDatabaseAsync(database);
        var orderId = await SeedAsync(options, SagaStatus.ProcessingPayment);
        var noLocking = new NoLocking();

        await RunTogetherAsync(Enumerable.Range(0, 12)
            .Select(_ => (Func<Task>)(() => IncrementRetryCountAsync(options, orderId, noLocking)))
            .ToArray());

        await using var check = new OrderDbContext(options);
        (await check.SagaStates.AsNoTracking().SingleAsync(s => s.OrderId == orderId))
            .RetryCount.Should().BeLessThan(12, "without the lock, concurrent read-modify-write loses updates");
    }

    [RequiresPostgresFact]
    public async Task LockByOrderId_ReturnsNull_WhenThereIsNoSaga()
    {
        await using var database = await PostgresTestDatabase.CreateAsync("sagalock_none");
        var options = await CreateOrderDatabaseAsync(database);
        await using var db = new OrderDbContext(options);
        await using var transaction = await db.Database.BeginTransactionAsync();

        (await new PostgresSagaLocking().LockByOrderIdAsync(db, Guid.NewGuid())).Should().BeNull();
    }

    [RequiresPostgresFact]
    public async Task LockByOrderId_RefreshesASagaThatWasAlreadyLoadedByTheSameContext()
    {
        // The orchestrator loads the saga first and locks it afterwards. By then another actor
        // may have changed it: the caller must see the new values, not its stale copy.
        await using var database = await PostgresTestDatabase.CreateAsync("sagalock_reload");
        var options = await CreateOrderDatabaseAsync(database);
        var orderId = await SeedAsync(options, SagaStatus.ReservingInventory);

        await using var db = new OrderDbContext(options);
        var stale = await db.SagaStates.SingleAsync(s => s.OrderId == orderId);   // tracked, old values

        await using (var other = new OrderDbContext(options))
        {
            var saga = await other.SagaStates.SingleAsync(s => s.OrderId == orderId);
            saga.Status = SagaStatus.Cancelled;
            await other.SaveChangesAsync();
        }

        await using var transaction = await db.Database.BeginTransactionAsync();
        var locked = await new PostgresSagaLocking().LockByOrderIdAsync(db, orderId);

        locked.Should().BeSameAs(stale);
        locked!.Status.Should().Be(SagaStatus.Cancelled);
    }

    // ── Real actors racing for one saga ──────────────────────────────────────

    private static PaymentSucceededConsumer PaymentSucceededOn(
        DbContextOptions<OrderDbContext> options, IPublishEndpoint publish, out OrderDbContext db)
    {
        db = new OrderDbContext(options);
        return new PaymentSucceededConsumer(
            db, new PostgresSagaLocking(), publish,
            new OrderReadModelUpdater(db, NullLogger<OrderReadModelUpdater>.Instance),
            NullLogger<PaymentSucceededConsumer>.Instance);
    }

    private static ConsumeContext<PaymentSucceededEvent> PaymentSucceededEvent(Guid orderId)
    {
        var context = Substitute.For<ConsumeContext<PaymentSucceededEvent>>();
        context.Message.Returns(new PaymentSucceededEvent
            { OrderId = orderId, PaymentId = Guid.NewGuid(), Amount = 100m });
        context.CancellationToken.Returns(CancellationToken.None);
        return context;
    }

    [RequiresPostgresFact]
    public async Task APaymentResult_RacingTheTimeout_NeverBothCommitsAndReleasesTheStock()
    {
        await using var database = await PostgresTestDatabase.CreateAsync("sagarace_timeout");
        var options = await CreateOrderDatabaseAsync(database);

        for (var round = 0; round < 25; round++)
        {
            // a saga whose payment has been pending for too long: the timeout service may act
            var orderId = await SeedAsync(
                options, SagaStatus.ProcessingPayment, updatedAt: DateTime.UtcNow.AddMinutes(-5));
            var outbox = new Outbox();

            await RunTogetherAsync(
                async () =>
                {
                    var consumer = PaymentSucceededOn(options, outbox.NewEndpoint(), out var db);
                    await using (db) await consumer.Consume(PaymentSucceededEvent(orderId));
                },
                async () =>
                {
                    await using var db = new OrderDbContext(options);
                    var stale = await db.SagaStates.AsNoTracking().SingleAsync(s => s.OrderId == orderId);
                    var timeout = new SagaTimeoutService(
                        Substitute.For<IServiceScopeFactory>(), NullLogger<SagaTimeoutService>.Instance);
                    await timeout.HandleTimedOutSaga(
                        stale, db, outbox.NewEndpoint(), new PostgresSagaLocking(), CancellationToken.None);
                });

            await using var check = new OrderDbContext(options);
            var saga = await check.SagaStates.AsNoTracking().SingleAsync(s => s.OrderId == orderId);

            if (outbox.Count<CommitInventoryCommand>() > 0)
            {
                // the payment result won: the order goes on, and the timeout must have stood down
                saga.Status.Should().Be(SagaStatus.Completing, $"round {round}");
                outbox.Count<ReleaseInventoryCommand>().Should().Be(0, $"round {round}");
                outbox.Count<RefundPaymentCommand>().Should().Be(0, $"round {round}");
            }
            else
            {
                // the timeout won: stock is released, and the payment that arrives after is refunded
                saga.Status.Should().Be(SagaStatus.ReleasingInventory, $"round {round}");
                outbox.Count<ReleaseInventoryCommand>().Should().Be(1, $"round {round}");
                outbox.Count<RefundPaymentCommand>().Should().Be(1, $"round {round}");
            }
        }
    }

    [RequiresPostgresFact]
    public async Task ACustomerCancel_RacingThePaymentResult_NeverLeavesAChargedOrderWithoutRefundOrCommit()
    {
        await using var database = await PostgresTestDatabase.CreateAsync("sagarace_cancel");
        var options = await CreateOrderDatabaseAsync(database);

        for (var round = 0; round < 25; round++)
        {
            var orderId = await SeedAsync(options, SagaStatus.ProcessingPayment);
            var outbox = new Outbox();
            CancelResult? cancelResult = null;

            await RunTogetherAsync(
                async () =>
                {
                    var consumer = PaymentSucceededOn(options, outbox.NewEndpoint(), out var db);
                    await using (db) await consumer.Consume(PaymentSucceededEvent(orderId));
                },
                async () =>
                {
                    await using var db = new OrderDbContext(options);
                    var service = new OrderCancellationService(
                        db, new PostgresSagaLocking(), outbox.NewEndpoint(),
                        new OrderReadModelUpdater(db, NullLogger<OrderReadModelUpdater>.Instance),
                        NullLogger<OrderCancellationService>.Instance);
                    cancelResult = await service.CancelAsync(orderId, Owner, isAdmin: false);
                });

            await using var check = new OrderDbContext(options);
            var saga = await check.SagaStates.AsNoTracking().SingleAsync(s => s.OrderId == orderId);

            if (outbox.Count<CommitInventoryCommand>() > 0)
            {
                // payment first: the cancel is kept until the stock commit lands, then refunded
                saga.Status.Should().Be(SagaStatus.Completing, $"round {round}");
                saga.FailureReason.Should().Be("Cancelled by customer", $"round {round}");
                cancelResult!.Outcome.Should().Be(CancelOutcome.Cancelling, $"round {round}");
                outbox.Count<ReleaseInventoryCommand>().Should().Be(0, $"round {round}");
                outbox.Count<RefundPaymentCommand>().Should().Be(0, $"round {round}");
            }
            else
            {
                // cancel first: stock is released, and the payment that still arrived is refunded
                saga.Status.Should().Be(SagaStatus.ReleasingInventory, $"round {round}");
                cancelResult!.Outcome.Should().Be(CancelOutcome.Cancelling, $"round {round}");
                outbox.Count<ReleaseInventoryCommand>().Should().Be(1, $"round {round}");
                outbox.Count<RefundPaymentCommand>().Should().Be(1, $"round {round}");
            }
        }
    }
}
