using System.Collections.Concurrent;
using FluentAssertions;
using MassTransit;
using MassTransit.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Npgsql;
using ShopEasy.InventoryService.Consumers;
using ShopEasy.InventoryService.Data;
using ShopEasy.Shared.Contracts;
using ShopEasy.Shared.Resilience;
using ShopEasy.Shared.Services;
using ShopEasy.UnitTests.Support;
using Xunit;

namespace ShopEasy.UnitTests.Infrastructure;

// Inventory consumers against a real PostgreSQL: the row locks (SELECT ... FOR UPDATE) and the
// MassTransit transactional outbox can only be proven there. See InfrastructureFacts.cs.
public class InventoryPostgresTests
{
    private static readonly Guid Product = Guid.Parse("00000000-0000-0000-0000-000000000003");

    private static CacheService NewCache() =>
        new(Substitute.For<IDistributedCache>(), NullLogger<CacheService>.Instance);

    private static async Task<DbContextOptions<InventoryDbContext>> CreateInventoryAsync(
        PostgresTestDatabase database, int quantity)
    {
        var options = new DbContextOptionsBuilder<InventoryDbContext>()
            .UseNpgsql(database.ConnectionString).Options;

        await using var db = new InventoryDbContext(options);
        await db.Database.MigrateAsync();
        db.InventoryItems.Add(new InventoryItem
        {
            ProductId = Product,
            ProductName = "Standing Desk",
            Quantity = quantity,
            UpdatedAt = DateTime.UtcNow
        });
        await db.SaveChangesAsync();
        return options;
    }

    private static ReserveInventoryCommand Reserve(Guid orderId, int quantity) => new()
    {
        OrderId = orderId,
        Items =
        [
            new OrderLineItem
            {
                ProductId = Product, ProductName = "Standing Desk",
                Quantity = quantity, UnitPrice = 1m
            }
        ]
    };

    // Runs one consumer on a context of its own, as the bus does for each message
    private static async Task Consume<TMessage>(
        DbContextOptions<InventoryDbContext> options, TMessage message,
        Func<InventoryDbContext, IConsumer<TMessage>> create) where TMessage : class
    {
        await using var db = new InventoryDbContext(options);
        var context = Substitute.For<ConsumeContext<TMessage>>();
        context.Message.Returns(message);
        context.CancellationToken.Returns(CancellationToken.None);
        await create(db).Consume(context);
    }

    // ── Row locking ───────────────────────────────────────────────────────────

    [RequiresPostgresFact]
    public async Task ConcurrentReservations_NeverReserveMoreThanTheStock()
    {
        await using var database = await PostgresTestDatabase.CreateAsync("lock");
        var options = await CreateInventoryAsync(database, quantity: 10);

        // 12 orders of 3 units compete for 10 units: exactly 3 can win (9 units)
        var orders = Enumerable.Range(0, 12).Select(_ => Guid.NewGuid()).ToList();
        var outcomes = new ConcurrentDictionary<Guid, bool>();

        await Task.WhenAll(orders.Select(orderId => Task.Run(async () =>
        {
            await using var db = new InventoryDbContext(options);
            var consumer = new ReserveInventoryConsumer(
                db, new PostgresInventoryLocking(), NewCache(),
                NullLogger<ReserveInventoryConsumer>.Instance);

            var context = Substitute.For<ConsumeContext<ReserveInventoryCommand>>();
            context.Message.Returns(Reserve(orderId, 3));
            context.CancellationToken.Returns(CancellationToken.None);
            await consumer.Consume(context);

            outcomes[orderId] = context.ReceivedCalls().Any(call =>
                call.GetMethodInfo().Name == nameof(IPublishEndpoint.Publish)
                && call.GetArguments().Any(a => a is InventoryReservedEvent));
        })));

        outcomes.Values.Count(reserved => reserved).Should().Be(3);

        await using var check = new InventoryDbContext(options);
        var item = await check.InventoryItems.SingleAsync();
        item.ReservedQty.Should().Be(9);
        item.AvailableQuantity.Should().Be(1);
        (await check.StockTransactions.CountAsync()).Should().Be(3);
    }

    [RequiresPostgresFact]
    public async Task AReservedCommittedAndRestockedOrder_EndsWithTheUnitsBack_AndRestocksOnce()
    {
        await using var database = await PostgresTestDatabase.CreateAsync("restock");
        var options = await CreateInventoryAsync(database, quantity: 10);
        var orderId = Guid.NewGuid();
        var locking = new PostgresInventoryLocking();
        var restock = new RestockInventoryCommand { OrderId = orderId };

        await Consume(options, Reserve(orderId, 3), db =>
            new ReserveInventoryConsumer(db, locking, NewCache(), NullLogger<ReserveInventoryConsumer>.Instance));
        await Consume(options, new CommitInventoryCommand { OrderId = orderId }, db =>
            new CommitInventoryConsumer(db, locking, NewCache(), NullLogger<CommitInventoryConsumer>.Instance));
        await using (var mid = new InventoryDbContext(options))
            (await mid.InventoryItems.SingleAsync()).Quantity.Should().Be(7);

        for (var i = 0; i < 2; i++) // the second command is a repeat
            await Consume(options, restock, db =>
                new RestockInventoryConsumer(db, locking, NewCache(), NullLogger<RestockInventoryConsumer>.Instance));

        await using var check = new InventoryDbContext(options);
        var item = await check.InventoryItems.SingleAsync();
        item.Quantity.Should().Be(10);
        item.ReservedQty.Should().Be(0);
        (await check.StockTransactions.CountAsync(t => t.Reason == StockReasons.Restocked(orderId))).Should().Be(1);
    }

    [RequiresPostgresFact]
    public async Task TheSameReserveCommandDeliveredTwiceAtOnce_ReservesOnce()
    {
        await using var database = await PostgresTestDatabase.CreateAsync("dup");
        var options = await CreateInventoryAsync(database, quantity: 10);
        var orderId = Guid.NewGuid();

        await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            await using var db = new InventoryDbContext(options);
            var consumer = new ReserveInventoryConsumer(
                db, new PostgresInventoryLocking(), NewCache(),
                NullLogger<ReserveInventoryConsumer>.Instance);
            var context = Substitute.For<ConsumeContext<ReserveInventoryCommand>>();
            context.Message.Returns(Reserve(orderId, 3));
            context.CancellationToken.Returns(CancellationToken.None);
            await consumer.Consume(context);
        })));

        await using var check = new InventoryDbContext(options);
        (await check.InventoryItems.SingleAsync()).ReservedQty.Should().Be(3);
        (await check.StockTransactions.CountAsync(t => t.OrderId == orderId)).Should().Be(1);
    }

    // ── Transactional outbox, wired the way Program.cs wires it ───────────────

    private static ServiceProvider BuildOutboxHost(
        PostgresTestDatabase database, Action<IBusRegistrationConfigurator> addConsumers,
        Action<IBusRegistrationContext, IInMemoryBusFactoryConfigurator> endpoints,
        string? connectionString = null, FaultInjector? injector = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IInventoryLocking, PostgresInventoryLocking>();
        services.AddSingleton(NewCache());
        services.AddDbContext<InventoryDbContext>(o =>
        {
            o.UseNpgsql(connectionString ?? database.ConnectionString);
            if (injector is not null) o.AddInterceptors(injector);
        });
        services.AddMassTransitTestHarness(x =>
        {
            x.AllowSlowRunners();
            addConsumers(x);
            x.AddEntityFrameworkOutbox<InventoryDbContext>(o =>
            {
                o.UsePostgres();
                o.IsolationLevel = System.Data.IsolationLevel.ReadCommitted;
            });
            x.UsingInMemory((ctx, cfg) => endpoints(ctx, cfg));
        });
        return services.BuildServiceProvider(true);
    }

    // Events stored in the outbox are delivered to their stored destination address as raw
    // messages, which the harness does not index by type under "published"/"sent". What matters is
    // what a subscriber receives, so these probes stand in for the Order service's consumers.
    private sealed class ReservedProbe : IConsumer<InventoryReservedEvent>
    {
        public Task Consume(ConsumeContext<InventoryReservedEvent> context) => Task.CompletedTask;
    }

    private sealed class ReleasedProbe : IConsumer<InventoryReleasedEvent>
    {
        public Task Consume(ConsumeContext<InventoryReleasedEvent> context) => Task.CompletedTask;
    }

    private static async Task<bool> WasDelivered<T>(
        ITestHarness harness, Func<T, bool> match, TimeSpan wait) where T : class
    {
        using var timeout = new CancellationTokenSource(wait);
        try
        {
            return await harness.Consumed.Any<T>(x => match(x.Context.Message), timeout.Token);
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    [RequiresPostgresFact]
    public async Task ReserveThroughTheOutbox_ChangesStockAndDeliversTheEvent()
    {
        await using var database = await PostgresTestDatabase.CreateAsync("outbox");
        var options = await CreateInventoryAsync(database, quantity: 10);
        var orderId = Guid.NewGuid();

        await using var provider = BuildOutboxHost(database,
            x =>
            {
                x.AddConsumer<ReserveInventoryConsumer>();
                x.AddConsumer<ReservedProbe>();
            },
            (ctx, cfg) =>
            {
                cfg.ReceiveEndpoint("inventory-reserve", e =>
                {
                    e.UseEntityFrameworkOutbox<InventoryDbContext>(ctx);
                    e.ConfigureConsumer<ReserveInventoryConsumer>(ctx);
                });
                cfg.ReceiveEndpoint("probe-reserved", e => e.ConfigureConsumer<ReservedProbe>(ctx));
            });
        var harness = provider.GetRequiredService<ITestHarness>();
        await harness.Start();

        await harness.Bus.Publish(Reserve(orderId, 4));

        (await WasDelivered<InventoryReservedEvent>(
            harness, e => e.OrderId == orderId, TimeSpan.FromSeconds(15))).Should().BeTrue();

        await using var check = new InventoryDbContext(options);
        (await check.InventoryItems.SingleAsync()).ReservedQty.Should().Be(4);
        // the consumer joined MassTransit's transaction instead of opening its own
        (await check.Set<MassTransit.EntityFrameworkCoreIntegration.InboxState>().CountAsync())
            .Should().Be(1);
    }

    // Another process holds the stock row and does not let go (a hung admin tool, a crashed pod
    // whose session the server has not noticed yet). The consumer's SELECT ... FOR UPDATE used to
    // queue behind it for as long as it took, with the message and a connection tied up; now it gives
    // up after the lock timeout, the message is retried, and as soon as the row is free the
    // reservation goes through.
    [RequiresPostgresFact]
    public async Task AStuckRowLock_DelaysAReservation_ButDoesNotLoseIt()
    {
        await using var database = await PostgresTestDatabase.CreateAsync("stuck");
        var options = await CreateInventoryAsync(database, quantity: 10);
        var orderId = Guid.NewGuid();
        var injector = new FaultInjector(); // used here only to record what the server reports
        var hardened = PostgresConnection.Harden(database.ConnectionString,
            new PostgresResilienceOptions { LockTimeoutSeconds = 1 });

        await using var holder = new NpgsqlConnection(database.ConnectionString);
        await holder.OpenAsync();
        await using var holderTransaction = await holder.BeginTransactionAsync();
        await using (var hold = new NpgsqlCommand(
            $"SELECT * FROM \"InventoryItems\" WHERE \"ProductId\" = '{Product}' FOR UPDATE", holder, holderTransaction))
            await hold.ExecuteNonQueryAsync();

        await using var provider = BuildOutboxHost(database,
            x =>
            {
                x.AddConsumer<ReserveInventoryConsumer>();
                x.AddConsumer<ReservedProbe>();
            },
            (ctx, cfg) =>
            {
                cfg.UseConsumerResilience(TestResilience.FastConsumers());
                cfg.ReceiveEndpoint("inventory-reserve", e =>
                {
                    e.UseEntityFrameworkOutbox<InventoryDbContext>(ctx);
                    e.ConfigureConsumer<ReserveInventoryConsumer>(ctx);
                });
                cfg.ReceiveEndpoint("probe-reserved", e => e.ConfigureConsumer<ReservedProbe>(ctx));
            },
            hardened, injector);
        var harness = provider.GetRequiredService<ITestHarness>();
        await harness.Start();

        await harness.Bus.Publish(Reserve(orderId, 4));

        // the consumer runs into the lock timeout, repeatedly, instead of waiting without end
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (injector.ServerErrors.Count(code => code == "55P03") < 2 && DateTime.UtcNow < deadline)
            await Task.Delay(100);
        injector.ServerErrors.Count(code => code == "55P03").Should().BeGreaterThanOrEqualTo(2);
        (await WasDelivered<InventoryReservedEvent>(harness, e => e.OrderId == orderId, TimeSpan.FromMilliseconds(300)))
            .Should().BeFalse("the row is still locked by somebody else");

        await holderTransaction.RollbackAsync(); // the stuck holder goes away

        (await WasDelivered<InventoryReservedEvent>(
            harness, e => e.OrderId == orderId, TimeSpan.FromSeconds(20))).Should().BeTrue();
        await using var check = new InventoryDbContext(options);
        (await check.InventoryItems.SingleAsync()).ReservedQty.Should().Be(4);
    }

    // An event published by a consumer that then fails must not leave the building, and the
    // consumer's database changes must roll back with it.
    private sealed class PublishThenFailConsumer(InventoryDbContext db) : IConsumer<ReleaseInventoryCommand>
    {
        public async Task Consume(ConsumeContext<ReleaseInventoryCommand> context)
        {
            db.StockTransactions.Add(new StockTransaction
            {
                Id = Guid.NewGuid(), ProductId = Product, QuantityChange = 0,
                Reason = "should be rolled back", OrderId = context.Message.OrderId,
                CreatedAt = DateTime.UtcNow
            });
            await db.SaveChangesAsync();
            await context.Publish(new InventoryReleasedEvent { OrderId = context.Message.OrderId });
            throw new InvalidOperationException("consumer failed after publishing");
        }
    }

    [RequiresPostgresFact]
    public async Task AConsumerThatFailsAfterPublishing_SendsNothingAndRollsBack()
    {
        await using var database = await PostgresTestDatabase.CreateAsync("atomic");
        var options = await CreateInventoryAsync(database, quantity: 10);
        var orderId = Guid.NewGuid();

        await using var provider = BuildOutboxHost(database,
            x =>
            {
                x.AddConsumer<PublishThenFailConsumer>();
                x.AddConsumer<ReleasedProbe>();
            },
            (ctx, cfg) =>
            {
                cfg.ReceiveEndpoint("inventory-release", e =>
                {
                    e.UseEntityFrameworkOutbox<InventoryDbContext>(ctx);
                    e.ConfigureConsumer<PublishThenFailConsumer>(ctx);
                });
                cfg.ReceiveEndpoint("probe-released", e => e.ConfigureConsumer<ReleasedProbe>(ctx));
            });
        var harness = provider.GetRequiredService<ITestHarness>();
        await harness.Start();

        await harness.Bus.Publish(new ReleaseInventoryCommand { OrderId = orderId });

        // how long to wait is decided here; AllowSlowRunners stops the harness cutting it short
        using var faultTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        (await harness.Published.Any<Fault<ReleaseInventoryCommand>>(faultTimeout.Token)).Should().BeTrue();
        (await WasDelivered<InventoryReleasedEvent>(
            harness, _ => true, TimeSpan.FromSeconds(2))).Should().BeFalse();

        await using var check = new InventoryDbContext(options);
        (await check.StockTransactions.CountAsync()).Should().Be(0);
        (await check.Set<MassTransit.EntityFrameworkCoreIntegration.OutboxMessage>().CountAsync())
            .Should().Be(0);
    }

    // ── UnitOfWork ────────────────────────────────────────────────────────────

    [RequiresPostgresFact]
    public async Task UnitOfWork_OpensItsOwnTransaction_WhenNoneIsActive()
    {
        await using var database = await PostgresTestDatabase.CreateAsync("uow");
        var options = await CreateInventoryAsync(database, quantity: 1);
        await using var db = new InventoryDbContext(options);

        await using var unit = await db.Database.BeginOrJoinAsync();

        unit.OwnsTransaction.Should().BeTrue();
        db.Database.CurrentTransaction.Should().NotBeNull();
        await unit.RollbackAsync();
    }

    [RequiresPostgresFact]
    public async Task UnitOfWork_JoinsTheActiveTransaction_AndLeavesCommitToItsOwner()
    {
        await using var database = await PostgresTestDatabase.CreateAsync("uowjoin");
        var options = await CreateInventoryAsync(database, quantity: 1);
        await using var db = new InventoryDbContext(options);

        await using var outer = await db.Database.BeginTransactionAsync();
        await using (var unit = await db.Database.BeginOrJoinAsync())
        {
            unit.OwnsTransaction.Should().BeFalse();
            db.InventoryItems.Single().Quantity = 99;
            await db.SaveChangesAsync();
            await unit.CommitAsync();      // no-op: the outer transaction is still open
        }

        db.Database.CurrentTransaction.Should().BeSameAs(outer);
        await outer.RollbackAsync();       // the owner decides — and the change is gone

        await using var check = new InventoryDbContext(options);
        (await check.InventoryItems.SingleAsync()).Quantity.Should().Be(1);
    }
}
