using FluentAssertions;
using MassTransit;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using ShopEasy.OrderService.Data;
using ShopEasy.OrderService.ReadModels;
using ShopEasy.OrderService.Sagas;
using ShopEasy.OrderService.Services;
using ShopEasy.Shared.Contracts;
using ShopEasy.Shared.Messaging;
using Xunit;

namespace ShopEasy.UnitTests.Sagas;

// The saga state machine as a whole: every path an order can take from Started to a final
// state, what happens on duplicates and unknown orders, and what the timeout service does
// with a saga that is stuck in each state.
//
// Single-handler behaviour in depth lives next to this file:
//   CommitFlowTests     — payment / commit results, duplicates, late events
//   CancellationTests   — customer cancellation and its effect on the handlers
//   SagaOrchestratorTests — the saga starter and the first handlers
// The saga row locking itself needs a real PostgreSQL: Infrastructure/SagaPostgresTests.
public class SagaStateTransitionTests : IDisposable
{
    private readonly OrderDbContext _db;
    private readonly IPublishEndpoint _publish = Substitute.For<IPublishEndpoint>();
    private readonly ISagaLocking _locking = new InMemorySagaLocking();
    private readonly OrderReadModelUpdater _readModel;

    public SagaStateTransitionTests()
    {
        var options = new DbContextOptionsBuilder<OrderDbContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString())
            .ConfigureWarnings(w => w.Ignore(InMemoryEventId.TransactionIgnoredWarning))
            .Options;

        _db = new OrderDbContext(options);
        _readModel = new OrderReadModelUpdater(_db, NullLogger<OrderReadModelUpdater>.Instance);
    }

    public void Dispose() => _db.Dispose();

    // ── consumers under test, built the way the DI container builds them ─────

    private OrderConfirmedSagaStarter Starter() =>
        new(_db, NullLogger<OrderConfirmedSagaStarter>.Instance);

    private InventoryReservedConsumer Reserved() =>
        new(_db, _locking, _publish, NullLogger<InventoryReservedConsumer>.Instance);

    private InventoryReservationFailedConsumer ReservationFailed() =>
        new(_db, _locking, _readModel, NullLogger<InventoryReservationFailedConsumer>.Instance);

    private PaymentSucceededConsumer PaymentSucceeded() =>
        new(_db, _locking, _publish, _readModel, NullLogger<PaymentSucceededConsumer>.Instance);

    private PaymentFailedConsumer PaymentFailed() =>
        new(_db, _locking, _publish, _readModel, NullLogger<PaymentFailedConsumer>.Instance);

    private InventoryCommittedConsumer Committed() =>
        new(_db, _locking, _readModel, NullLogger<InventoryCommittedConsumer>.Instance);

    private InventoryReleasedConsumer Released() =>
        new(_db, _locking, _readModel, NullLogger<InventoryReleasedConsumer>.Instance);

    private static SagaTimeoutService TimeoutService() =>
        new(Substitute.For<IServiceScopeFactory>(), NullLogger<SagaTimeoutService>.Instance);

    // ── Happy path ───────────────────────────────────────────────────────────

    [Fact]
    public async Task OrderConfirmed_CreatesTheSagaInStartedState()
    {
        var orderId = Guid.NewGuid();

        await Starter().Consume(Ctx(new OrderConfirmedEvent
        {
            OrderId = orderId, UserId = "user-1", UserEmail = "user@test.com",
            TotalAmount = 100m, ConfirmedAt = DateTime.UtcNow,
            Items = [new() { ProductId = Guid.NewGuid(), ProductName = "Widget", Quantity = 2, UnitPrice = 50m }]
        }));

        var saga = await SagaOf(orderId);
        saga.Status.Should().Be(SagaStatus.Started);
        saga.TotalAmount.Should().Be(100m);
        saga.UserId.Should().Be("user-1");
    }

    [Fact]
    public async Task FullHappyPath_EveryTransitionIsCorrect_AndTheOrderIsConfirmedOnlyAtTheEnd()
    {
        var orderId = Guid.NewGuid();
        await SeedOrder(orderId);

        // 1. the saga starter creates the saga
        await Starter().Consume(Ctx(OrderConfirmed(orderId, 200m)));
        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.Started);

        // 2. the orchestrator moves it to ReservingInventory (it sends ReserveInventoryCommand)
        await SetStatus(orderId, SagaStatus.ReservingInventory);

        // 3. stock reserved → payment is requested
        await Reserved().Consume(Ctx(new InventoryReservedEvent { OrderId = orderId }));
        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.ProcessingPayment);

        // 4. payment succeeded → the stock commit is requested; the order is NOT complete yet
        await PaymentSucceeded().Consume(Ctx(new PaymentSucceededEvent
            { OrderId = orderId, PaymentId = Guid.NewGuid(), Amount = 200m }));
        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.Completing);
        (await OrderOf(orderId)).Status.Should().Be(OrderStatus.Pending);

        // 5. inventory confirmed the commit → done
        await Committed().Consume(Ctx(new InventoryCommittedEvent { OrderId = orderId }));
        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.Completed);
        (await OrderOf(orderId)).Status.Should().Be(OrderStatus.Confirmed);
        (await SummaryOf(orderId)).StatusName.Should().Be("Confirmed");

        // the commands went out in order, each exactly once
        Received.InOrder(() =>
        {
            _publish.Publish(Arg.Is<ProcessPaymentCommand>(c => c.OrderId == orderId && c.Amount == 200m),
                Arg.Any<CancellationToken>());
            _publish.Publish(Arg.Is<CommitInventoryCommand>(c => c.OrderId == orderId),
                Arg.Any<CancellationToken>());
        });
        await _publish.DidNotReceive().Publish(Arg.Any<ReleaseInventoryCommand>(), Arg.Any<CancellationToken>());
        await _publish.DidNotReceive().Publish(Arg.Any<RefundPaymentCommand>(), Arg.Any<CancellationToken>());
    }

    // ── Compensation ─────────────────────────────────────────────────────────

    [Fact]
    public async Task FullCompensationPath_PaymentDeclined_ReleasesTheStockAndCancelsTheOrder()
    {
        var orderId = await Seed(SagaStatus.ProcessingPayment);

        // payment declined → give the reserved stock back
        await PaymentFailed().Consume(Ctx(new PaymentFailedEvent { OrderId = orderId, Reason = "Declined" }));
        var saga = await SagaOf(orderId);
        saga.Status.Should().Be(SagaStatus.ReleasingInventory);
        saga.FailureReason.Should().Be("Declined");
        await _publish.Received(1).Publish(
            Arg.Is<ReleaseInventoryCommand>(c => c.OrderId == orderId), Arg.Any<CancellationToken>());

        // inventory confirms the release → cancelled, with the reason the customer sees
        await Released().Consume(Ctx(new InventoryReleasedEvent { OrderId = orderId }));
        saga = await SagaOf(orderId);
        saga.Status.Should().Be(SagaStatus.Cancelled);
        saga.FailureReason.Should().Be("Declined");
        (await OrderOf(orderId)).Status.Should().Be(OrderStatus.Cancelled);
        (await SummaryOf(orderId)).StatusName.Should().Be("Cancelled");
    }

    [Fact]
    public async Task InsufficientStock_CancelsTheOrderWithTheReason_AndPublishesNothing()
    {
        var orderId = await Seed(SagaStatus.ReservingInventory);

        await ReservationFailed().Consume(Ctx(new InventoryReservationFailedEvent
            { OrderId = orderId, Reason = "Insufficient stock for 4K Webcam" }));

        var saga = await SagaOf(orderId);
        saga.Status.Should().Be(SagaStatus.Cancelled);
        saga.FailureReason.Should().Contain("Insufficient stock");
        (await OrderOf(orderId)).Status.Should().Be(OrderStatus.Cancelled);
        (await SummaryOf(orderId)).FailureReason.Should().Contain("Insufficient stock");

        // nothing was reserved, so there is nothing to release, charge or refund
        await _publish.DidNotReceive().Publish(Arg.Any<ReleaseInventoryCommand>(), Arg.Any<CancellationToken>());
        await _publish.DidNotReceive().Publish(Arg.Any<ProcessPaymentCommand>(), Arg.Any<CancellationToken>());
    }

    // ── Duplicates ───────────────────────────────────────────────────────────

    [Fact]
    public async Task DuplicateOrderConfirmed_CreatesOnlyOneSaga()
    {
        var orderId = Guid.NewGuid();
        var evt = OrderConfirmed(orderId, 100m);

        await Starter().Consume(Ctx(evt));
        await Starter().Consume(Ctx(evt));

        (await _db.SagaStates.CountAsync(s => s.OrderId == orderId)).Should().Be(1);
    }

    [Fact]
    public async Task DuplicateInventoryReserved_RequestsThePaymentOnlyOnce()
    {
        var orderId = await Seed(SagaStatus.ReservingInventory);

        await Reserved().Consume(Ctx(new InventoryReservedEvent { OrderId = orderId }));
        await Reserved().Consume(Ctx(new InventoryReservedEvent { OrderId = orderId }));

        await _publish.Received(1).Publish(
            Arg.Is<ProcessPaymentCommand>(c => c.OrderId == orderId), Arg.Any<CancellationToken>());
    }

    // ── Edge cases ───────────────────────────────────────────────────────────

    [Fact]
    public async Task EventsForAnOrderWithoutASaga_DoNothing()
    {
        var unknown = Guid.NewGuid();

        await Reserved().Consume(Ctx(new InventoryReservedEvent { OrderId = unknown }));
        await PaymentFailed().Consume(Ctx(new PaymentFailedEvent { OrderId = unknown, Reason = "Declined" }));
        await ReservationFailed().Consume(Ctx(new InventoryReservationFailedEvent { OrderId = unknown, Reason = "x" }));
        await PaymentSucceeded().Consume(Ctx(new PaymentSucceededEvent { OrderId = unknown, PaymentId = Guid.NewGuid(), Amount = 1m }));

        _publish.ReceivedCalls().Should().BeEmpty();
        (await _db.SagaStates.CountAsync()).Should().Be(0);
    }

    [Fact]
    public async Task AZeroAmountOrder_IsStoredAsIs()
    {
        var orderId = Guid.NewGuid();

        await Starter().Consume(Ctx(OrderConfirmed(orderId, 0m)));

        (await SagaOf(orderId)).TotalAmount.Should().Be(0m);
    }

    // ── Timeouts: a saga that stopped making progress ────────────────────────

    private static readonly DateTime LongAgo = DateTime.UtcNow.AddMinutes(-5);

    private Task RunTimeoutCheck() =>
        TimeoutService().CheckForTimedOutSagasPublic(_db, _publish, _locking, CancellationToken.None);

    [Theory]
    [InlineData(SagaStatus.Started)]
    [InlineData(SagaStatus.ReservingInventory)]
    public async Task Timeout_WaitingForInventory_FailsTheSaga_AndCancelsTheOrder(SagaStatus stuckIn)
    {
        var orderId = await Seed(stuckIn, updatedAt: LongAgo);

        await RunTimeoutCheck();

        var saga = await SagaOf(orderId);
        saga.Status.Should().Be(SagaStatus.Failed);
        saga.FailureReason.Should().Contain("Timed out waiting for inventory");
        (await OrderOf(orderId)).Status.Should().Be(OrderStatus.Cancelled);
        (await SummaryOf(orderId)).StatusName.Should().Be("Cancelled");
        // a reservation might still be on its way: nothing was confirmed, so nothing to give back
        // here — a late InventoryReservedEvent is released by the zombie guard (CancellationTests)
        await _publish.DidNotReceive().Publish(Arg.Any<ProcessPaymentCommand>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Timeout_WaitingForPayment_ReleasesTheStock()
    {
        var orderId = await Seed(SagaStatus.ProcessingPayment, updatedAt: LongAgo);

        await RunTimeoutCheck();

        await _publish.Received(1).Publish(
            Arg.Is<ReleaseInventoryCommand>(c => c.OrderId == orderId), Arg.Any<CancellationToken>());
        var saga = await SagaOf(orderId);
        saga.Status.Should().Be(SagaStatus.ReleasingInventory);
        saga.FailureReason.Should().Contain("Timed out waiting for payment");
    }

    [Fact]
    public async Task Timeout_WaitingForTheCommitResult_ResendsTheCommit()
    {
        var orderId = await Seed(SagaStatus.Completing, updatedAt: LongAgo);

        await RunTimeoutCheck();

        var saga = await SagaOf(orderId);
        saga.Status.Should().Be(SagaStatus.Completing);
        saga.RetryCount.Should().Be(1);
        await _publish.Received(1).Publish(
            Arg.Is<CommitInventoryCommand>(c => c.OrderId == orderId), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Timeout_ACommitThatNeverConfirms_IsFlaggedForAHuman_NotRetriedForever()
    {
        var orderId = await Seed(SagaStatus.Completing, updatedAt: LongAgo, retryCount: 10);

        await RunTimeoutCheck();

        var saga = await SagaOf(orderId);
        saga.Status.Should().Be(SagaStatus.Completing);          // the payment is taken: never auto-cancel
        saga.FailureReason.Should().Contain("stuck");
        await _publish.DidNotReceive().Publish(Arg.Any<CommitInventoryCommand>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Timeout_AStuckCommit_KeepsTheCancellationThatWasAskedFor()
    {
        var orderId = await Seed(SagaStatus.Completing, updatedAt: LongAgo, retryCount: 10);
        (await _db.SagaStates.SingleAsync(s => s.OrderId == orderId)).FailureReason = "Cancelled by customer";
        await _db.SaveChangesAsync();

        await RunTimeoutCheck();

        (await SagaOf(orderId)).FailureReason.Should().Be("Cancelled by customer");
    }

    [Theory]
    [InlineData(SagaStatus.Completed)]
    [InlineData(SagaStatus.Cancelled)]
    [InlineData(SagaStatus.Failed)]
    public async Task Timeout_FinishedSagas_AreLeftAlone(SagaStatus status)
    {
        var orderId = await Seed(status, updatedAt: DateTime.UtcNow.AddHours(-1));

        await RunTimeoutCheck();

        (await SagaOf(orderId)).Status.Should().Be(status);
        _publish.ReceivedCalls().Should().BeEmpty();
    }

    [Fact]
    public async Task Timeout_ASagaThatIsStillFresh_IsLeftAlone()
    {
        var orderId = await Seed(SagaStatus.ReservingInventory, updatedAt: DateTime.UtcNow.AddSeconds(-30));

        await RunTimeoutCheck();

        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.ReservingInventory);
        _publish.ReceivedCalls().Should().BeEmpty();
    }

    // ── Correlation: the background workers have no HTTP request ────────────

    [Fact]
    public async Task TheSaga_RemembersTheCorrelationIdOfTheRequestThatPlacedTheOrder()
    {
        var orderId = Guid.NewGuid();

        // the consume filter restores the id from the message header before the starter runs
        using (CorrelationContext.Push("req-77"))
            await Starter().Consume(Ctx(OrderConfirmed(orderId, 10m)));

        (await SagaOf(orderId)).CorrelationId.Should().Be("req-77");
    }

    [Fact]
    public async Task TheOrchestrator_SendsTheReserveCommandInTheTraceOfTheOriginalRequest()
    {
        var orderId = await Seed(SagaStatus.Started, correlationId: "req-77");
        string? seen = "not published";
        _publish.Publish(Arg.Any<ReserveInventoryCommand>(), Arg.Any<CancellationToken>())
            .Returns(_ => { seen = CorrelationContext.Current; return Task.CompletedTask; });
        var orchestrator = new SagaOrchestrator(
            Substitute.For<IServiceScopeFactory>(), NullLogger<SagaOrchestrator>.Instance);

        await orchestrator.ProcessSaga(
            await SagaOf(orderId), _db, _publish, _locking, CancellationToken.None);

        seen.Should().Be("req-77");
        CorrelationContext.Current.Should().BeNull("the scope ends with the saga");
    }

    [Fact]
    public async Task TheTimeoutService_ReleasesStockInTheTraceOfTheOriginalRequest()
    {
        var orderId = await Seed(SagaStatus.ProcessingPayment, updatedAt: LongAgo, correlationId: "req-77");
        string? seen = "not published";
        _publish.Publish(Arg.Any<ReleaseInventoryCommand>(), Arg.Any<CancellationToken>())
            .Returns(_ => { seen = CorrelationContext.Current; return Task.CompletedTask; });

        await RunTimeoutCheck();

        seen.Should().Be("req-77");
        CorrelationContext.Current.Should().BeNull();
    }

    [Fact]
    public async Task ASagaFromBeforeCorrelationIds_StillWorks()
    {
        var orderId = await Seed(SagaStatus.ProcessingPayment, updatedAt: LongAgo, correlationId: null);

        await RunTimeoutCheck();

        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.ReleasingInventory);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static OrderConfirmedEvent OrderConfirmed(Guid orderId, decimal total) => new()
    {
        OrderId = orderId, UserId = "user-1", UserEmail = "user@test.com",
        TotalAmount = total, ConfirmedAt = DateTime.UtcNow, Items = []
    };

    // Order + read model row, without a saga
    private async Task SeedOrder(Guid orderId)
    {
        var now = DateTime.UtcNow;
        _db.Orders.Add(new Order
        {
            Id = orderId, UserId = "user-1", UserEmail = "user@test.com",
            Status = OrderStatus.Pending, TotalAmount = 100m, CreatedAt = now, UpdatedAt = now
        });
        _db.OrderSummaries.Add(new OrderSummary
        {
            OrderId = orderId, UserId = "user-1", UserEmail = "user@test.com",
            StatusName = "Pending", StatusCode = (int)OrderStatus.Pending, TotalAmount = 100m,
            AmountDisplay = "$100.00", ItemCount = 1, ItemsSummary = "Widget x1",
            PlacedAt = now, UpdatedAt = now
        });
        await _db.SaveChangesAsync();
    }

    // Order + read model row + a saga in the given state
    private async Task<Guid> Seed(
        SagaStatus status, DateTime? updatedAt = null, int retryCount = 0, string? correlationId = null)
    {
        var orderId = Guid.NewGuid();
        await SeedOrder(orderId);
        var now = DateTime.UtcNow;
        _db.SagaStates.Add(new OrderSagaState
        {
            Id = Guid.NewGuid(), OrderId = orderId, Status = status,
            UserId = "user-1", UserEmail = "user@test.com", TotalAmount = 100m,
            ItemsJson = "[]", RetryCount = retryCount, CorrelationId = correlationId,
            CreatedAt = updatedAt ?? now, UpdatedAt = updatedAt ?? now
        });
        await _db.SaveChangesAsync();
        return orderId;
    }

    private async Task SetStatus(Guid orderId, SagaStatus status)
    {
        var saga = await _db.SagaStates.FirstAsync(s => s.OrderId == orderId);
        saga.Status = status;
        saga.UpdatedAt = DateTime.UtcNow;
        await _db.SaveChangesAsync();
    }

    // Reads bypass the change tracker so they see what is really stored
    private Task<OrderSagaState> SagaOf(Guid orderId) =>
        _db.SagaStates.AsNoTracking().FirstAsync(s => s.OrderId == orderId);

    private Task<Order> OrderOf(Guid orderId) =>
        _db.Orders.AsNoTracking().FirstAsync(o => o.Id == orderId);

    private Task<OrderSummary> SummaryOf(Guid orderId) =>
        _db.OrderSummaries.AsNoTracking().FirstAsync(o => o.OrderId == orderId);

    private static ConsumeContext<T> Ctx<T>(T message) where T : class
    {
        var context = Substitute.For<ConsumeContext<T>>();
        context.Message.Returns(message);
        context.CancellationToken.Returns(CancellationToken.None);
        return context;
    }
}
