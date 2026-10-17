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
using System.Text.Json;
using Xunit;

namespace ShopEasy.UnitTests.Sagas;

// Cancelling an order goes through the saga: stock is released and payments refunded by the
// same state machine that created them, never by just flipping the order status.
public class CancellationTests : IDisposable
{
    private const string Owner = "user-1";
    private const string Stranger = "user-2";
    private static readonly Guid ProductA = Guid.Parse("00000000-0000-0000-0000-00000000000a");

    private readonly OrderDbContext _db;
    private readonly IPublishEndpoint _publish = Substitute.For<IPublishEndpoint>();
    private readonly ISagaLocking _locking = new InMemorySagaLocking();
    private readonly OrderReadModelUpdater _readModel;
    private readonly OrderCancellationService _service;

    public CancellationTests()
    {
        var options = new DbContextOptionsBuilder<OrderDbContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString())
            .ConfigureWarnings(w => w.Ignore(InMemoryEventId.TransactionIgnoredWarning))
            .Options;

        _db = new OrderDbContext(options);
        _readModel = new OrderReadModelUpdater(_db, NullLogger<OrderReadModelUpdater>.Instance);
        _service = new OrderCancellationService(
            _db, _locking, _publish, _readModel, NullLogger<OrderCancellationService>.Instance);
    }

    public void Dispose() => _db.Dispose();

    // ── helpers ──────────────────────────────────────────────────────────────

    private async Task<Guid> Seed(
        SagaStatus? sagaStatus,
        OrderStatus orderStatus = OrderStatus.Pending,
        string userId = Owner,
        DateTime? sagaUpdatedAt = null,
        int retryCount = 0)
    {
        var orderId = Guid.NewGuid();
        var now = DateTime.UtcNow;

        _db.Orders.Add(new Order
        {
            Id = orderId, UserId = userId, UserEmail = "u@test.com",
            Status = orderStatus, TotalAmount = 20m, CreatedAt = now, UpdatedAt = now,
            Items = [new OrderItem
            {
                Id = Guid.NewGuid(), ProductId = ProductA, ProductName = "Widget",
                Quantity = 2, UnitPrice = 10m
            }]
        });
        _db.OrderSummaries.Add(new OrderSummary
        {
            OrderId = orderId, UserId = userId, UserEmail = "u@test.com",
            StatusName = orderStatus.ToString(), StatusCode = (int)orderStatus, TotalAmount = 20m,
            AmountDisplay = "$20.00", ItemCount = 2, ItemsSummary = "Widget x2",
            PlacedAt = now, UpdatedAt = now
        });
        if (sagaStatus is not null)
        {
            _db.SagaStates.Add(new OrderSagaState
            {
                Id = Guid.NewGuid(), OrderId = orderId, Status = sagaStatus.Value,
                UserId = userId, UserEmail = "u@test.com", TotalAmount = 20m, ItemsJson = "[]",
                RetryCount = retryCount, CreatedAt = now, UpdatedAt = sagaUpdatedAt ?? now
            });
        }
        await _db.SaveChangesAsync();
        return orderId;
    }

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

    private async Task AssertNothingPublished()
    {
        await _publish.DidNotReceive().Publish(
            Arg.Any<ReleaseInventoryCommand>(), Arg.Any<CancellationToken>());
        await _publish.DidNotReceive().Publish(
            Arg.Any<RefundPaymentCommand>(), Arg.Any<CancellationToken>());
    }

    // ── cancelling at each stage of the saga ─────────────────────────────────

    [Fact]
    public async Task BeforeTheSagaExists_CancelsImmediately_AndTheSagaStarterSkipsTheOrder()
    {
        var orderId = await Seed(sagaStatus: null);

        var result = await _service.CancelAsync(orderId, Owner, isAdmin: false);

        result.Outcome.Should().Be(CancelOutcome.Cancelled);
        (await OrderOf(orderId)).Status.Should().Be(OrderStatus.Cancelled);
        (await SummaryOf(orderId)).StatusName.Should().Be("Cancelled");
        var saga = await SagaOf(orderId);
        saga.Status.Should().Be(SagaStatus.Cancelled);
        saga.FailureReason.Should().Be("Cancelled by customer");
        JsonSerializer.Deserialize<List<OrderLineItem>>(saga.ItemsJson)!
            .Should().ContainSingle().Which.Quantity.Should().Be(2);
        await AssertNothingPublished();

        // The (late) outbox event now reaches the saga starter: it must not start a saga
        var starter = new OrderConfirmedSagaStarter(_db, NullLogger<OrderConfirmedSagaStarter>.Instance);
        await starter.Consume(Ctx(new OrderConfirmedEvent
        {
            OrderId = orderId, UserId = Owner, UserEmail = "u@test.com", TotalAmount = 20m,
            ConfirmedAt = DateTime.UtcNow, Items = new List<OrderLineItem>()
        }));

        _db.SagaStates.Count(s => s.OrderId == orderId).Should().Be(1);
        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.Cancelled);
    }

    [Fact]
    public async Task WhenTheSagaHasNotStartedReserving_CancelsImmediately_WithoutTouchingInventory()
    {
        var orderId = await Seed(SagaStatus.Started);

        var result = await _service.CancelAsync(orderId, Owner, isAdmin: false);

        result.Outcome.Should().Be(CancelOutcome.Cancelled);
        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.Cancelled);
        (await OrderOf(orderId)).Status.Should().Be(OrderStatus.Cancelled);
        await AssertNothingPublished();
    }

    [Theory]
    [InlineData(SagaStatus.ReservingInventory)]
    [InlineData(SagaStatus.ProcessingPayment)]
    public async Task WhileStockIsReservedOrPaymentIsInFlight_HandsOverToTheSagasCompensation(SagaStatus status)
    {
        var orderId = await Seed(status);

        var result = await _service.CancelAsync(orderId, Owner, isAdmin: false);

        result.Outcome.Should().Be(CancelOutcome.Cancelling);
        var saga = await SagaOf(orderId);
        saga.Status.Should().Be(SagaStatus.ReleasingInventory);
        saga.FailureReason.Should().Be("Cancelled by customer");
        // the order is only Cancelled once Inventory confirms the release
        (await OrderOf(orderId)).Status.Should().Be(OrderStatus.Pending);
        (await SummaryOf(orderId)).FailureReason.Should().Be("Cancelled by customer");
        await _publish.Received(1).Publish(
            Arg.Is<ReleaseInventoryCommand>(c => c.OrderId == orderId), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task WhenTheSagaIsAlreadyReleasingStock_ReportsInProgress_WithoutSendingAnotherCommand()
    {
        var orderId = await Seed(SagaStatus.ReleasingInventory);

        var result = await _service.CancelAsync(orderId, Owner, isAdmin: false);

        result.Outcome.Should().Be(CancelOutcome.Cancelling);
        await AssertNothingPublished();
    }

    [Fact]
    public async Task WhileTheStockCommitIsInFlight_TheRequestIsKept_AndNothingIsSentYet()
    {
        var orderId = await Seed(SagaStatus.Completing, OrderStatus.Pending);

        var result = await _service.CancelAsync(orderId, Owner, isAdmin: false);

        result.Outcome.Should().Be(CancelOutcome.Cancelling);
        var saga = await SagaOf(orderId);
        (saga.Status, saga.FailureReason).Should().Be((SagaStatus.Completing, "Cancelled by customer"));
        (await OrderOf(orderId)).Status.Should().Be(OrderStatus.Pending);
        await AssertNothingPublished();
    }

    [Theory]
    [InlineData(SagaStatus.Completed, OrderStatus.Shipped)]
    [InlineData(SagaStatus.Completed, OrderStatus.Delivered)]
    public async Task AfterShipping_TheOrderCanNoLongerBeCancelled(SagaStatus saga, OrderStatus order)
    {
        var orderId = await Seed(saga, order);

        var result = await _service.CancelAsync(orderId, Owner, isAdmin: false);

        result.Outcome.Should().Be(CancelOutcome.NotCancellable);
        result.Message.Should().NotBeNullOrEmpty();
        (await SagaOf(orderId)).Status.Should().Be(saga);
        (await OrderOf(orderId)).Status.Should().Be(order);
        await AssertNothingPublished();
    }

    [Theory]
    [InlineData(false, "Cancelled by customer")]
    [InlineData(true, "Cancelled by admin")]
    public async Task AConfirmedOrder_IsCancelledByRefundingTheMoneyFirst(bool admin, string reason)
    {
        var orderId = await Seed(SagaStatus.Completed, OrderStatus.Confirmed);

        var result = await _service.CancelAsync(orderId, admin ? "an-admin" : Owner, isAdmin: admin);

        result.Outcome.Should().Be(CancelOutcome.Cancelling);
        var saga = await SagaOf(orderId);
        (saga.Status, saga.FailureReason).Should().Be((SagaStatus.RefundingPayment, reason));
        (await OrderOf(orderId)).Status.Should().Be(OrderStatus.Confirmed, "it is only Cancelled once the money and the stock are back");
        var summary = await SummaryOf(orderId);
        (summary.PaymentStatus, summary.FailureReason).Should().Be(("Refunding", reason));
        await _publish.Received(1).Publish(
            Arg.Is<RefundPaymentCommand>(c => c.OrderId == orderId && c.Reason == reason), Arg.Any<CancellationToken>());
        await _publish.DidNotReceive().Publish(Arg.Any<ReleaseInventoryCommand>(), Arg.Any<CancellationToken>());
    }

    [Theory]
    [InlineData(SagaStatus.RefundingPayment)]
    [InlineData(SagaStatus.RestockingInventory)]
    public async Task CancellingAPaidOrderThatIsAlreadyBeingCancelled_ReportsInProgress_WithoutAnotherCommand(SagaStatus status)
    {
        var orderId = await Seed(status, OrderStatus.Confirmed);

        var result = await _service.CancelAsync(orderId, Owner, isAdmin: false);

        result.Outcome.Should().Be(CancelOutcome.Cancelling);
        (await SagaOf(orderId)).Status.Should().Be(status);
        await AssertNothingPublished();
    }

    [Fact]
    public async Task CancellingTwice_IsHarmless()
    {
        var orderId = await Seed(SagaStatus.Cancelled, OrderStatus.Cancelled);

        var result = await _service.CancelAsync(orderId, Owner, isAdmin: false);

        result.Outcome.Should().Be(CancelOutcome.AlreadyCancelled);
        await AssertNothingPublished();
    }

    [Fact]
    public async Task WhenTheSagaAlreadyFailed_TheOrderIsMarkedCancelled()
    {
        // e.g. the orchestrator gave up on the saga but left the order Pending
        var orderId = await Seed(SagaStatus.Failed, OrderStatus.Pending);

        var result = await _service.CancelAsync(orderId, Owner, isAdmin: false);

        result.Outcome.Should().Be(CancelOutcome.AlreadyCancelled);
        (await OrderOf(orderId)).Status.Should().Be(OrderStatus.Cancelled);
        (await SummaryOf(orderId)).StatusName.Should().Be("Cancelled");
    }

    // ── who may cancel ───────────────────────────────────────────────────────

    [Fact]
    public async Task AnotherCustomersOrder_LooksLikeItDoesNotExist()
    {
        var orderId = await Seed(SagaStatus.ProcessingPayment);

        var result = await _service.CancelAsync(orderId, Stranger, isAdmin: false);

        result.Outcome.Should().Be(CancelOutcome.NotFound);
        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.ProcessingPayment);
        await AssertNothingPublished();
    }

    [Fact]
    public async Task AnAdminCanCancelAnyOrder_AndTheReasonSaysSo()
    {
        var orderId = await Seed(SagaStatus.ProcessingPayment);

        var result = await _service.CancelAsync(orderId, Stranger, isAdmin: true);

        result.Outcome.Should().Be(CancelOutcome.Cancelling);
        (await SagaOf(orderId)).FailureReason.Should().Be("Cancelled by admin");
    }

    [Fact]
    public async Task UnknownOrder_IsNotFound()
    {
        var result = await _service.CancelAsync(Guid.NewGuid(), Owner, isAdmin: false);

        result.Outcome.Should().Be(CancelOutcome.NotFound);
    }

    // ── resilience ───────────────────────────────────────────────────────────

    [Fact]
    public async Task IfTheReleaseCommandCannotBePublished_TheCancellationIsStillSaved()
    {
        var orderId = await Seed(SagaStatus.ProcessingPayment);
        _publish.Publish(Arg.Any<ReleaseInventoryCommand>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("broker down")));

        var result = await _service.CancelAsync(orderId, Owner, isAdmin: false);

        // the saga timeout service re-sends the (idempotent) release command
        result.Outcome.Should().Be(CancelOutcome.Cancelling);
        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.ReleasingInventory);
    }

    // ── what happens after the cancel (saga handlers) ────────────────────────

    [Fact]
    public async Task AfterTheCancel_InventoryConfirmingTheRelease_CancelsTheOrder()
    {
        var orderId = await Seed(SagaStatus.ProcessingPayment);
        await _service.CancelAsync(orderId, Owner, isAdmin: false);

        var released = new InventoryReleasedConsumer(
            _db, _locking, _readModel, NullLogger<InventoryReleasedConsumer>.Instance);
        await released.Consume(Ctx(new InventoryReleasedEvent { OrderId = orderId }));

        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.Cancelled);
        (await OrderOf(orderId)).Status.Should().Be(OrderStatus.Cancelled);
        (await SummaryOf(orderId)).StatusName.Should().Be("Cancelled");
    }

    [Fact]
    public async Task AfterTheCancel_APaymentThatStillSucceeds_IsRefunded_AndNeverCommitted()
    {
        var orderId = await Seed(SagaStatus.ProcessingPayment);
        await _service.CancelAsync(orderId, Owner, isAdmin: false);

        var paid = new PaymentSucceededConsumer(
            _db, _locking, _publish, _readModel, NullLogger<PaymentSucceededConsumer>.Instance);
        await paid.Consume(Ctx(new PaymentSucceededEvent
        {
            OrderId = orderId, PaymentId = Guid.NewGuid(), Amount = 20m
        }));

        await _publish.Received(1).Publish(
            Arg.Is<RefundPaymentCommand>(c => c.OrderId == orderId), Arg.Any<CancellationToken>());
        await _publish.DidNotReceive().Publish(
            Arg.Any<CommitInventoryCommand>(), Arg.Any<CancellationToken>());
        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.ReleasingInventory);
    }

    [Fact]
    public async Task AfterTheCancel_ALateReservation_IsReleasedAgain()
    {
        // the cancel overtook the reserve command: the reservation arrives after the cancel
        var orderId = await Seed(SagaStatus.ReservingInventory);
        await _service.CancelAsync(orderId, Owner, isAdmin: false);
        _publish.ClearReceivedCalls();

        var reserved = new InventoryReservedConsumer(
            _db, _locking, _publish, NullLogger<InventoryReservedConsumer>.Instance);
        await reserved.Consume(Ctx(new InventoryReservedEvent { OrderId = orderId }));

        await _publish.Received(1).Publish(
            Arg.Is<ReleaseInventoryCommand>(c => c.OrderId == orderId), Arg.Any<CancellationToken>());
        await _publish.DidNotReceive().Publish(
            Arg.Any<ProcessPaymentCommand>(), Arg.Any<CancellationToken>());
    }

    // ── timeout service: lost release commands ───────────────────────────────

    private static SagaTimeoutService NewTimeoutService() =>
        new(Substitute.For<IServiceScopeFactory>(), NullLogger<SagaTimeoutService>.Instance);

    [Fact]
    public async Task ARelease_ThatIsNeverConfirmed_IsResent()
    {
        var orderId = await Seed(
            SagaStatus.ReleasingInventory, sagaUpdatedAt: DateTime.UtcNow.AddMinutes(-5));

        await NewTimeoutService().CheckForTimedOutSagasPublic(
            _db, _publish, _locking, CancellationToken.None);

        var saga = await SagaOf(orderId);
        saga.Status.Should().Be(SagaStatus.ReleasingInventory);
        saga.RetryCount.Should().Be(1);
        await _publish.Received(1).Publish(
            Arg.Is<ReleaseInventoryCommand>(c => c.OrderId == orderId), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task AfterRepeatedAttempts_TheSagaGivesUp_AndTheOrderIsCancelledEverywhere()
    {
        var orderId = await Seed(
            SagaStatus.ReleasingInventory, sagaUpdatedAt: DateTime.UtcNow.AddMinutes(-5), retryCount: 3);

        await NewTimeoutService().CheckForTimedOutSagasPublic(
            _db, _publish, _locking, CancellationToken.None);

        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.Failed);
        (await OrderOf(orderId)).Status.Should().Be(OrderStatus.Cancelled);
        (await SummaryOf(orderId)).StatusName.Should().Be("Cancelled");
        await _publish.DidNotReceive().Publish(
            Arg.Any<ReleaseInventoryCommand>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ASagaThatMadeProgressRecently_IsNotTreatedAsTimedOut()
    {
        var orderId = await Seed(SagaStatus.ReleasingInventory);   // UpdatedAt = now

        await NewTimeoutService().CheckForTimedOutSagasPublic(
            _db, _publish, _locking, CancellationToken.None);

        (await SagaOf(orderId)).RetryCount.Should().Be(0);
        await AssertNothingPublished();
    }
}
