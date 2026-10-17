using FluentAssertions;
using MassTransit;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using ShopEasy.OrderService.Data;
using ShopEasy.OrderService.ReadModels;
using ShopEasy.OrderService.Sagas;
using ShopEasy.Shared.Contracts;
using Xunit;

namespace ShopEasy.UnitTests.Sagas;

// The saga completes an order only after Inventory confirms the stock commit, and every
// handler ignores (or compensates for) events that do not fit the saga's current state.
public class CommitFlowTests : IDisposable
{
    private readonly OrderDbContext _db;
    private readonly IPublishEndpoint _publish = Substitute.For<IPublishEndpoint>();
    private readonly OrderReadModelUpdater _readModel;
    private readonly ISagaLocking _locking = new InMemorySagaLocking();

    public CommitFlowTests()
    {
        var options = new DbContextOptionsBuilder<OrderDbContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString())
            .ConfigureWarnings(w => w.Ignore(InMemoryEventId.TransactionIgnoredWarning))
            .Options;

        _db = new OrderDbContext(options);
        _readModel = new OrderReadModelUpdater(_db, NullLogger<OrderReadModelUpdater>.Instance);
    }

    public void Dispose() => _db.Dispose();

    // ── helpers ──────────────────────────────────────────────────────────────

    private async Task<Guid> Seed(SagaStatus status, string? failureReason = null)
    {
        var orderId = Guid.NewGuid();
        var now = DateTime.UtcNow;

        _db.Orders.Add(new Order
        {
            Id = orderId, UserId = "user-1", UserEmail = "u@test.com",
            Status = OrderStatus.Pending, TotalAmount = 50m, CreatedAt = now, UpdatedAt = now
        });
        _db.OrderSummaries.Add(new OrderSummary
        {
            OrderId = orderId, UserId = "user-1", UserEmail = "u@test.com",
            StatusName = "Pending", StatusCode = (int)OrderStatus.Pending, TotalAmount = 50m,
            AmountDisplay = "$50.00", ItemCount = 1, ItemsSummary = "Widget x1",
            PlacedAt = now, UpdatedAt = now
        });
        _db.SagaStates.Add(new OrderSagaState
        {
            Id = Guid.NewGuid(), OrderId = orderId, Status = status,
            UserId = "user-1", UserEmail = "u@test.com", TotalAmount = 50m,
            ItemsJson = "[]", FailureReason = failureReason, CreatedAt = now, UpdatedAt = now
        });
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

    // ── InventoryCommittedConsumer ───────────────────────────────────────────

    private InventoryCommittedConsumer Committed() =>
        new(_db, _locking, _readModel, NullLogger<InventoryCommittedConsumer>.Instance);

    [Fact]
    public async Task InventoryCommitted_WhenCompleting_CompletesTheSagaAndConfirmsTheOrder()
    {
        var orderId = await Seed(SagaStatus.Completing);

        await Committed().Consume(Ctx(new InventoryCommittedEvent { OrderId = orderId }));

        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.Completed);
        (await OrderOf(orderId)).Status.Should().Be(OrderStatus.Confirmed);
        var summary = await SummaryOf(orderId);
        summary.StatusName.Should().Be("Confirmed");
        summary.PaymentStatus.Should().Be("Succeeded");
    }

    [Theory]
    [InlineData("Cancelled by customer")]
    [InlineData("Cancelled by admin")]
    public async Task InventoryCommitted_ForAnOrderCancelledMeanwhile_RefundsInsteadOfCompleting(string reason)
    {
        var orderId = await Seed(SagaStatus.Completing, failureReason: reason);
        var context = Ctx(new InventoryCommittedEvent { OrderId = orderId });

        await Committed().Consume(context);

        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.RefundingPayment);
        (await OrderOf(orderId)).Status.Should().Be(OrderStatus.Pending, "it was never confirmed");
        (await SummaryOf(orderId)).PaymentStatus.Should().Be("Refunding");
        await context.Received(1).Publish(
            Arg.Is<RefundPaymentCommand>(c => c.OrderId == orderId && c.Reason == reason), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task InventoryCommitted_WithAFailureReasonThatIsNotACancellation_StillCompletes()
    {
        var orderId = await Seed(SagaStatus.Completing, failureReason: "CommitInventory stuck after 10 retries");

        await Committed().Consume(Ctx(new InventoryCommittedEvent { OrderId = orderId }));

        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.Completed);
    }

    [Fact]
    public async Task InventoryCommitted_WhenAlreadyCompleted_IsIgnored()
    {
        var orderId = await Seed(SagaStatus.Completed);

        await Committed().Consume(Ctx(new InventoryCommittedEvent { OrderId = orderId }));

        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.Completed);
    }

    [Fact]
    public async Task InventoryCommitted_WhenTheSagaWasAlreadyCancelled_LeavesItForAHuman()
    {
        var orderId = await Seed(SagaStatus.Cancelled);

        await Committed().Consume(Ctx(new InventoryCommittedEvent { OrderId = orderId }));

        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.Cancelled);
        (await OrderOf(orderId)).Status.Should().Be(OrderStatus.Pending);
    }

    // ── InventoryCommitFailedConsumer ────────────────────────────────────────

    private InventoryCommitFailedConsumer CommitFailed() =>
        new(_db, _locking, _publish, _readModel, NullLogger<InventoryCommitFailedConsumer>.Instance);

    [Fact]
    public async Task InventoryCommitFailed_WhenCompleting_RefundsAndCancelsTheOrder()
    {
        var orderId = await Seed(SagaStatus.Completing);

        await CommitFailed().Consume(Ctx(new InventoryCommitFailedEvent
        {
            OrderId = orderId, Reason = "The reservation was already released"
        }));

        var saga = await SagaOf(orderId);
        saga.Status.Should().Be(SagaStatus.Failed);
        saga.FailureReason.Should().Contain("released");
        (await OrderOf(orderId)).Status.Should().Be(OrderStatus.Cancelled);
        (await SummaryOf(orderId)).StatusName.Should().Be("Cancelled");
        await _publish.Received(1).Publish(
            Arg.Is<RefundPaymentCommand>(c => c.OrderId == orderId), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task InventoryCommitFailed_WhenTheSagaIsAlreadyCancelling_OnlyRefunds()
    {
        var orderId = await Seed(SagaStatus.ReleasingInventory);

        await CommitFailed().Consume(Ctx(new InventoryCommitFailedEvent
        {
            OrderId = orderId, Reason = "The reservation was already released"
        }));

        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.ReleasingInventory);
        await _publish.Received(1).Publish(
            Arg.Is<RefundPaymentCommand>(c => c.OrderId == orderId), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task InventoryCommitFailed_WhenAlreadyCompleted_IsIgnored()
    {
        var orderId = await Seed(SagaStatus.Completed);

        await CommitFailed().Consume(Ctx(new InventoryCommitFailedEvent
        {
            OrderId = orderId, Reason = "stale"
        }));

        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.Completed);
        await _publish.DidNotReceive().Publish(
            Arg.Any<RefundPaymentCommand>(), Arg.Any<CancellationToken>());
    }

    // ── PaymentSucceededConsumer ─────────────────────────────────────────────

    private PaymentSucceededConsumer PaymentSucceeded() =>
        new(_db, _locking, _publish, _readModel, NullLogger<PaymentSucceededConsumer>.Instance);

    private static PaymentSucceededEvent Paid(Guid orderId) =>
        new() { OrderId = orderId, PaymentId = Guid.NewGuid(), Amount = 50m };

    [Fact]
    public async Task PaymentSucceeded_MovesToCompleting_AndAsksInventoryToCommit_ButDoesNotCompleteYet()
    {
        var orderId = await Seed(SagaStatus.ProcessingPayment);

        await PaymentSucceeded().Consume(Ctx(Paid(orderId)));

        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.Completing);
        (await OrderOf(orderId)).Status.Should().Be(OrderStatus.Pending);   // not confirmed until stock is committed
        (await SummaryOf(orderId)).PaymentStatus.Should().Be("Succeeded");
        await _publish.Received(1).Publish(
            Arg.Is<CommitInventoryCommand>(c => c.OrderId == orderId), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PaymentSucceeded_DuplicateWhileCompleting_DoesNothing()
    {
        var orderId = await Seed(SagaStatus.Completing);

        await PaymentSucceeded().Consume(Ctx(Paid(orderId)));

        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.Completing);
        await _publish.DidNotReceive().Publish(
            Arg.Any<CommitInventoryCommand>(), Arg.Any<CancellationToken>());
        await _publish.DidNotReceive().Publish(
            Arg.Any<RefundPaymentCommand>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PaymentSucceeded_DuplicateAfterCompletion_MustNotRefundTheCustomer()
    {
        var orderId = await Seed(SagaStatus.Completed);

        await PaymentSucceeded().Consume(Ctx(Paid(orderId)));

        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.Completed);
        await _publish.DidNotReceive().Publish(
            Arg.Any<RefundPaymentCommand>(), Arg.Any<CancellationToken>());
    }

    [Theory]
    [InlineData(SagaStatus.Cancelled)]
    [InlineData(SagaStatus.Failed)]
    [InlineData(SagaStatus.ReleasingInventory)]
    public async Task PaymentSucceeded_ForAnOrderTheSagaGaveUpOn_RefundsTheLatePayment(SagaStatus status)
    {
        var orderId = await Seed(status);

        await PaymentSucceeded().Consume(Ctx(Paid(orderId)));

        (await SagaOf(orderId)).Status.Should().Be(status);
        await _publish.Received(1).Publish(
            Arg.Is<RefundPaymentCommand>(c => c.OrderId == orderId), Arg.Any<CancellationToken>());
        await _publish.DidNotReceive().Publish(
            Arg.Any<CommitInventoryCommand>(), Arg.Any<CancellationToken>());
    }

    // ── PaymentFailedConsumer ────────────────────────────────────────────────

    private PaymentFailedConsumer PaymentFailed() =>
        new(_db, _locking, _publish, _readModel, NullLogger<PaymentFailedConsumer>.Instance);

    [Fact]
    public async Task PaymentFailed_WhileProcessingPayment_ReleasesTheInventory()
    {
        var orderId = await Seed(SagaStatus.ProcessingPayment);

        await PaymentFailed().Consume(Ctx(new PaymentFailedEvent
        {
            OrderId = orderId, Reason = "Payment declined by issuing bank"
        }));

        var saga = await SagaOf(orderId);
        saga.Status.Should().Be(SagaStatus.ReleasingInventory);
        saga.FailureReason.Should().Be("Payment declined by issuing bank");
        await _publish.Received(1).Publish(
            Arg.Is<ReleaseInventoryCommand>(c => c.OrderId == orderId), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PaymentFailed_InAnyOtherState_IsIgnored()
    {
        var orderId = await Seed(SagaStatus.Completing);

        await PaymentFailed().Consume(Ctx(new PaymentFailedEvent
        {
            OrderId = orderId, Reason = "stale"
        }));

        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.Completing);
        await _publish.DidNotReceive().Publish(
            Arg.Any<ReleaseInventoryCommand>(), Arg.Any<CancellationToken>());
    }

    // ── InventoryReleasedConsumer ────────────────────────────────────────────

    private InventoryReleasedConsumer Released() =>
        new(_db, _locking, _readModel, NullLogger<InventoryReleasedConsumer>.Instance);

    [Fact]
    public async Task InventoryReleased_WhileReleasing_CancelsTheOrder()
    {
        var orderId = await Seed(SagaStatus.ReleasingInventory, failureReason: "Payment declined");

        await Released().Consume(Ctx(new InventoryReleasedEvent { OrderId = orderId }));

        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.Cancelled);
        (await OrderOf(orderId)).Status.Should().Be(OrderStatus.Cancelled);
        (await SummaryOf(orderId)).StatusName.Should().Be("Cancelled");
    }

    [Fact]
    public async Task InventoryReleased_ForACompletedSaga_MustNotCancelTheOrder()
    {
        var orderId = await Seed(SagaStatus.Completed);

        await Released().Consume(Ctx(new InventoryReleasedEvent { OrderId = orderId }));

        (await SagaOf(orderId)).Status.Should().Be(SagaStatus.Completed);
        (await OrderOf(orderId)).Status.Should().Be(OrderStatus.Pending);
    }

    // ── InventoryReservedConsumer (zombie / duplicate handling) ──────────────

    private InventoryReservedConsumer Reserved() =>
        new(_db, _locking, _publish, NullLogger<InventoryReservedConsumer>.Instance);

    [Fact]
    public async Task InventoryReserved_ForACompletedSaga_MustNotReleaseTheCommittedStock()
    {
        var orderId = await Seed(SagaStatus.Completed);

        await Reserved().Consume(Ctx(new InventoryReservedEvent { OrderId = orderId }));

        await _publish.DidNotReceive().Publish(
            Arg.Any<ReleaseInventoryCommand>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task InventoryReserved_ForACancelledSaga_ReleasesTheLateReservation()
    {
        var orderId = await Seed(SagaStatus.Cancelled);

        await Reserved().Consume(Ctx(new InventoryReservedEvent { OrderId = orderId }));

        await _publish.Received(1).Publish(
            Arg.Is<ReleaseInventoryCommand>(c => c.OrderId == orderId), Arg.Any<CancellationToken>());
    }
}
