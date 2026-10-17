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
using ShopEasy.UnitTests.Support;
using Xunit;

namespace ShopEasy.UnitTests.Sagas;

// The steps of cancelling an order that was already paid, as the saga takes them.
[Collection(MetricsCollection.Name)]
public class PaidCancellationTests : IDisposable
{
    private readonly OrderDbContext _db = new(new DbContextOptionsBuilder<OrderDbContext>()
        .UseInMemoryDatabase(Guid.NewGuid().ToString())
        .ConfigureWarnings(w => w.Ignore(InMemoryEventId.TransactionIgnoredWarning)).Options);
    private readonly IPublishEndpoint _publish = Substitute.For<IPublishEndpoint>();

    public void Dispose() => _db.Dispose();

    private async Task<Guid> Seed(SagaStatus saga, OrderStatus order = OrderStatus.Confirmed, int retryCount = 0)
    {
        var id = Guid.NewGuid();
        var now = DateTime.UtcNow;
        _db.Orders.Add(new Order { Id = id, UserId = "u", UserEmail = "u@test.com", Status = order, TotalAmount = 20m, CreatedAt = now, UpdatedAt = now });
        _db.OrderSummaries.Add(new OrderSummary
        {
            OrderId = id, UserId = "u", UserEmail = "u@test.com", StatusName = order.ToString(), StatusCode = (int)order,
            TotalAmount = 20m, AmountDisplay = "$20.00", ItemsSummary = "Widget x1", PaymentStatus = "Succeeded", PlacedAt = now, UpdatedAt = now
        });
        _db.SagaStates.Add(new OrderSagaState
        {
            Id = Guid.NewGuid(), OrderId = id, Status = saga, UserId = "u", UserEmail = "u@test.com", TotalAmount = 20m,
            ItemsJson = "[]", FailureReason = "Cancelled by customer", RetryCount = retryCount, CreatedAt = now, UpdatedAt = now
        });
        await _db.SaveChangesAsync();
        return id;
    }

    private async Task PaymentRefunded(Guid orderId)
    {
        var context = Substitute.For<ConsumeContext<PaymentRefundedEvent>>();
        context.Message.Returns(new PaymentRefundedEvent { OrderId = orderId, PaymentId = Guid.NewGuid(), Amount = 20m });
        context.CancellationToken.Returns(CancellationToken.None);
        await new PaymentRefundedConsumer(_db, new InMemorySagaLocking(), _publish,
            new OrderReadModelUpdater(_db, NullLogger<OrderReadModelUpdater>.Instance),
            NullLogger<PaymentRefundedConsumer>.Instance).Consume(context);
    }

    [Fact]
    public async Task ARefundedPayment_MovesTheSagaOn_AndAsksInventoryToPutTheStockBack()
    {
        var orderId = await Seed(SagaStatus.RefundingPayment, retryCount: 4);

        await PaymentRefunded(orderId);

        var saga = await _db.SagaStates.SingleAsync(s => s.OrderId == orderId);
        (saga.Status, saga.RetryCount).Should().Be((SagaStatus.RestockingInventory, 0));
        (await _db.OrderSummaries.SingleAsync(s => s.OrderId == orderId)).PaymentStatus.Should().Be("Refunded");
        await _publish.Received(1).Publish(
            Arg.Is<RestockInventoryCommand>(c => c.OrderId == orderId), Arg.Any<CancellationToken>());
    }

    [Theory]
    [InlineData(SagaStatus.Completed)]
    [InlineData(SagaStatus.ReleasingInventory)]
    [InlineData(SagaStatus.RestockingInventory)] // a duplicate of the event
    [InlineData(SagaStatus.Cancelled)]
    [InlineData(SagaStatus.Failed)]
    public async Task ARefundThatTheSagaIsNotWaitingFor_ChangesNothing(SagaStatus status)
    {
        var orderId = await Seed(status);

        await PaymentRefunded(orderId);

        (await _db.SagaStates.SingleAsync(s => s.OrderId == orderId)).Status.Should().Be(status);
        (await _db.OrderSummaries.SingleAsync(s => s.OrderId == orderId)).PaymentStatus.Should().Be("Succeeded");
        await _publish.DidNotReceive().Publish(Arg.Any<RestockInventoryCommand>(), Arg.Any<CancellationToken>());
    }

    private async Task StockRestored(Guid orderId)
    {
        var context = Substitute.For<ConsumeContext<InventoryRestockedEvent>>();
        context.Message.Returns(new InventoryRestockedEvent { OrderId = orderId });
        await new InventoryRestockedConsumer(_db, new InMemorySagaLocking(),
            new OrderReadModelUpdater(_db, NullLogger<OrderReadModelUpdater>.Instance),
            NullLogger<InventoryRestockedConsumer>.Instance).Consume(context);
    }

    private static Task<double> CancelledByCustomer() =>
        MetricValues.ReadAsync("shopeasy_order_outcomes_total{outcome=\"cancelled\",reason=\"customer\"}");

    [Fact]
    public async Task WhenTheStockIsBack_TheOrderIsCancelled_AndRefunded_AndCountedOnce()
    {
        var orderId = await Seed(SagaStatus.RestockingInventory);
        var before = await CancelledByCustomer();

        await StockRestored(orderId);
        await StockRestored(orderId); // a repeat of the event

        (await _db.SagaStates.SingleAsync(s => s.OrderId == orderId)).Status.Should().Be(SagaStatus.Cancelled);
        (await _db.Orders.SingleAsync(o => o.Id == orderId)).Status.Should().Be(OrderStatus.Cancelled);
        var summary = await _db.OrderSummaries.SingleAsync(s => s.OrderId == orderId);
        (summary.StatusName, summary.PaymentStatus, summary.FailureReason).Should().Be(("Cancelled", "Refunded", "Cancelled by customer"));
        (await CancelledByCustomer()).Should().Be(before + 1);
    }

    [Theory]
    [InlineData(SagaStatus.Completed)]
    [InlineData(SagaStatus.RefundingPayment)] // the stock cannot be back before the money is
    [InlineData(SagaStatus.ReleasingInventory)]
    [InlineData(SagaStatus.Cancelled)]
    public async Task StockThatTheSagaWasNotWaitingFor_ChangesNothing(SagaStatus status)
    {
        var orderId = await Seed(status);

        await StockRestored(orderId);

        (await _db.SagaStates.SingleAsync(s => s.OrderId == orderId)).Status.Should().Be(status);
        (await _db.Orders.SingleAsync(o => o.Id == orderId)).Status.Should().Be(OrderStatus.Confirmed);
    }

    [Fact]
    public async Task ARefundForAnOrderWithoutASaga_IsIgnored()
    {
        await PaymentRefunded(Guid.NewGuid());

        await _publish.DidNotReceive().Publish(Arg.Any<RestockInventoryCommand>(), Arg.Any<CancellationToken>());
    }
}
