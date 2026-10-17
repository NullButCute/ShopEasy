using MassTransit;
using ShopEasy.OrderService.Data;
using ShopEasy.OrderService.Metrics;
using ShopEasy.OrderService.ReadModels;
using ShopEasy.Shared.Contracts;

namespace ShopEasy.OrderService.Sagas;

// Cancelling an order that was already paid goes RefundingPayment (the refund is out) →
// RestockingInventory (the money is back, the stock is being put back) → Cancelled.
// Each consumer moves the saga only on the answer it is waiting for. Any other answer (a
// duplicate, a refund that belongs to another path) leaves the saga alone.

// ── Payment refunded → put the stock back ─────────────────────────────────────
public class PaymentRefundedConsumer(
    OrderDbContext db,
    ISagaLocking sagaLocking,
    IPublishEndpoint publishEndpoint,
    OrderReadModelUpdater readModelUpdater,
    ILogger<PaymentRefundedConsumer> logger)
    : IConsumer<PaymentRefundedEvent>
{
    public async Task Consume(ConsumeContext<PaymentRefundedEvent> context)
    {
        var evt = context.Message;

        await using (var transaction = await db.Database.BeginTransactionAsync())
        {
            var saga = await sagaLocking.LockByOrderIdAsync(db, evt.OrderId);
            if (saga is null) return;

            if (saga.Status != SagaStatus.RefundingPayment)
            {
                logger.LogInformation(
                    "Saga {SagaId}: ignoring PaymentRefundedEvent in state {Status}", saga.Id, saga.Status);
                return;
            }

            logger.LogInformation(
                "Saga {SagaId}: payment refunded → putting the stock of Order {OrderId} back",
                saga.Id, saga.OrderId);

            saga.Status = SagaStatus.RestockingInventory;
            saga.RetryCount = 0;
            saga.UpdatedAt = DateTime.UtcNow;
            await db.SaveChangesAsync();

            var order = await db.Orders.FindAsync(saga.OrderId);
            await readModelUpdater.UpdateStatusAsync(
                saga.OrderId, order?.Status ?? OrderStatus.Confirmed, paymentStatus: "Refunded");
            await transaction.CommitAsync();
        }

        // If this publish is lost, SagaTimeoutService re-sends it (restock is idempotent)
        await publishEndpoint.Publish(
            new RestockInventoryCommand { OrderId = evt.OrderId }, context.CancellationToken);
    }
}

// ── Stock put back → the order is cancelled ───────────────────────────────────
public class InventoryRestockedConsumer(
    OrderDbContext db,
    ISagaLocking sagaLocking,
    OrderReadModelUpdater readModelUpdater,
    ILogger<InventoryRestockedConsumer> logger)
    : IConsumer<InventoryRestockedEvent>
{
    public async Task Consume(ConsumeContext<InventoryRestockedEvent> context)
    {
        var evt = context.Message;

        await using var transaction = await db.Database.BeginTransactionAsync();

        var saga = await sagaLocking.LockByOrderIdAsync(db, evt.OrderId);
        if (saga is null) return;

        if (saga.Status != SagaStatus.RestockingInventory)
        {
            logger.LogInformation(
                "Saga {SagaId}: ignoring InventoryRestockedEvent in state {Status}", saga.Id, saga.Status);
            return;
        }

        var order = await db.Orders.FindAsync(saga.OrderId);
        if (order is not null)
        {
            order.Status = OrderStatus.Cancelled;
            order.UpdatedAt = DateTime.UtcNow;
        }

        saga.Status = SagaStatus.Cancelled;
        saga.UpdatedAt = DateTime.UtcNow;
        await db.SaveChangesAsync();

        await readModelUpdater.UpdateStatusAsync(
            saga.OrderId, OrderStatus.Cancelled, paymentStatus: "Refunded", failureReason: saga.FailureReason);
        await transaction.CommitAsync();

        OrderMetrics.RecordSagaCancelled(DateTime.UtcNow - saga.CreatedAt);
        OrderMetrics.RecordOrderCancelled(FailureReasons.CancelReasonOf(saga.FailureReason));

        logger.LogInformation(
            "Saga {SagaId}: Order {OrderId} cancelled, refunded and restocked", saga.Id, saga.OrderId);
    }
}
