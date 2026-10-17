using MassTransit;
using Microsoft.EntityFrameworkCore;
using ShopEasy.OrderService.Data;
using ShopEasy.OrderService.Metrics;
using ShopEasy.OrderService.ReadModels;
using ShopEasy.OrderService.Sagas;
using ShopEasy.Shared.Contracts;
using System.Text.Json;

namespace ShopEasy.OrderService.Services;

public enum CancelOutcome
{
    Cancelled,          // cancelled right away, nothing was reserved or charged yet
    Cancelling,         // accepted: the saga is releasing stock / the payment will be refunded
    AlreadyCancelled,   // nothing to do — repeating a cancel is safe
    NotFound,           // no such order (or it belongs to someone else)
    NotCancellable      // too late: the order has shipped
}

public record CancelResult(CancelOutcome Outcome, string? Message = null);

// Cancels an order THROUGH the saga, so stock and money are handled by the same state
// machine that created them — never by just flipping the order's status.
//
// The saga row is locked first, so a cancel cannot interleave with the orchestrator, a
// payment result or the timeout service. What happens depends on how far the saga got:
//
//   no saga yet (outbox event in flight)  → record a Cancelled saga so the starter skips it → Cancelled
//   Started                               → nothing reserved or charged yet               → Cancelled
//   ReservingInventory / ProcessingPayment→ saga moves to ReleasingInventory and a
//                                           ReleaseInventoryCommand is sent. A payment that still
//                                           succeeds is refunded by the late-payment guard    → Cancelling
//   ReleasingInventory                    → compensation already running                      → Cancelling
//   Cancelled / Failed                    → already over                                      → AlreadyCancelled
//   Completed (paid, committed)           → saga moves to RefundingPayment and a RefundPaymentCommand
//                                           is sent; the refund, then the restock, then Cancelled → Cancelling
//   RefundingPayment / RestockingInventory→ that cancellation is already running                → Cancelling
//   Completing                            → payment taken, commit in flight: the request is kept in
//                                           FailureReason; InventoryCommittedConsumer then refunds → Cancelling
//   a shipped or delivered order          → NotCancellable
public class OrderCancellationService(
    OrderDbContext db,
    ISagaLocking sagaLocking,
    IPublishEndpoint publishEndpoint,
    OrderReadModelUpdater readModelUpdater,
    ILogger<OrderCancellationService> logger)
{
    private const int MaxAttempts = 3;

    public async Task<CancelResult> CancelAsync(
        Guid orderId, string requesterId, bool isAdmin, CancellationToken ct = default)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                var (result, command) =
                    await TryCancelAsync(orderId, requesterId, isAdmin, ct);

                if (command is not null)
                    await PublishFollowUpAsync(command, ct);

                return result;
            }
            catch (DbUpdateException ex) when (attempt < MaxAttempts)
            {
                // The unique index on SagaStates.OrderId rejected our insert: the saga starter
                // created the saga at the same moment. Start over — the saga exists now.
                logger.LogWarning(ex,
                    "Concurrent saga creation while cancelling Order {OrderId} (attempt {Attempt}) — retrying",
                    orderId, attempt);
                db.ChangeTracker.Clear();
            }
        }
    }

    // The result, and the command that carries the cancellation on (a release or a refund), if any
    private async Task<(CancelResult Result, object? Command)> TryCancelAsync(
        Guid orderId, string requesterId, bool isAdmin, CancellationToken ct)
    {
        await using var transaction = await db.Database.BeginTransactionAsync(ct);

        var saga = await sagaLocking.LockByOrderIdAsync(db, orderId, ct);
        var order = await db.Orders
            .Include(o => o.Items)
            .FirstOrDefaultAsync(o => o.Id == orderId, ct);

        // Other customers' orders look like they do not exist
        if (order is null || (!isAdmin && order.UserId != requesterId))
            return (new CancelResult(CancelOutcome.NotFound), null);

        var reason = order.UserId == requesterId ? FailureReasons.CancelledByCustomer : FailureReasons.CancelledByAdmin;
        var now = DateTime.UtcNow;

        if (order.Status == OrderStatus.Cancelled)
            return (new CancelResult(CancelOutcome.AlreadyCancelled), null);

        if (order.Status is OrderStatus.Shipped or OrderStatus.Delivered)
            return (NotCancellable("The order has already shipped and can no longer be cancelled."), null);

        // ── The saga has not been created yet (the outbox event is still in flight) ──
        if (saga is null)
        {
            // Record a Cancelled saga: OrderConfirmedSagaStarter skips orders that already
            // have one, so this order will never reserve stock or take a payment.
            db.SagaStates.Add(new OrderSagaState
            {
                Id = Guid.NewGuid(),
                OrderId = order.Id,
                Status = SagaStatus.Cancelled,
                UserId = order.UserId,
                UserEmail = order.UserEmail,
                TotalAmount = order.TotalAmount,
                ItemsJson = JsonSerializer.Serialize(order.Items.Select(i => new OrderLineItem
                {
                    ProductId = i.ProductId,
                    ProductName = i.ProductName,
                    Quantity = i.Quantity,
                    UnitPrice = i.UnitPrice
                }).ToList()),
                FailureReason = reason,
                CreatedAt = now,
                UpdatedAt = now
            });

            await MarkCancelledAsync(order, reason, now, ct);
            await transaction.CommitAsync(ct);

            OrderMetrics.RecordOrderCancelled(FailureReasons.CancelReasonOf(reason));
            logger.LogInformation(
                "Order {OrderId} cancelled before its saga started ({Reason})", orderId, reason);
            return (new CancelResult(CancelOutcome.Cancelled), null);
        }

        switch (saga.Status)
        {
            case SagaStatus.Completed:
                // Paid and committed: give the money back, then the stock (PaymentRefundedConsumer
                // and InventoryRestockedConsumer take the next steps); the order is Cancelled at the end
                saga.Status = SagaStatus.RefundingPayment;
                saga.FailureReason = reason;
                saga.UpdatedAt = now;
                await db.SaveChangesAsync(ct);

                await readModelUpdater.UpdateStatusAsync(
                    order.Id, order.Status, paymentStatus: "Refunding", failureReason: reason);
                await transaction.CommitAsync(ct);

                logger.LogInformation(
                    "Order {OrderId} cancellation accepted after payment — refunding ({Reason})", orderId, reason);
                return (new CancelResult(CancelOutcome.Cancelling,
                    "Cancellation accepted. The payment is being refunded and the stock put back."),
                    new RefundPaymentCommand { OrderId = order.Id, Reason = reason });

            case SagaStatus.Completing:
                // Payment taken, stock commit in flight. Keep the request; when Inventory confirms the
                // commit, InventoryCommittedConsumer refunds instead of completing (a failed commit
                // already refunds and cancels). UpdatedAt stays, so the timeout keeps its own clock.
                saga.FailureReason = reason;
                await db.SaveChangesAsync(ct);
                await readModelUpdater.UpdateStatusAsync(
                    order.Id, order.Status, failureReason: reason);
                await transaction.CommitAsync(ct);

                logger.LogInformation(
                    "Order {OrderId} cancellation accepted while its stock commit is in flight ({Reason})",
                    orderId, reason);
                return (new CancelResult(CancelOutcome.Cancelling,
                    "Cancellation accepted. The order is being finalized and will then be refunded."), null);

            case SagaStatus.Cancelled:
            case SagaStatus.Failed:
                // The saga is already over; make sure the order says so, then report success
                await MarkCancelledAsync(order, saga.FailureReason ?? reason, now, ct);
                await transaction.CommitAsync(ct);
                return (new CancelResult(CancelOutcome.AlreadyCancelled), null);

            case SagaStatus.ReleasingInventory:
            case SagaStatus.RefundingPayment:
            case SagaStatus.RestockingInventory:
                return (new CancelResult(CancelOutcome.Cancelling,
                    "The order is already being cancelled."), null);

            case SagaStatus.Started:
                // Nothing has been reserved or charged yet: cancel on the spot
                saga.Status = SagaStatus.Cancelled;
                saga.FailureReason = reason;
                saga.UpdatedAt = now;
                await MarkCancelledAsync(order, reason, now, ct);
                await transaction.CommitAsync(ct);

                OrderMetrics.RecordSagaCancelled(now - saga.CreatedAt);
                OrderMetrics.RecordOrderCancelled(FailureReasons.CancelReasonOf(reason));
                logger.LogInformation(
                    "Order {OrderId} cancelled before any stock was reserved ({Reason})", orderId, reason);
                return (new CancelResult(CancelOutcome.Cancelled), null);

            case SagaStatus.ReservingInventory:
            case SagaStatus.ProcessingPayment:
                // Stock is (or is about to be) reserved and a payment may be in flight. Hand the
                // order to the saga's compensation path: release the stock, and refund a payment
                // that still succeeds (PaymentSucceededConsumer refunds payments that arrive for
                // a ReleasingInventory saga). InventoryReleasedConsumer then cancels the order.
                var previousStatus = saga.Status;
                saga.Status = SagaStatus.ReleasingInventory;
                saga.FailureReason = reason;
                saga.UpdatedAt = now;
                await db.SaveChangesAsync(ct);

                await readModelUpdater.UpdateStatusAsync(
                    order.Id, OrderStatus.Pending, failureReason: reason);
                await transaction.CommitAsync(ct);

                logger.LogInformation(
                    "Order {OrderId} cancellation accepted while saga was {Status} — releasing stock ({Reason})",
                    orderId, previousStatus, reason);
                return (new CancelResult(CancelOutcome.Cancelling,
                    "Cancellation accepted. Reserved stock is being released and any payment will be refunded."),
                    new ReleaseInventoryCommand { OrderId = order.Id });

            default:
                return (NotCancellable($"The order cannot be cancelled in its current state ({saga.Status})."), null);
        }
    }

    private async Task MarkCancelledAsync(
        Order order, string reason, DateTime now, CancellationToken ct)
    {
        order.Status = OrderStatus.Cancelled;
        order.UpdatedAt = now;
        await db.SaveChangesAsync(ct);

        await readModelUpdater.UpdateStatusAsync(
            order.Id, OrderStatus.Cancelled, failureReason: reason);
    }

    private async Task PublishFollowUpAsync(object command, CancellationToken ct)
    {
        try
        {
            switch (command)
            {
                case ReleaseInventoryCommand release: await publishEndpoint.Publish(release, ct); break;
                case RefundPaymentCommand refund: await publishEndpoint.Publish(refund, ct); break;
            }
        }
        catch (Exception ex)
        {
            // The cancellation is already saved. If the command was lost, SagaTimeoutService
            // re-sends it (release and refund are idempotent), so the caller still gets "accepted".
            logger.LogError(ex,
                "Could not publish {Command}; the saga timeout service will retry", command.GetType().Name);
        }
    }

    private static CancelResult NotCancellable(string message) =>
        new(CancelOutcome.NotCancellable, message);
}
