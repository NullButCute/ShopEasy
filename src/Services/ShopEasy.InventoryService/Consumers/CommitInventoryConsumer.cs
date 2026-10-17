using MassTransit;
using Microsoft.EntityFrameworkCore;
using ShopEasy.InventoryService.Data;
using ShopEasy.Shared.Contracts;
using ShopEasy.Shared.Services;

namespace ShopEasy.InventoryService.Consumers;

// Turns a reservation into a real stock deduction once payment succeeded, and reports the
// result back to the saga (InventoryCommittedEvent / InventoryCommitFailedEvent).
//
// Idempotent: the order's ledger rows (checked under the row locks) decide what to do, so a
// duplicate or retried command never deducts stock twice — it just re-sends the result.
//   already committed            → success again, no change
//   stock put back (cancelled)   → InventoryCommitFailedEvent
//   reservation was released     → InventoryCommitFailedEvent (the saga refunds the payment)
//   no reservation at all        → InventoryCommitFailedEvent
// Transient errors (database down, ...) throw, so MassTransit retries the message.
public class CommitInventoryConsumer(
    InventoryDbContext db,
    IInventoryLocking locking,
    CacheService cache,
    ILogger<CommitInventoryConsumer> logger) : IConsumer<CommitInventoryCommand>
{
    public async Task Consume(ConsumeContext<CommitInventoryCommand> context)
    {
        var orderId = context.Message.OrderId;
        var ct = context.CancellationToken;
        logger.LogInformation("Committing inventory for Order {OrderId}", orderId);

        string? failureReason = null;
        var committed = false;
        var affectedProductIds = new List<Guid>();

        await using var transaction = await db.Database.BeginOrJoinAsync(ct);
        try
        {
            // 1. Which products did this order reserve? (unlocked read — only decides what to lock)
            var reservedReason = StockReasons.Reserved(orderId);
            var productIds = await db.StockTransactions.AsNoTracking()
                .Where(t => t.OrderId == orderId && t.Reason == reservedReason)
                .Select(t => t.ProductId)
                .Distinct()
                .ToListAsync(ct);

            if (productIds.Count == 0)
            {
                failureReason = "No reservation found for this order";
            }
            else
            {
                // 2. Lock the rows, then read the order's ledger state under the lock
                var items = await locking.LockItemsAsync(db, productIds, ct);
                var rows = await db.StockTransactions.AsNoTracking()
                    .Where(t => t.OrderId == orderId)
                    .ToListAsync(ct);
                var state = OrderStockState.From(orderId, rows);

                if (state.AnyRestocked)
                {
                    failureReason = "The order was cancelled and its stock put back";
                }
                else if (state.FullyCommitted)
                {
                    logger.LogWarning(
                        "Order {OrderId} was already committed — re-confirming", orderId);
                }
                else if (state.AnyReleased)
                {
                    failureReason = "The reservation was already released";
                }
                else
                {
                    var now = DateTime.UtcNow;
                    var toCommit = state.ReservedQuantity
                        .Where(r => !state.Committed.Contains(r.Key))
                        .ToList();

                    if (toCommit.Any(r => items.All(i => i.ProductId != r.Key)))
                    {
                        failureReason = "Inventory record missing for a reserved product";
                    }
                    else
                    {
                        foreach (var (productId, qty) in toCommit)
                        {
                            var item = items.First(i => i.ProductId == productId);

                            item.Quantity = Math.Max(0, item.Quantity - qty);
                            item.ReservedQty = Math.Max(0, item.ReservedQty - qty);
                            item.UpdatedAt = now;

                            db.StockTransactions.Add(new StockTransaction
                            {
                                Id = Guid.NewGuid(),
                                ProductId = productId,
                                QuantityChange = -qty,
                                Reason = StockReasons.Committed(orderId),
                                OrderId = orderId,
                                CreatedAt = now
                            });

                            affectedProductIds.Add(productId);

                            logger.LogInformation(
                                "Committed {Qty} units for Product {ProductId}. New quantity: {New}",
                                qty, productId, item.Quantity);
                        }

                        await db.SaveChangesAsync(ct);
                        await transaction.CommitAsync(ct);
                        committed = true;
                    }
                }
            }

            // Nothing was changed — release the row locks
            if (!committed)
                await transaction.RollbackAsync(ct);
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync(CancellationToken.None);
            logger.LogError(ex, "Failed to commit inventory for Order {OrderId}", orderId);
            throw;
        }

        if (failureReason is not null)
        {
            logger.LogError(
                "Cannot commit inventory for Order {OrderId}: {Reason}", orderId, failureReason);
            await context.Publish(new InventoryCommitFailedEvent
            {
                OrderId = orderId,
                Reason = failureReason
            });
            return;
        }

        foreach (var productId in affectedProductIds)
            await cache.InvalidateAsync(CacheKeys.Inventory(productId));
        if (committed)
            await cache.InvalidateAsync(CacheKeys.InventoryAll);

        logger.LogInformation("Inventory committed for Order {OrderId}", orderId);
        await context.Publish(new InventoryCommittedEvent { OrderId = orderId });
    }
}
