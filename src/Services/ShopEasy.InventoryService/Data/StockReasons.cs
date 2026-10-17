namespace ShopEasy.InventoryService.Data;

// The Reason text on a StockTransaction doubles as the "kind" of ledger entry
// (reserved / committed / released / restocked) for an order, so it must always be built here
// and compared for exact equality — never assembled ad hoc.
public static class StockReasons
{
    public static string Reserved(Guid orderId) => $"Reserved for Order {orderId}";
    public static string Committed(Guid orderId) => $"Committed for Order {orderId}";
    public static string Released(Guid orderId) => $"Released reservation for Order {orderId}";
    public static string Restocked(Guid orderId) => $"Restocked after cancelled Order {orderId}";
}

// What the ledger says has happened to one order's stock so far.
// Derived from the order's StockTransaction rows; used to make commit and release idempotent.
public sealed class OrderStockState
{
    // Quantity reserved per product (rows written by ReserveInventoryConsumer)
    public Dictionary<Guid, int> ReservedQuantity { get; } = new();
    public HashSet<Guid> Committed { get; } = new();
    public HashSet<Guid> Released { get; } = new();
    public HashSet<Guid> Restocked { get; } = new();

    public bool HasReservation => ReservedQuantity.Count > 0;
    public bool AnyCommitted => Committed.Count > 0;
    public bool AnyReleased => Released.Count > 0;
    public bool AnyRestocked => Restocked.Count > 0;
    public bool FullyCommitted => HasReservation && ReservedQuantity.Keys.All(Committed.Contains);
    public bool FullyReleased => HasReservation && ReservedQuantity.Keys.All(Released.Contains);
    public bool FullyRestocked => HasReservation && ReservedQuantity.Keys.All(Restocked.Contains);

    public static OrderStockState From(Guid orderId, IEnumerable<StockTransaction> rows)
    {
        var reserved = StockReasons.Reserved(orderId);
        var committed = StockReasons.Committed(orderId);
        var released = StockReasons.Released(orderId);
        var restocked = StockReasons.Restocked(orderId);

        var state = new OrderStockState();
        foreach (var row in rows)
        {
            if (row.OrderId != orderId) continue;

            if (row.Reason == reserved)
            {
                state.ReservedQuantity[row.ProductId] =
                    state.ReservedQuantity.GetValueOrDefault(row.ProductId) + Math.Abs(row.QuantityChange);
            }
            else if (row.Reason == committed)
            {
                state.Committed.Add(row.ProductId);
            }
            else if (row.Reason == released)
            {
                state.Released.Add(row.ProductId);
            }
            else if (row.Reason == restocked)
            {
                state.Restocked.Add(row.ProductId);
            }
        }
        return state;
    }
}
