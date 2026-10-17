namespace ShopEasy.Shared.Contracts;

// Saga → InventoryService: reserve stock (don't reduce yet, just lock it)
public record ReserveInventoryCommand
{
    public Guid OrderId { get; init; }
    public List<OrderLineItem> Items { get; init; } = new();
}

// Saga → InventoryService: commit the reservation (actually reduce stock)
public record CommitInventoryCommand
{
    public Guid OrderId { get; init; }
}

// Saga → InventoryService: release the reservation (put stock back)
public record ReleaseInventoryCommand
{
    public Guid OrderId { get; init; }
}

// Saga → InventoryService: put the stock of a committed order back (the paid order was cancelled)
public record RestockInventoryCommand
{
    public Guid OrderId { get; init; }
}

// Saga → PaymentService: process payment
public record ProcessPaymentCommand
{
    public Guid OrderId { get; init; }
    public string UserId { get; init; } = default!;
    public string UserEmail { get; init; } = default!;
    public string Currency { get; init; } = default!;
    public decimal Amount { get; init; }
}

public record RefundPaymentCommand
{
    public Guid OrderId { get; init; }
    public string Reason { get; init; } = string.Empty;
}