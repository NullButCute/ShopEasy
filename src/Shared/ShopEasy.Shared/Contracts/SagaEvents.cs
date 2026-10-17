namespace ShopEasy.Shared.Contracts;

// InventoryService → Saga: stock successfully reserved
public record InventoryReservedEvent
{
    public Guid OrderId { get; init; }
}

// InventoryService → Saga: not enough stock
public record InventoryReservationFailedEvent
{
    public Guid OrderId { get; init; }
    public string Reason { get; init; } = default!;
}

// InventoryService → Saga: reservation released (compensating action done)
public record InventoryReleasedEvent
{
    public Guid OrderId { get; init; }
}

// InventoryService → Saga: the reservation was committed (stock actually deducted)
public record InventoryCommittedEvent
{
    public Guid OrderId { get; init; }
}

// InventoryService → Saga: the stock of a cancelled, paid order is back in the shelf counts.
// Also sent again when the command is repeated.
public record InventoryRestockedEvent
{
    public Guid OrderId { get; init; }
}

// InventoryService → Saga: the reservation can never be committed
// (it does not exist or was already released). Payment has been taken, so the saga
// must refund and cancel. Transient errors are NOT reported this way — they are retried.
public record InventoryCommitFailedEvent
{
    public Guid OrderId { get; init; }
    public string Reason { get; init; } = default!;
}

// PaymentService → Saga: payment succeeded
public record PaymentSucceededEvent
{
    public Guid OrderId { get; init; }
    public Guid PaymentId { get; init; }
    public decimal Amount { get; init; }
}

// PaymentService → Saga: the payment was given back to the customer. Also sent again when a
// refund command is repeated, which is how the saga asks for a lost event a second time.
public record PaymentRefundedEvent
{
    public Guid OrderId { get; init; }
    public Guid PaymentId { get; init; }
    public decimal Amount { get; init; }
}

// PaymentService → Saga: payment failed
public record PaymentFailedEvent
{
    public Guid OrderId { get; init; }
    public string Reason { get; init; } = default!;
}