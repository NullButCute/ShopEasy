using ShopEasy.OrderService.Metrics;

namespace ShopEasy.OrderService.Sagas;

// The failure reasons the Order service writes itself (the others arrive from Inventory and Payment
// as free text). Constants, because the cancellation metric tells why an order was cancelled by them.
public static class FailureReasons
{
    public const string CancelledByCustomer = "Cancelled by customer";
    public const string CancelledByAdmin = "Cancelled by admin";
    public const string TimedOutWaitingForPayment = "Timed out waiting for payment";

    // Somebody asked for the order to be cancelled (as opposed to the system giving up on it)
    public static bool IsCancelRequest(string? reason) => reason is CancelledByCustomer or CancelledByAdmin;

    // Why an order whose stock was released was cancelled: somebody cancelled it, the payment did
    // not arrive in time, or else the payment was declined
    public static string CancelReasonOf(string? reason) => reason switch
    {
        _ when reason?.StartsWith(CancelledByCustomer, StringComparison.Ordinal) == true => CancelReason.Customer,
        _ when reason?.StartsWith(CancelledByAdmin, StringComparison.Ordinal) == true => CancelReason.Admin,
        _ when reason?.StartsWith(TimedOutWaitingForPayment, StringComparison.Ordinal) == true => CancelReason.Timeout,
        _ => CancelReason.PaymentDeclined,
    };
}
