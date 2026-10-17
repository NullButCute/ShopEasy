namespace ShopEasy.EndToEndTests;

// A customer buys something, from sign-up to a confirmed order, through the gateway only. Every
// service takes part: Identity (account and session), the gateway, Product (the price), Order (the
// order and its saga), Inventory (the reservation and the commit), Payment (the charge) and
// RabbitMQ between them. In the Compose stack the payment provider is told to succeed, so a
// failing order here means the system is broken, not that a payment was declined.
[Collection(ShopCollection.Name)]
public class OrderJourneyTests(ShopFixture shop)
{
    private static readonly TimeSpan Settle = TimeSpan.FromSeconds(60);

    private Shopper Customer => shop.Customer;

    [Fact]
    public async Task AnOrder_IsPricedByTheServer_ThenReservedPaidAndConfirmed_AndTheStockGoesDown()
    {
        const int quantity = 2;
        var unitPrice = await Catalog.PriceAsync(Customer, Catalog.Webcam);
        var before = await Catalog.StockAsync(Customer, Catalog.Webcam);

        // the request claims a price of one cent: the server must use the catalog's
        var placed = await Customer.PostAsync("/api/orders", new
        {
            items = new[] { new { productId = Catalog.Webcam, quantity, unitPrice = 0.01m } },
            notes = "end-to-end",
        }, idempotencyKey: Guid.NewGuid().ToString());

        placed.Status.Should().Be(HttpStatusCode.Created, "{0}", placed);
        var orderId = placed.Json.Text("id");
        placed.Json.Number("totalAmount").Should().Be(unitPrice * quantity);
        placed.Json.Prop("items")[0].Number("unitPrice").Should().Be(unitPrice);

        var confirmed = await WaitForStatusAsync(orderId, "Confirmed");
        confirmed.Json.Number("totalAmount").Should().Be(unitPrice * quantity);
        confirmed.Json.Text("paymentStatus").Should().Be("Succeeded");

        // Inventory committed the reservation: the units left the shelf and nothing is held any more
        var after = await Wait.UntilAsync(
            () => Catalog.StockAsync(Customer, Catalog.Webcam),
            stock => stock.Quantity == before.Quantity - quantity,
            "the stock to go down by the ordered quantity", TimeSpan.FromSeconds(45));
        after.Available.Should().Be(before.Available - quantity);

        // Payment charged the total, once
        var payment = await Customer.GetAsync($"/api/payments/order/{orderId}");
        payment.Status.Should().Be(HttpStatusCode.OK, "{0}", payment);
        payment.Json.Number("amount").Should().Be(unitPrice * quantity);

        // the order is in the customer's own list and in their counts
        var list = await Customer.GetAsync("/api/orders?pageSize=50");
        list.Status.Should().Be(HttpStatusCode.OK, "{0}", list);
        list.Json.Prop("items").EnumerateArray()
            .Should().Contain(o => o.Text("orderId") == orderId && o.Text("statusName") == "Confirmed");
    }

    [Fact]
    public async Task AConfirmedOrder_CanBeCancelled_TheMoneyIsRefunded_AndTheStockIsBack()
    {
        var before = await Catalog.StockAsync(Customer, Catalog.Keyboard);
        var placed = await Customer.PostAsync("/api/orders",
            new { items = new[] { new { productId = Catalog.Keyboard, quantity = 2 } } }, Guid.NewGuid().ToString());
        placed.Status.Should().Be(HttpStatusCode.Created, "{0}", placed);
        var orderId = placed.Json.Text("id");
        await WaitForStatusAsync(orderId, "Confirmed");
        await Wait.UntilAsync(() => Catalog.StockAsync(Customer, Catalog.Keyboard),
            stock => stock.Quantity == before.Quantity - 2, "the units to leave the shelf", TimeSpan.FromSeconds(45));

        // the payment is taken and the order is complete, and it is still cancellable: the money
        // goes back, then the stock, and only then is the order Cancelled
        var cancel = await Customer.DeleteAsync($"/api/orders/{orderId}");
        cancel.Status.Should().Be(HttpStatusCode.Accepted, "{0}", cancel);

        var cancelled = await WaitForStatusAsync(orderId, "Cancelled");
        cancelled.Json.Text("paymentStatus").Should().Be("Refunded");
        var after = await Wait.UntilAsync(() => Catalog.StockAsync(Customer, Catalog.Keyboard),
            stock => stock.Quantity == before.Quantity, "the stock to be put back", TimeSpan.FromSeconds(45));
        after.Should().Be(before);

        // asking again changes nothing and says so
        var again = await Customer.DeleteAsync($"/api/orders/{orderId}");
        again.Status.Should().Be(HttpStatusCode.NoContent, "{0}", again);
        (await Catalog.StockAsync(Customer, Catalog.Keyboard)).Should().Be(before);
    }

    [Fact]
    public async Task SendingTheSameRequestAgain_ReturnsTheSameOrder_AndTheCustomerIsChargedOnce()
    {
        var key = Guid.NewGuid().ToString();
        var request = new { items = new[] { new { productId = Catalog.Keyboard, quantity = 1 } } };
        var before = await Catalog.StockAsync(Customer, Catalog.Keyboard);
        var ordersBefore = await OrderCountAsync();

        var first = await Customer.PostAsync("/api/orders", request, key);
        var orderId = first.Json.Text("id");
        first.Status.Should().Be(HttpStatusCode.Created, "{0}", first);

        // a client that lost the answer sends it again
        var repeat = await Customer.PostAsync("/api/orders", request, key);
        repeat.Status.Should().Be(HttpStatusCode.Created, "{0}", repeat);
        repeat.Json.Text("id").Should().Be(orderId);
        repeat.Header("Idempotency-Replayed").Should().Be("true");

        // the key belongs to that request: another request under it is refused
        var other = await Customer.PostAsync("/api/orders",
            new { items = new[] { new { productId = Catalog.Keyboard, quantity = 3 } } }, key);
        other.Status.Should().Be(HttpStatusCode.UnprocessableEntity, "{0}", other);

        await WaitForStatusAsync(orderId, "Confirmed");
        var after = await Wait.UntilAsync(
            () => Catalog.StockAsync(Customer, Catalog.Keyboard),
            stock => stock.Quantity < before.Quantity,
            "the stock to go down", TimeSpan.FromSeconds(45));
        after.Quantity.Should().Be(before.Quantity - 1, "one order was placed, not three");
        (await OrderCountAsync()).Should().Be(ordersBefore + 1);
    }

    [Fact]
    public async Task AnOrderThatCannotBeFilled_IsRefusedWithTheReason_AndNothingIsSavedOrHeld()
    {
        var before = await Catalog.StockAsync(Customer, Catalog.Desk);
        var ordersBefore = await OrderCountAsync();
        var tooMany = before.Available + 1;
        tooMany.Should().BeLessThanOrEqualTo(100, "100 is the most one line may ask for");

        var unknownProduct = await Customer.PostAsync("/api/orders",
            new { items = new[] { new { productId = Guid.NewGuid(), quantity = 1 } } });
        var moreThanInStock = await Customer.PostAsync("/api/orders",
            new { items = new[] { new { productId = Catalog.Desk, quantity = tooMany } } });

        ReasonsOf(unknownProduct).Should().ContainSingle().Which.Should().Contain("does not exist");
        ReasonsOf(moreThanInStock).Should().ContainSingle().Which.Should().ContainEquivalentOf("stock");

        (await OrderCountAsync()).Should().Be(ordersBefore);
        (await Catalog.StockAsync(Customer, Catalog.Desk)).Should().Be(before);
    }

    private static List<string> ReasonsOf(Reply refused)
    {
        refused.Status.Should().Be(HttpStatusCode.BadRequest, "{0}", refused);
        return refused.Json.Prop("errors").EnumerateArray().Select(e => e.GetString()!).ToList();
    }

    private async Task<int> OrderCountAsync()
    {
        var list = await Customer.GetAsync("/api/orders?pageSize=1");
        list.Status.Should().Be(HttpStatusCode.OK, "{0}", list);
        return (int)list.Json.Number("total");
    }

    // The order is finished in the background by the saga, so its status is asked for until it says
    // so; an order that was cancelled or failed instead ends the wait at once with its reason
    private async Task<Reply> WaitForStatusAsync(string orderId, string status)
    {
        var order = await Wait.UntilAsync(
            () => Customer.GetAsync($"/api/orders/{orderId}"),
            reply => reply.Status == HttpStatusCode.OK && IsFinished(reply.Json.Text("statusName"), status),
            $"order {orderId} to be {status}", Settle);
        order.Json.Text("statusName").Should().Be(status, "the order ended otherwise: {0}", order);
        return order;
    }

    private static bool IsFinished(string current, string wanted) => current == wanted || current is "Cancelled" or "Failed";
}
