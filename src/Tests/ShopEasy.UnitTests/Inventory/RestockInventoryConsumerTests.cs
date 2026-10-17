using FluentAssertions;
using MassTransit;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using ShopEasy.InventoryService.Consumers;
using ShopEasy.InventoryService.Data;
using ShopEasy.Shared.Contracts;
using Xunit;

namespace ShopEasy.UnitTests.Inventory;

// A paid order that is cancelled gets its committed stock back, once, and the saga is told.
public class RestockInventoryConsumerTests : IDisposable
{
    private static readonly Guid ProductA = Guid.Parse("00000000-0000-0000-0000-00000000000a");
    private static readonly Guid ProductB = Guid.Parse("00000000-0000-0000-0000-00000000000b");

    private readonly InventoryTestEnvironment _env = new();

    public void Dispose() => _env.Dispose();

    private async Task<ConsumeContext<RestockInventoryCommand>> Run(Guid orderId)
    {
        var context = InventoryTestEnvironment.Context(new RestockInventoryCommand { OrderId = orderId });
        await new RestockInventoryConsumer(_env.Db, _env.Locking, _env.Cache, NullLogger<RestockInventoryConsumer>.Instance)
            .Consume(context);
        return context;
    }

    // what the ledger holds after a reservation of qty units was committed (the shelf already lost them)
    private void Committed(Guid orderId, Guid productId, int qty)
    {
        _env.AddLedger(orderId, productId, StockReasons.Reserved(orderId), -qty);
        _env.AddLedger(orderId, productId, StockReasons.Committed(orderId), -qty);
    }

    [Fact]
    public async Task PutsEveryCommittedProductBack_AndTellsTheSaga()
    {
        var orderId = Guid.NewGuid();
        var a = _env.AddItem(ProductA, quantity: 7, reserved: 0);
        var b = _env.AddItem(ProductB, quantity: 4, reserved: 0);
        Committed(orderId, ProductA, 3);
        Committed(orderId, ProductB, 1);

        var context = await Run(orderId);

        a.Quantity.Should().Be(10);
        b.Quantity.Should().Be(5);
        _env.LedgerCount(orderId, StockReasons.Restocked(orderId)).Should().Be(2);
        await context.Received(1).Publish(
            Arg.Is<InventoryRestockedEvent>(e => e.OrderId == orderId), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ARepeatedCommand_AddsNothingMore_ButConfirmsAgain()
    {
        var orderId = Guid.NewGuid();
        var item = _env.AddItem(ProductA, quantity: 7, reserved: 0);
        Committed(orderId, ProductA, 3);
        await Run(orderId);

        var again = await Run(orderId);

        item.Quantity.Should().Be(10);
        _env.LedgerCount(orderId, StockReasons.Restocked(orderId)).Should().Be(1);
        await again.Received(1).Publish(
            Arg.Is<InventoryRestockedEvent>(e => e.OrderId == orderId), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task AnOrderThatWasOnlyReserved_IsLeftAlone_AndNothingIsPublished()
    {
        var orderId = Guid.NewGuid();
        var item = _env.AddItem(ProductA, quantity: 10, reserved: 3);
        _env.AddLedger(orderId, ProductA, StockReasons.Reserved(orderId), -3);

        var context = await Run(orderId);

        item.Quantity.Should().Be(10);
        _env.LedgerCount(orderId, StockReasons.Restocked(orderId)).Should().Be(0);
        await context.DidNotReceive().Publish(Arg.Any<InventoryRestockedEvent>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task AnOrderWithoutAReservation_IsConfirmed_WithoutChangingAnything()
    {
        var context = await Run(Guid.NewGuid());

        await context.Received(1).Publish(Arg.Any<InventoryRestockedEvent>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ACommitThatArrivesAfterTheRestock_FailsInsteadOfTakingTheStockAgain()
    {
        var orderId = Guid.NewGuid();
        var item = _env.AddItem(ProductA, quantity: 7, reserved: 0);
        Committed(orderId, ProductA, 3);
        await Run(orderId);
        var commit = InventoryTestEnvironment.Context(new CommitInventoryCommand { OrderId = orderId });

        await new CommitInventoryConsumer(_env.Db, _env.Locking, _env.Cache, NullLogger<CommitInventoryConsumer>.Instance)
            .Consume(commit);

        item.Quantity.Should().Be(10);
        await commit.Received(1).Publish(
            Arg.Is<InventoryCommitFailedEvent>(e => e.OrderId == orderId), Arg.Any<CancellationToken>());
        await commit.DidNotReceive().Publish(Arg.Any<InventoryCommittedEvent>(), Arg.Any<CancellationToken>());
    }
}
