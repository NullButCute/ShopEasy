using MassTransit;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using ShopEasy.OrderService.Data;
using ShopEasy.OrderService.Idempotency;
using ShopEasy.OrderService.Metrics;
using ShopEasy.OrderService.ReadModels;
using ShopEasy.OrderService.Services;
using ShopEasy.Shared.Contracts;
using ShopEasy.Shared.Messaging;
using System.Text.Json;
using ShopEasy.Shared.Security;

namespace ShopEasy.OrderService.Controllers;

// ── Command side (CQRS) ───────────────────────────────────────────────────
// Write-only endpoints that modify state.
// Publishes events that update the read model asynchronously.
[ApiController]
[Route("api/orders")]
[Authorize(Policy = PolicyNames.OrdersWrite)]
[Produces("application/json")]

public class OrderCommandsController(
    OrderDbContext db,
    ILogger<OrderCommandsController> logger, IInventoryStock inventoryCache, OrderReadModelUpdater readModelUpdater,
    IProductCatalogClient productCatalog,
    OrderCancellationService cancellation,
    IdempotencyStore idempotency,
    IOptions<IdempotencyOptions> idempotencyOptions,
    IOptions<Microsoft.AspNetCore.Mvc.JsonOptions> jsonOptions) : ControllerBase
{
    private readonly OrderDbContext _db = db;
    private readonly ILogger<OrderCommandsController> _logger = logger;
    private readonly IInventoryStock _inventoryCache = inventoryCache;
    private readonly IProductCatalogClient _productCatalog = productCatalog;

    // ── POST /api/orders ──────────────────────────────────────────────────────
    // The client only chooses WHAT to buy (productId + quantity). Product names and
    // prices are always taken from the Product service — never from the request body.
    //
    // Idempotency-Key (optional unless Idempotency:RequireKey is set): a client that did not see
    // the answer repeats the request with the same key and gets the original response back
    // (with an Idempotency-Replayed header) instead of a second order. The same key with a
    // different request is rejected with 422. Keys are remembered for Idempotency:RetentionHours.
    // Only a request that actually placed an order uses up its key: one rejected with 400 or 503
    // can be repeated with the same key.
    [HttpPost]
    [EnableRateLimiting("create-order")]
    [ProducesResponseType(StatusCodes.Status201Created)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status422UnprocessableEntity)]
    [ProducesResponseType(StatusCodes.Status503ServiceUnavailable)]
    public async Task<IActionResult> PlaceOrder(
        [FromBody] PlaceOrderRequest request,
        [FromHeader(Name = IdempotencyKey.HeaderName)] string? idempotencyKey = null)
    {
        var userId = User.FindFirst("sub")?.Value!;
        var userEmail = User.FindFirst("email")?.Value ?? "";

        if (!IdempotencyKey.TryParse(
                idempotencyKey, idempotencyOptions.Value.RequireKey, out var key, out var keyError))
        {
            OrderMetrics.RecordOrderRejected(OrderRejection.InvalidRequest);
            return BadRequest(new
            {
                message = "Order could not be placed.",
                errors = new[] { keyError! }
            });
        }

        // ── Step 0: Validate the request itself (shape, limits, duplicates) ───
        var validation = OrderRequestValidator.Validate(request);
        if (!validation.IsValid)
        {
            _logger.LogWarning(
                "Order rejected for User {UserId}: invalid request — {Errors}",
                userId, string.Join("; ", validation.Errors));
            OrderMetrics.RecordOrderRejected(OrderRejection.InvalidRequest);
            return BadRequest(new
            {
                message = "Order could not be placed.",
                errors = validation.Errors
            });
        }

        // ── Step 0a: Is this a repeat of a request that already placed an order? ─
        // Checked before anything is looked up, so a retry gets the original answer even if
        // the stock or the prices have changed since.
        string? requestHash = null;
        if (key is not null)
        {
            requestHash = OrderRequestFingerprint.Compute(validation.Lines, request.Notes);
            var previous = await idempotency.FindAsync(
                userId, key, requestHash, HttpContext.RequestAborted);
            if (previous.Outcome != IdempotencyOutcome.New)
                return AnswerFromIdempotencyRecord(previous, userId, key);
        }

        // ── Step 0b: Resolve authoritative names and prices ───────────────────
        // If the Product service cannot answer we reject the order (HTTP 503) rather
        // than guess a price: charging a stale or client-supplied price is worse than
        // asking the caller to retry.
        IReadOnlyDictionary<Guid, CatalogProduct?> catalog;
        try
        {
            catalog = await _productCatalog.GetProductsAsync(
                validation.Lines.Select(l => l.ProductId), HttpContext.RequestAborted);
        }
        catch (ProductCatalogUnavailableException ex)
        {
            _logger.LogError(ex,
                "Order rejected for User {UserId}: Product service unavailable", userId);
            OrderMetrics.RecordOrderRejected(OrderRejection.CatalogUnavailable);
            Response.Headers["Retry-After"] = "5";
            return StatusCode(StatusCodes.Status503ServiceUnavailable, new
            {
                message = "Product catalog is temporarily unavailable. Please retry shortly.",
                errors = Array.Empty<string>()
            });
        }

        var resolution = OrderLineResolver.Resolve(validation.Lines, catalog);
        if (!resolution.IsValid)
        {
            _logger.LogWarning(
                "Order rejected for User {UserId}: {Errors}",
                userId, string.Join("; ", resolution.Errors));
            OrderMetrics.RecordOrderRejected(OrderRejection.UnknownProduct);
            return BadRequest(new
            {
                message = "Order could not be placed.",
                errors = resolution.Errors
            });
        }

        _logger.LogInformation(
            "User {UserId} placing order with {ItemCount} items",
            userId, resolution.Lines.Count);

        // ── Step 1: Validate stock via cache (eventual consistency) ───────────
        // Layer 1: Redis cache hit    → ~1ms,   30s TTL
        // Layer 2: Redis cache miss   → ~150ms, gRPC → DB
        // Layer 3: gRPC down          → stale cache, 5min TTL
        // Layer 4: Everything down    → reject order
        // Layer 5: Saga reservation   → authoritative check in InventoryService (row locking planned)
        var stockErrors = new List<string>();
        var inventoryUnreachable = false;
        var hasStaleData = false;

        var productIds = resolution.Lines.Select(l => l.ProductId).ToList();
        var stockItems = await _inventoryCache.GetStockBatchAsync(
            productIds, HttpContext.RequestAborted);

        foreach (var item in resolution.Lines)
        {
            var stock = stockItems.FirstOrDefault(s => s.ProductId == item.ProductId);

            if (stock is null)
            {
                stockErrors.Add(
                    $"Product '{item.ProductName}' unavailable — " +
                    "inventory service temporarily unreachable.");
                inventoryUnreachable = true;
                continue;
            }

            if (stock.IsStale) hasStaleData = true;

            if (stock.AvailableQuantity < item.Quantity)
            {
                stockErrors.Add(
                    $"Insufficient stock for '{stock.ProductName}': " +
                    $"requested {item.Quantity}, available {stock.AvailableQuantity}.");
            }
        }

        if (stockErrors.Any())
        {
            _logger.LogWarning(
                "Order rejected for User {UserId}: {Errors}",
                userId, string.Join("; ", stockErrors));
            OrderMetrics.RecordOrderRejected(inventoryUnreachable
                ? OrderRejection.InventoryUnreachable : OrderRejection.InsufficientStock);
            return BadRequest(new
            {
                message = "Order could not be placed.",
                errors = stockErrors
            });
        }

        if (hasStaleData)
            _logger.LogWarning(
                "Order accepted with stale inventory data for User {UserId}. " +
                "Saga will verify actual stock.", userId);

        // ── Step 2: Build the order ───────────────────────────────────────────
        var orderItems = resolution.Lines.Select(l => new OrderItem
        {
            Id = Guid.NewGuid(),
            ProductId = l.ProductId,
            ProductName = l.ProductName,   // from the Product service
            Quantity = l.Quantity,
            UnitPrice = l.UnitPrice,       // from the Product service
        }).ToList();

        var order = new Order
        {
            Id = Guid.NewGuid(),
            UserId = userId,
            UserEmail = userEmail,
            Status = OrderStatus.Pending,
            Notes = request.Notes?.Trim(),
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
            Items = orderItems
        };
        order.TotalAmount = order.Items.Sum(i => i.LineTotal);

        // ── Step 3: Save order + outbox entry + read model + idempotency key atomically ─
        var eventData = new OrderConfirmedEvent
        {
            OrderId = order.Id,
            UserId = order.UserId,
            UserEmail = order.UserEmail,
            TotalAmount = order.TotalAmount,
            ConfirmedAt = DateTime.UtcNow,
            Items = order.Items.Select(i => new OrderLineItem
            {
                ProductId = i.ProductId,
                ProductName = i.ProductName,
                Quantity = i.Quantity,
                UnitPrice = i.UnitPrice,
            }).ToList()
        };

        var outboxEntry = new OutboxEntry
        {
            Id = Guid.NewGuid(),
            EventType = nameof(OrderConfirmedEvent),
            Payload = System.Text.Json.JsonSerializer.Serialize(eventData),
            CreatedAt = DateTime.UtcNow,
            // lets the saga's messages, in every service, be found by this request's id
            CorrelationId = CorrelationContext.Current
        };

        // The read model row is part of the same transaction, so an order is never visible
        // without its summary (it appears immediately in GET /api/orders). Later status
        // changes (Confirmed, Cancelled) are applied by the saga.
        var response = MapToResponse(order);

        _db.Orders.Add(order);
        _db.OutboxEntries.Add(outboxEntry);
        readModelUpdater.Stage(order, orderItems);
        if (key is not null)
        {
            idempotency.Add(userId, key, requestHash!, order.Id, StatusCodes.Status201Created,
                JsonSerializer.Serialize(response, jsonOptions.Value.JsonSerializerOptions));
        }

        try
        {
            await _db.SaveChangesAsync();
        }
        catch (DbUpdateException ex) when (key is not null && IdempotencyStore.IsDuplicateKey(ex))
        {
            // A request with the same key committed first (two copies in flight at once).
            // Nothing of ours was saved; answer from the one that won.
            _db.ChangeTracker.Clear();
            var winner = await idempotency.FindAsync(
                userId, key, requestHash!, HttpContext.RequestAborted);
            if (winner.Outcome != IdempotencyOutcome.New)
                return AnswerFromIdempotencyRecord(winner, userId, key);
            throw;
        }

        _logger.LogInformation(
            "Order {OrderId} saved with status Pending. " +
            "Outbox entry created — Saga will orchestrate inventory + payment.",
            order.Id);

        OrderMetrics.RecordOrderPlaced(order.TotalAmount);

        return CreatedAtAction(
            actionName: "GetById",
            controllerName: "OrderQueries",
            routeValues: new { orderId = order.Id },
            value: response);
    }

    // The answer to a request whose Idempotency-Key is already known: the stored response for a
    // repeat of the same request, 422 for a different request that reuses the key.
    private IActionResult AnswerFromIdempotencyRecord(IdempotencyLookup lookup, string userId, string key)
    {
        var record = lookup.Record!;

        if (lookup.Outcome == IdempotencyOutcome.Mismatch)
        {
            _logger.LogWarning(
                "Order rejected for User {UserId}: Idempotency-Key {IdempotencyKey} was already used " +
                "for a different request (Order {OrderId})",
                userId, key, record.OrderId);
            OrderMetrics.RecordIdempotentReplay("key_reused_with_different_request");
            return UnprocessableEntity(new
            {
                message = "Order could not be placed.",
                errors = new[]
                {
                    $"The {IdempotencyKey.HeaderName} was already used with a different request. " +
                    "Use a new key for a different order."
                }
            });
        }

        _logger.LogInformation(
            "Order request for User {UserId} repeats Idempotency-Key {IdempotencyKey}: " +
            "returning the response for Order {OrderId} without placing another order",
            userId, key, record.OrderId);
        OrderMetrics.RecordIdempotentReplay("replayed");

        Response.Headers["Idempotency-Replayed"] = "true";
        Response.Headers.Location = Url.Action("GetById", "OrderQueries", new { orderId = record.OrderId });
        return new ContentResult
        {
            StatusCode = record.StatusCode,
            ContentType = "application/json; charset=utf-8",
            Content = record.ResponseBody
        };
    }

    // ── DELETE /api/orders/{id} ───────────────────────────────────────────────
    // Cancels the order through the saga (see OrderCancellationService):
    //   204 cancelled / already cancelled · 202 accepted, stock release and/or refund in progress
    //   404 unknown order (or someone else's) · 409 too late: shipped, or the payment is being finalized
    [HttpDelete("{id:guid}")]
    public async Task<IActionResult> Cancel(Guid id)
    {
        var userId = User.FindFirst("sub")?.Value;
        if (userId is null) return Unauthorized();

        var result = await cancellation.CancelAsync(
            id, userId, User.IsInRole("admin"), HttpContext.RequestAborted);

        switch (result.Outcome)
        {
            case CancelOutcome.NotFound:
                return NotFound();

            case CancelOutcome.NotCancellable:
                return Conflict(new { message = result.Message });

            case CancelOutcome.Cancelling:
                return Accepted(new { status = "Cancelling", message = result.Message });

            default: // Cancelled, AlreadyCancelled
                return NoContent();
        }
    }

    private static object MapToResponse(Order o) => new
    {
        o.Id,
        o.UserId,
        o.UserEmail,
        o.Status,
        o.TotalAmount,
        o.Notes,
        o.CreatedAt,
        o.UpdatedAt,
        Items = o.Items.Select(i => new
        {
            i.Id,
            i.ProductId,
            i.ProductName,
            i.Quantity,
            i.UnitPrice,
            i.LineTotal
        })
    };
}

// Request body for POST /api/orders.
// Items is nullable so that a missing/null list reaches OrderRequestValidator and
// produces a clean 400 message instead of a framework-generated one.
public record PlaceOrderRequest
{
    public List<OrderItemRequest?>? Items { get; init; }
    public string? Notes { get; init; }
}

// Only ProductId and Quantity are used.
// ProductName and UnitPrice are still accepted so existing clients keep working,
// but they are IGNORED: the Product service is the source of truth for both.
public record OrderItemRequest
{
    public Guid ProductId { get; init; }
    public int Quantity { get; init; }
    public string? ProductName { get; init; }
    public decimal? UnitPrice { get; init; }
}