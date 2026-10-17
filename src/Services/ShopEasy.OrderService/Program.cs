using MassTransit;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Prometheus;
using Serilog;
using Serilog.Events;
using ShopEasy.OrderService.Data;
using ShopEasy.OrderService.ReadModels;
using ShopEasy.OrderService.Sagas;
using ShopEasy.OrderService.Services;
using ShopEasy.OrderService.Idempotency;
using ShopEasy.Shared.Grpc;
using ShopEasy.Shared.Logging;
using ShopEasy.Shared.Messaging;
using ShopEasy.Shared.Middleware;
using ShopEasy.Shared.Resilience;
using ShopEasy.Shared.Security;
using ShopEasy.Shared.Services;
using StackExchange.Redis;
using System.Threading.RateLimiting;

Log.Logger = new LoggerConfiguration()
    .ConfigureShopEasy("ShopEasy.OrderService")
    .CreateBootstrapLogger();

try
{
    AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

    var builder = WebApplication.CreateBuilder(args);

    builder.Host.UseSerilog((ctx, _, cfg) => cfg
        .ConfigureShopEasy("ShopEasy.OrderService", ctx.Configuration, ctx.HostingEnvironment.EnvironmentName));

    builder.Services.AddDbContext<OrderDbContext>(options =>
    options.UseNpgsql(
        PostgresConnection.FromConfiguration(builder.Configuration)));

    // Bearer tokens issued by Identity for this API, and the scope/role policies of its endpoints
    // (anything not marked otherwise requires a signed-in caller)
    builder.Services.AddApiAuthentication(builder.Configuration, ApiAudiences.Order);
    builder.Services.AddApiAuthorization(AccessPolicies.Order);

    // ── Token Service (for Client Credentials / M2M calls) ───────────────────
    // Row-level locking (SELECT ... FOR UPDATE) for every saga state transition
    builder.Services.AddSingleton<ISagaLocking, PostgresSagaLocking>();

    // ── Calls to other services: deadlines, retry budget, circuit breaker ─────
    // Settings: Resilience:Inventory (the gRPC stock check) and Resilience:RetryBudget (how much
    // extra load retries may add). Invalid values stop the service at start-up.
    var retryBudgetOptions = RetryBudgetOptions.From(builder.Configuration);
    builder.Services.AddOptions<RetryBudgetOptions>()
        .Bind(builder.Configuration.GetSection(RetryBudgetOptions.SectionName))
        .Validate(o => { o.Validate(); return true; })
        .ValidateOnStart();
    builder.Services.AddOptions<InventoryCallOptions>()
        .Bind(builder.Configuration.GetSection(InventoryCallOptions.SectionName))
        .Validate(o => { o.Validate(); return true; })
        .ValidateOnStart();
    builder.Services.AddSingleton<InventoryCallPolicy>();
    builder.Services.AddSingleton<StockFetchCoordinator>();
    builder.Services.AddSingleton<InventoryServiceCall>();

    builder.Services.AddSingleton<ITokenService, TokenService>();

    // The token endpoint of Identity, used by TokenService. Bounded: when Identity is down the
    // service token is simply not renewed (the cached one is used while it is valid) instead of
    // every stock check waiting out the default 100 seconds.
    builder.Services.AddHttpClient("idp", client => client.Timeout = TimeSpan.FromSeconds(5));

    // ── Typed HTTP Client for ProductService (server-side price lookup) ───────
    // PlaceOrder never trusts client-supplied prices: it asks the Product service.
    // Tight timeouts because a customer is waiting; if the catalog is unavailable
    // the order is rejected with 503 (see OrderCommandsController).
    var productRetryBudget = new RetryBudget(retryBudgetOptions);
    builder.Services.AddHttpClient<IProductCatalogClient, ProductCatalogClient>(client =>
        {
            client.BaseAddress = new Uri(
                builder.Configuration["Services:ProductService"] ?? "http://product:8080");
        })
        .AddHttpMessageHandler(() => new RetryBudgetHandler(productRetryBudget))
        .AddHttpMessageHandler<CorrelationIdHandler>()
        .AddStandardResilienceHandler(options =>
        {
            options.AttemptTimeout.Timeout = TimeSpan.FromSeconds(3);
            options.Retry.MaxRetryAttempts = 2;
            options.Retry.Delay = TimeSpan.FromMilliseconds(200);
            // retries only while the budget allows them (see Resilience:RetryBudget)
            options.Retry.ShouldHandle = HttpRetryBudget.ShouldRetry(
                productRetryBudget, "product", options.Retry.MaxRetryAttempts);

            options.CircuitBreaker.SamplingDuration = TimeSpan.FromSeconds(30);
            options.CircuitBreaker.FailureRatio = 0.5;
            options.CircuitBreaker.MinimumThroughput = 5;
            options.CircuitBreaker.BreakDuration = TimeSpan.FromSeconds(15);

            options.TotalRequestTimeout.Timeout = TimeSpan.FromSeconds(10);
        });

    builder.Services.AddTransient<CorrelationIdHandler>();
    builder.Services.AddScoped<OrderReadModelUpdater>();
    builder.Services.AddScoped<OrderCancellationService>();
    // Outbox delivery waits for MassTransit's bus health check (bus started, broker connected,
    // every receive endpoint declared) instead of polling the RabbitMQ management API.
    builder.Services.AddSingleton<IBrokerReadiness, MassTransitBrokerReadiness>();
    // Error queues are inspected and requeued over AMQP, again without the management API.
    builder.Services.AddSingleton<IErrorQueueClient, RabbitMqErrorQueueClient>();
    builder.Services.AddHttpClient();
    builder.Services.AddHostedService<OutboxDeliveryService>();
    // Carry the correlation id across RabbitMQ: stamp it on outgoing messages, restore it (and
    // the OrderId) for the logs of the consumer that receives them
    builder.Services.AddScoped(typeof(CorrelationConsumeFilter<>));
    builder.Services.AddScoped(typeof(CorrelationPublishFilter<>));
    builder.Services.AddScoped(typeof(CorrelationSendFilter<>));

    builder.Services.AddMassTransit(x =>
    {
        x.AddConsumer<OrderConfirmedSagaStarter>();
        x.AddConsumer<InventoryReservedConsumer>();
        x.AddConsumer<InventoryReservationFailedConsumer>();
        x.AddConsumer<PaymentSucceededConsumer>();
        x.AddConsumer<PaymentFailedConsumer>();
        x.AddConsumer<InventoryReleasedConsumer>();
        x.AddConsumer<InventoryCommittedConsumer>();
        x.AddConsumer<InventoryCommitFailedConsumer>();
        x.AddConsumer<PaymentRefundedConsumer>();
        x.AddConsumer<InventoryRestockedConsumer>();

        x.UsingRabbitMq((ctx, cfg) =>
        {
            cfg.Host(builder.Configuration["RabbitMQ:Host"] ?? "rabbitmq", "/", h =>
            {
                h.Username(builder.Configuration["RabbitMQ:Username"] ?? throw new InvalidOperationException("RabbitMQ:Username is not configured."));
                h.Password(builder.Configuration["RabbitMQ:Password"] ?? throw new InvalidOperationException("RabbitMQ:Password is not configured."));
            });

            // First, so the correlation scope also covers the retries and the outbox below
            cfg.UseConsumeFilter(typeof(CorrelationConsumeFilter<>), ctx);
            cfg.UsePublishFilter(typeof(CorrelationPublishFilter<>), ctx);
            cfg.UseSendFilter(typeof(CorrelationSendFilter<>), ctx);

            // ── Global retry policy ───────────────────────────────────────────────
            // A database or network outage is ridden out for several minutes with growing
            // pauses; anything else is retried 3 times quickly before moving to the error
            // queue (see ShopEasy.Shared.Resilience.ConsumerResilience, settings under
            // Resilience:Consumers).
            cfg.UseConsumerResilience(ConsumerResilienceOptions.From(builder.Configuration));

            // Saga starter — bind directly to the publisher exchange
            cfg.ReceiveEndpoint("order-saga-starter", e =>
            {
                e.ConfigureConsumeTopology = false;
                e.Bind("ShopEasy.Shared.Contracts:OrderConfirmedEvent", b =>
                {
                    b.ExchangeType = "fanout";
                    b.Durable = true;
                });
                e.ConfigureConsumer<OrderConfirmedSagaStarter>(ctx);
            });

            // Saga event consumers — these receive responses from inventory and payment
            cfg.ReceiveEndpoint("order-saga-inventory-reserved", e =>
            {
                e.ConfigureConsumeTopology = false;
                e.Bind("ShopEasy.Shared.Contracts:InventoryReservedEvent", b =>
                {
                    b.ExchangeType = "fanout";
                    b.Durable = true;
                });
                e.ConfigureConsumer<InventoryReservedConsumer>(ctx);
            });

            cfg.ReceiveEndpoint("order-saga-inventory-failed", e =>
            {
                e.ConfigureConsumeTopology = false;
                e.Bind("ShopEasy.Shared.Contracts:InventoryReservationFailedEvent", b =>
                {
                    b.ExchangeType = "fanout";
                    b.Durable = true;
                });
                e.ConfigureConsumer<InventoryReservationFailedConsumer>(ctx);
            });

            cfg.ReceiveEndpoint("order-saga-payment-succeeded", e =>
            {
                e.ConfigureConsumeTopology = false;
                e.Bind("ShopEasy.Shared.Contracts:PaymentSucceededEvent", b =>
                {
                    b.ExchangeType = "fanout";
                    b.Durable = true;
                });
                e.ConfigureConsumer<PaymentSucceededConsumer>(ctx);
            });

            cfg.ReceiveEndpoint("order-saga-payment-failed", e =>
            {
                e.ConfigureConsumeTopology = false;
                e.Bind("ShopEasy.Shared.Contracts:PaymentFailedEvent", b =>
                {
                    b.ExchangeType = "fanout";
                    b.Durable = true;
                });
                e.ConfigureConsumer<PaymentFailedConsumer>(ctx);
            });

            cfg.ReceiveEndpoint("order-saga-inventory-released", e =>
            {
                e.ConfigureConsumeTopology = false;
                e.Bind("ShopEasy.Shared.Contracts:InventoryReleasedEvent", b =>
                {
                    b.ExchangeType = "fanout";
                    b.Durable = true;
                });
                e.ConfigureConsumer<InventoryReleasedConsumer>(ctx);
            });

            // Inventory reports the result of CommitInventoryCommand
            cfg.ReceiveEndpoint("order-saga-inventory-committed", e =>
            {
                e.ConfigureConsumeTopology = false;
                e.Bind("ShopEasy.Shared.Contracts:InventoryCommittedEvent", b =>
                {
                    b.ExchangeType = "fanout";
                    b.Durable = true;
                });
                e.ConfigureConsumer<InventoryCommittedConsumer>(ctx);
            });

            cfg.ReceiveEndpoint("order-saga-inventory-commit-failed", e =>
            {
                e.ConfigureConsumeTopology = false;
                e.Bind("ShopEasy.Shared.Contracts:InventoryCommitFailedEvent", b =>
                {
                    b.ExchangeType = "fanout";
                    b.Durable = true;
                });
                e.ConfigureConsumer<InventoryCommitFailedConsumer>(ctx);
            });

            // Payment reports that a refund is done (the cancellation of a paid order)
            cfg.ReceiveEndpoint("order-saga-payment-refunded", e =>
            {
                e.ConfigureConsumeTopology = false;
                e.Bind("ShopEasy.Shared.Contracts:PaymentRefundedEvent", b =>
                {
                    b.ExchangeType = "fanout";
                    b.Durable = true;
                });
                e.ConfigureConsumer<PaymentRefundedConsumer>(ctx);
            });

            // Inventory reports that the stock of a cancelled, paid order is back
            cfg.ReceiveEndpoint("order-saga-inventory-restocked", e =>
            {
                e.ConfigureConsumeTopology = false;
                e.Bind("ShopEasy.Shared.Contracts:InventoryRestockedEvent", b =>
                {
                    b.ExchangeType = "fanout";
                    b.Durable = true;
                });
                e.ConfigureConsumer<InventoryRestockedConsumer>(ctx);
            });
        });
    });
    builder.Services.AddSingleton<DeadLetterMonitor>();
    builder.Services.AddHostedService(sp => sp.GetRequiredService<DeadLetterMonitor>());
    builder.Services.AddHostedService<SagaOrchestrator>();
    builder.Services.AddHostedService<SagaTimeoutService>();
    builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .SetResourceBuilder(ResourceBuilder.CreateDefault()
            .AddService("ShopEasy.OrderService"))
        .AddSource("MassTransit") // a span per message published / consumed, linked across RabbitMQ
        .AddSource("Npgsql") // a span per database command
        .AddAspNetCoreInstrumentation(options =>
        {
            options.Filter = ctx =>
                !ctx.Request.Path.StartsWithSegments("/health");
        })
        .AddHttpClientInstrumentation()
        .AddOtlpExporter(options =>
        {
            options.Endpoint = new Uri(
                builder.Configuration["Telemetry:Endpoint"]
                ?? "http://jaeger:4317");
        }));

    builder.Services.AddHttpContextAccessor();
    builder.Services.AddControllers();
    builder.Services.AddEndpointsApiExplorer();
    builder.Services.AddSwaggerGen(options =>
    {
        options.SwaggerDoc("v1", new Microsoft.OpenApi.Models.OpenApiInfo
        {
            Title = "ShopEasy - Order API",
            Version = "v1"
        });

        // This adds the Authorize button in Swagger UI
        // so you can paste a Bearer token and test protected endpoints
        options.AddSecurityDefinition("Bearer", new Microsoft.OpenApi.Models.OpenApiSecurityScheme
        {
            Name = "Authorization",
            Type = Microsoft.OpenApi.Models.SecuritySchemeType.Http,
            Scheme = "Bearer",
            BearerFormat = "JWT",
            In = Microsoft.OpenApi.Models.ParameterLocation.Header,
            Description = "Paste your JWT token here. Get it from http://localhost:5000/account/token"
        });

        options.AddSecurityRequirement(new Microsoft.OpenApi.Models.OpenApiSecurityRequirement
    {
        {
            new Microsoft.OpenApi.Models.OpenApiSecurityScheme
            {
                Reference = new Microsoft.OpenApi.Models.OpenApiReference
                {
                    Type = Microsoft.OpenApi.Models.ReferenceType.SecurityScheme,
                    Id = "Bearer"
                }
            },
            Array.Empty<string>()
        }
    });
    });

    // ── Service-level rate limiting ───────────────────────────────────────────
    // Defense in depth — protects even if Gateway is bypassed
    builder.Services.AddRateLimiter(options =>
    {
        options.RejectionStatusCode = 429;

        // Limit order creation per user
        options.AddPolicy("create-order", context =>
        {
            var userId = context.User.FindFirst("sub")?.Value
                      ?? context.Connection.RemoteIpAddress?.ToString()
                      ?? "unknown";

            return RateLimitPartition.GetFixedWindowLimiter(
                partitionKey: userId,
                factory: _ => new System.Threading.RateLimiting.FixedWindowRateLimiterOptions
                {
                    PermitLimit = 10,
                    Window = TimeSpan.FromMinutes(1),
                    QueueProcessingOrder = System.Threading.RateLimiting.QueueProcessingOrder.OldestFirst,
                    QueueLimit = 0
                });
        });

        options.OnRejected = async (context, ct) =>
        {
            context.HttpContext.Response.StatusCode = 429;
            context.HttpContext.Response.ContentType = "application/json";
            await context.HttpContext.Response.WriteAsync(
                System.Text.Json.JsonSerializer.Serialize(new
                {
                    error = "Too many requests",
                    message = "Maximum 10 orders per minute allowed."
                }), ct);
        };
    });

    builder.Services
    .AddHealthChecks()
    .AddDbContextCheck<OrderDbContext>(
        name: "database",
        tags: new[] { "ready" })
    .AddRabbitMQ(
        rabbitConnectionString: $"amqp://{builder.Configuration["RabbitMQ:Username"]}:{builder.Configuration["RabbitMQ:Password"]}@{builder.Configuration["RabbitMQ:Host"]}/",
        name: "rabbitmq",
        tags: new[] { "ready" })
    .AddUrlGroup(
        uri: new Uri($"{builder.Configuration["IdentityServer:Authority"]}/health"),
        name: "identity",
        failureStatus: Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus.Degraded,
        tags: new[] { "ready" });

    // ── Prometheus Metrics ────────────────────────────────────────────────────
    builder.Services.AddMetricServer(options =>
    {
        options.Port = builder.Configuration.GetValue<ushort>("Metrics:Port", 9091); // separate port from the API
    });

    // ── gRPC Client for InventoryService ──────────────────────────────────────
    // Replace HTTP client with gRPC for faster, typed communication
    var inventoryGrpcAddress = builder.Configuration["Services:InventoryGrpcService"]
        ?? "http://inventory:8080";

    builder.Services.AddGrpcClient<InventoryGrpc.InventoryGrpcClient>(options =>
    {
        options.Address = new Uri(inventoryGrpcAddress);
    })
    .ConfigureChannel(options =>
    {
        options.Credentials = Grpc.Core.ChannelCredentials.Insecure;
    })
    // Connection level protection (the per-call deadline, retries and circuit breaker are in
    // InventoryCallPolicy): a connect that hangs is cut off after 3 s, and keep-alive pings find a
    // connection whose other end has vanished (a restarted pod) instead of waiting for the next
    // call to fail on it.
    .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
    {
        ConnectTimeout = TimeSpan.FromSeconds(3),
        PooledConnectionIdleTimeout = TimeSpan.FromMinutes(5),
        KeepAlivePingDelay = TimeSpan.FromSeconds(30),
        KeepAlivePingTimeout = TimeSpan.FromSeconds(10),
        KeepAlivePingPolicy = HttpKeepAlivePingPolicy.WithActiveRequests,
        EnableMultipleHttp2Connections = true
    });
    builder.Services.AddScoped<IStockSource, InventoryGrpcClient>();

    // ── Redis Distributed Cache ───────────────────────────────────────────────
    // Caches product data to reduce database load.
    // TTL: 5 minutes for product lists, 5 minutes for individual products.
    // Cache is invalidated when products are created, updated, or deleted.
    var redisConnection = builder.Configuration["Redis:Connection"] ?? "redis:6379";

    builder.Services.AddStackExchangeRedisCache(options =>
    {
        options.Configuration = redisConnection;
        options.InstanceName = "shopeasy:";
    });

    // With this — abortConnect=false means it won't crash if Redis is unavailable:
    builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
        ConnectionMultiplexer.Connect(
            $"{redisConnection},abortConnect=false"));

    builder.Services.AddScoped<CacheService>();
    builder.Services.AddScoped<InventoryCacheService>();
    builder.Services.AddScoped<IInventoryStock>(sp => sp.GetRequiredService<InventoryCacheService>());

    // ── Idempotent order placement (Idempotency-Key header) ───────────────────
    builder.Services.AddOptions<IdempotencyOptions>()
        .Bind(builder.Configuration.GetSection(IdempotencyOptions.SectionName))
        .Validate(o => o.RetentionHours >= 1,
            $"{IdempotencyOptions.SectionName}:{nameof(IdempotencyOptions.RetentionHours)} must be at least 1.")
        .ValidateOnStart();
    builder.Services.AddScoped<IdempotencyStore>();
    builder.Services.AddHostedService<IdempotencyCleanupService>();

    var app = builder.Build();

    app.UseMiddleware<CorrelationIdMiddleware>();

    app.UseSwagger();
    app.UseSwaggerUI(options =>
    {
        options.SwaggerEndpoint("/swagger/v1/swagger.json", "v1");
        options.RoutePrefix = string.Empty; // Swagger UI at root "/"
    });

    using (var scope = app.Services.CreateScope())
    {
        var db = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
        var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();
        try
        {
            logger.LogInformation("Applying database migrations...");
            db.Database.Migrate();
            logger.LogInformation("Database migrations applied.");
        }
        catch (Npgsql.PostgresException ex) when (ex.SqlState == "42P07")
        {
            // Table already exists — another instance ran migrations first
            // This is safe to ignore in a multi-instance deployment
            logger.LogWarning(
                "Migration table already exists — another instance applied migrations first. Continuing.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to apply database migrations");
            throw;
        }
    }

    app.UseSerilogRequestLogging(options =>
    {
        // Don't log health checks or metrics scrapes
        options.GetLevel = (ctx, elapsed, ex) =>
        {
            if (ctx.Request.Path.StartsWithSegments("/health") ||
                ctx.Request.Path.StartsWithSegments("/metrics"))
                return Serilog.Events.LogEventLevel.Debug;
            return Serilog.Events.LogEventLevel.Information;
        };
    });

    app.UseAuthentication();
    app.UseAuthorization();
    // After authentication: the "create-order" limit is per user, and a limiter that runs before it
    // cannot see the user and would count every customer against the gateway's one address
    app.UseRateLimiter();

    // Expose /metrics endpoint for Prometheus scraping
    app.UseHttpMetrics(options =>
    {
        options.AddCustomLabel("service", _ => "OrderService"); 
    });
    // scraped by Prometheus without credentials (see "Known limitations" in the README)
    app.MapMetrics().AllowAnonymous();

    app.MapControllers();

    // Liveness — is the process alive?
    app.MapHealthChecks("/health/live", new Microsoft.AspNetCore.Diagnostics.HealthChecks.HealthCheckOptions
    {
        Predicate = _ => false, // no checks — if process is running, it's live
        ResponseWriter = WriteHealthResponse
    }).AllowAnonymous();

    // Readiness — can it serve traffic?
    app.MapHealthChecks("/health/ready", new Microsoft.AspNetCore.Diagnostics.HealthChecks.HealthCheckOptions
    {
        Predicate = check => check.Tags.Contains("ready"),
        ResponseWriter = WriteHealthResponse
    }).AllowAnonymous();

    // Legacy endpoint — keeps docker-compose healthcheck working
    app.MapGet("/health", () => Results.Ok(new { status = "healthy" })).AllowAnonymous();

    app.Run();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Order Service terminated unexpectedly");
}
finally
{
    Log.CloseAndFlush();
}
static Task WriteHealthResponse(
    HttpContext context,
    Microsoft.Extensions.Diagnostics.HealthChecks.HealthReport report)
{
    context.Response.ContentType = "application/json";
    var result = System.Text.Json.JsonSerializer.Serialize(new
    {
        status = report.Status.ToString(),
        checks = report.Entries.Select(e => new
        {
            name = e.Key,
            status = e.Value.Status.ToString(),
            duration = e.Value.Duration.TotalMilliseconds + "ms",
            error = e.Value.Exception?.Message
        }),
        totalDuration = report.TotalDuration.TotalMilliseconds + "ms"
    });
    return context.Response.WriteAsync(result);
}