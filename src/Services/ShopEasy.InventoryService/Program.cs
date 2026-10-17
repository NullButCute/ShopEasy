using MassTransit;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Prometheus;
using Serilog;
using Serilog.Events;
using ShopEasy.InventoryService.Consumers;
using ShopEasy.InventoryService.Data;
using ShopEasy.InventoryService.Grpc;
using ShopEasy.Shared.Logging;
using ShopEasy.Shared.Messaging;
using ShopEasy.Shared.Middleware;
using ShopEasy.Shared.Resilience;
using ShopEasy.Shared.Security;
using ShopEasy.Shared.Services;
using StackExchange.Redis;

Log.Logger = new LoggerConfiguration()
    .ConfigureShopEasy("ShopEasy.InventoryService")
    .CreateBootstrapLogger();

try
{
    // Enable HTTP/2 for gRPC before builder is created
    AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

    var builder = WebApplication.CreateBuilder(args);

    // Configure Kestrel to accept both HTTP/1.1 (REST) and HTTP/2 (gRPC)
    builder.WebHost.ConfigureKestrel(options =>
    {
        // REST API — HTTP/1.1 only
        options.ListenAnyIP(8080, o =>
            o.Protocols = HttpProtocols.Http1);

        // gRPC — HTTP/2 only (no TLS negotiation needed when Http2 only)
        options.ListenAnyIP(8090, o =>
            o.Protocols = HttpProtocols.Http2);
    });


    builder.Host.UseSerilog((ctx, _, cfg) => cfg
        .ConfigureShopEasy("ShopEasy.InventoryService", ctx.Configuration, ctx.HostingEnvironment.EnvironmentName));

    builder.Services.AddGrpc(options =>
    {
        options.EnableDetailedErrors = builder.Environment.IsDevelopment();
    });

    builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .SetResourceBuilder(ResourceBuilder.CreateDefault()
            .AddService("ShopEasy.InventoryService"))
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

    builder.Services.AddDbContext<InventoryDbContext>(options =>
    options.UseNpgsql(
        PostgresConnection.FromConfiguration(builder.Configuration)));

    // Bearer tokens issued by Identity for this API, and the scope/role policies of its endpoints
    // (anything not marked otherwise requires a signed-in caller)
    builder.Services.AddApiAuthentication(builder.Configuration, ApiAudiences.Inventory);
    builder.Services.AddApiAuthorization(AccessPolicies.Inventory);

    // ── Prometheus Metrics ────────────────────────────────────────────────────
    builder.Services.AddMetricServer(options =>
    {
        options.Port = builder.Configuration.GetValue<ushort>("Metrics:Port", 9091); // separate port from the API
    });

    // Row-level locking (SELECT ... FOR UPDATE) used by every stock-changing code path
    builder.Services.AddSingleton<IInventoryLocking, PostgresInventoryLocking>();

    // Carry the correlation id across RabbitMQ: stamp it on outgoing messages, restore it (and
    // the OrderId) for the logs of the consumer that receives them
    builder.Services.AddScoped(typeof(CorrelationConsumeFilter<>));
    builder.Services.AddScoped(typeof(CorrelationPublishFilter<>));
    builder.Services.AddScoped(typeof(CorrelationSendFilter<>));

    builder.Services.AddMassTransit(x =>
    {
        x.AddConsumer<ReserveInventoryConsumer>();
        x.AddConsumer<ReleaseInventoryConsumer>();
        x.AddConsumer<CommitInventoryConsumer>();
        x.AddConsumer<RestockInventoryConsumer>();

        // ── Transactional inbox/outbox ────────────────────────────────────────
        // Each consumer endpoint below runs inside ONE database transaction holding the stock
        // changes, an inbox row (a redelivered command is not processed twice) and the events
        // the consumer publishes (written to the OutboxMessage table, not sent yet). MassTransit
        // sends them to RabbitMQ after the commit, so stock is never changed without its result
        // event, and no event is sent for a change that was rolled back. If the broker is
        // unreachable at that moment the command stays unacknowledged; its redelivery completes
        // the delivery without repeating the stock change.
        x.AddEntityFrameworkOutbox<InventoryDbContext>(o =>
        {
            o.UsePostgres();
            // The consumers lock rows with SELECT ... FOR UPDATE; under READ COMMITTED a waiting
            // transaction re-reads the committed values instead of failing with a
            // serialization error.
            o.IsolationLevel = System.Data.IsolationLevel.ReadCommitted;
        });

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

            cfg.ReceiveEndpoint("inventory-reserve", e =>
            {
                e.ConfigureConsumeTopology = false;
                e.ConcurrentMessageLimit = 1;
                e.Bind("ShopEasy.Shared.Contracts:ReserveInventoryCommand", b =>
                {
                    b.ExchangeType = "fanout";
                    b.Durable = true;
                });
                e.UseEntityFrameworkOutbox<InventoryDbContext>(ctx);
                e.ConfigureConsumer<ReserveInventoryConsumer>(ctx);
            });

            cfg.ReceiveEndpoint("inventory-release", e =>
            {
                e.ConfigureConsumeTopology = false;
                e.ConcurrentMessageLimit = 1;
                e.Bind("ShopEasy.Shared.Contracts:ReleaseInventoryCommand", b =>
                {
                    b.ExchangeType = "fanout";
                    b.Durable = true;
                });
                e.UseEntityFrameworkOutbox<InventoryDbContext>(ctx);
                e.ConfigureConsumer<ReleaseInventoryConsumer>(ctx);
            });

            cfg.ReceiveEndpoint("inventory-commit", e =>
            {
                e.ConfigureConsumeTopology = false;
                e.ConcurrentMessageLimit = 1;
                e.Bind("ShopEasy.Shared.Contracts:CommitInventoryCommand", b =>
                {
                    b.ExchangeType = "fanout";
                    b.Durable = true;
                });
                e.UseEntityFrameworkOutbox<InventoryDbContext>(ctx);
                e.ConfigureConsumer<CommitInventoryConsumer>(ctx);
            });

            cfg.ReceiveEndpoint("inventory-restock", e =>
            {
                e.ConfigureConsumeTopology = false;
                e.ConcurrentMessageLimit = 1;
                e.Bind("ShopEasy.Shared.Contracts:RestockInventoryCommand", b =>
                {
                    b.ExchangeType = "fanout";
                    b.Durable = true;
                });
                e.UseEntityFrameworkOutbox<InventoryDbContext>(ctx);
                e.ConfigureConsumer<RestockInventoryConsumer>(ctx);
            });
        });
    });

    builder.Services.AddControllers();
    builder.Services.AddEndpointsApiExplorer();
    builder.Services.AddSwaggerGen(options =>
    {
        options.SwaggerDoc("v1", new Microsoft.OpenApi.Models.OpenApiInfo
        {
            Title = "ShopEasy - Inventory API",
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

    builder.Services.AddHealthChecks()
        .AddDbContextCheck<InventoryDbContext>(
        name: "database",
        tags: new[] { "ready" })
        .AddRabbitMQ(
        rabbitConnectionString: $"amqp://{builder.Configuration["RabbitMQ:Username"]}:{builder.Configuration["RabbitMQ:Password"]}@{builder.Configuration["RabbitMQ:Host"]}/",
        name: "rabbitmq",
        failureStatus: Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus.Unhealthy,
        tags: new[] { "ready" });

    builder.Services.AddGrpcHealthChecks()
    .AddCheck("grpc", () =>Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckResult.Healthy());

    #region caching
    var redisConnection = builder.Configuration["Redis:Connection"] ?? "redis:6379";
    builder.Services.AddStackExchangeRedisCache(options =>
    {
        options.Configuration = redisConnection;
        options.InstanceName = "shopeasy:";
    });
    // With this — abortConnect=false means it won't crash if Redis is unavailable:
    builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>ConnectionMultiplexer.Connect($"{redisConnection},abortConnect=false"));
    builder.Services.AddScoped<CacheService>();
    #endregion



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
        var db = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();
        var logger = scope.ServiceProvider
            .GetRequiredService<ILogger<Program>>();
        try
        {
            logger.LogInformation("Applying database migrations...");
            db.Database.Migrate();
            // Seed only if no data exists
            if (!db.InventoryItems.Any())
                InventoryDbSeeder.Seed(db);
            logger.LogInformation("Database migrations applied.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to apply migrations");
            throw;
        }
    }

    app.UseSerilogRequestLogging(options =>
    {
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
    app.UseHttpMetrics(options =>
    {
        options.AddCustomLabel("service", _ => "InventoryService");
    });
    // scraped by Prometheus without credentials (see "Known limitations" in the README)
    app.MapMetrics().AllowAnonymous();
    app.MapControllers();

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

    // gRPC stock check for the Order service: HTTP/2 only (port 8090), bearer token required
    app.MapInventoryGrpc();
    app.MapGrpcHealthChecksService().AllowAnonymous();

    app.Run();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Inventory Service terminated unexpectedly");
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