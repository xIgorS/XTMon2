using Microsoft.AspNetCore.Authentication.Negotiate;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Options;
using Serilog;
using Serilog.Debugging;
using Serilog.Events;
using XTMon.Components;
using XTMon.Infrastructure;
using XTMon.Repositories;
using XTMon.Services;
using XTMon.Options;
using XTMon.Security;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Hosting.StaticWebAssets;

var builder = WebApplication.CreateBuilder(args);
var selfLogSync = new object();

SelfLog.Enable(message =>
{
    try
    {
        var logsDirectory = Path.Combine(AppContext.BaseDirectory, "logs");
        Directory.CreateDirectory(logsDirectory);
        var selfLogPath = Path.Combine(logsDirectory, "serilog-selflog.log");
        lock (selfLogSync)
        {
            File.AppendAllText(selfLogPath, $"{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss.fff zzz} {message}{Environment.NewLine}");
        }
    }
    catch
    {
        // Intentionally ignored to avoid recursive logger errors.
    }
});

builder.Host.UseSerilog((context, services, loggerConfiguration) =>
{
    var loggingConnectionStringName = context.Configuration["StoredProcedureLogging:ConnectionStringName"] ?? "LogFiAlmt";
    var storedProcedureName = context.Configuration["StoredProcedureLogging:StoredProcedure"] ?? "monitoring.UspInsertAPSActionsLog";
    var commandTimeoutSeconds = int.TryParse(context.Configuration["StoredProcedureLogging:CommandTimeoutSeconds"], out var timeout)
        ? Math.Max(1, timeout)
        : 5;
    var loggingConnectionString = context.Configuration.GetConnectionString(loggingConnectionStringName);

    loggerConfiguration
        .ReadFrom.Configuration(context.Configuration)
        .ReadFrom.Services(services)
        .Enrich.FromLogContext();

    if (!string.IsNullOrWhiteSpace(loggingConnectionString) && !string.IsNullOrWhiteSpace(storedProcedureName))
    {
        loggerConfiguration.WriteTo.Async(a => a.Sink(
            new StoredProcedureLogSink(loggingConnectionString, storedProcedureName, commandTimeoutSeconds),
            restrictedToMinimumLevel: LogEventLevel.Warning));
    }
});

// Force HTTP/1.1 for Negotiate/NTLM compatibility when running on Kestrel directly.
// When running behind IIS Express (Visual Studio), IIS handles auth natively.
builder.WebHost.ConfigureKestrel(options =>
{
    options.ConfigureEndpointDefaults(listenOptions =>
    {
        listenOptions.Protocols = HttpProtocols.Http1;
    });
});

builder.Services.AddAuthentication(NegotiateDefaults.AuthenticationScheme)
    .AddNegotiate();

// When hosted in IIS in-process, IIS handles auth natively and the Negotiate handler
// defers to it automatically. AutomaticAuthentication ensures IIS forwards the identity.
builder.Services.Configure<IISOptions>(options =>
{
    options.AutomaticAuthentication = true;
});

// Add services to the container.
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services
    .AddOptions<MonitoringOptions>()
    .Bind(builder.Configuration.GetSection(MonitoringOptions.SectionName))
    .ValidateDataAnnotations()
    .ValidateOnStart();
builder.Services
    .AddOptions<UamAuthorizationOptions>()
    .Bind(builder.Configuration.GetSection(UamAuthorizationOptions.SectionName))
    .ValidateDataAnnotations()
    .ValidateOnStart();
builder.Services
    .AddOptions<ReplayFlowsOptions>()
    .Bind(builder.Configuration.GetSection(ReplayFlowsOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.GetFailedFlowsStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.ReplayFlowsStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.ReplayFlowsTableTypeName) &&
        !string.IsNullOrWhiteSpace(options.ProcessReplayFlowsStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.GetReplayFlowStatusStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.GetReplayFlowProcessStatusStoredProcedure),
        "ReplayFlows options must define all required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<JvCalculationOptions>()
    .Bind(builder.Configuration.GetSection(JvCalculationOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.PnlDatesConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.PublicationConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.GetPnlDatesStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.CheckJvCalculationStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.DtmFiConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.FixJvCalculationStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.JobEnqueueStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobTakeNextStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobHeartbeatStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobSaveResultStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobMarkCompletedStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobMarkFailedStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobGetByIdStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobGetLatestStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobExpireStaleStoredProcedure),
        "JvCalculation options must define all required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services.AddSingleton<SqlConnectionFactory>();
builder.Services.AddScoped<IMonitoringRepository, MonitoringRepository>();
builder.Services.AddScoped<IJvCalculationRepository, JvCalculationRepository>();
builder.Services.AddScoped<IReplayFlowRepository, ReplayFlowRepository>();
builder.Services.AddScoped<IUamAuthorizationRepository, UamAuthorizationRepository>();
builder.Services.AddScoped<IAuthorizationHandler, UamPermissionHandler>();
builder.Services.AddScoped<IDeploymentCheckService, DeploymentCheckService>();
builder.Services.AddSingleton<ReplayFlowProcessingQueue>();
builder.Services.AddHostedService<ReplayFlowProcessingService>();
builder.Services.AddHostedService<JvCalculationProcessingService>();

// Use default authentication scheme (Negotiate)
builder.Services.AddAuthorization(options =>
{
    options.FallbackPolicy = options.DefaultPolicy;

    options.AddPolicy("UamRestricted", policy =>
    {
        if (builder.Environment.IsProduction())
        {
            policy.Requirements.Add(new RequiresUamPermissionRequirement());
        }
        else
        {
            // In Development/others, bypass the DB check and just require authentication
            policy.RequireAuthenticatedUser();
            policy.RequireAssertion(_ => true);
        }
    });
});
builder.Services.AddCascadingAuthenticationState();

// Enable static web assets in non-Development environments so that
// 'dotnet run --launch-profile https-prod' works without publishing.
if (!builder.Environment.IsDevelopment())
{
    StaticWebAssetsLoader.UseStaticWebAssets(builder.Environment, builder.Configuration);
}

var app = builder.Build();

app.Lifetime.ApplicationStarted.Register(() =>
{
    foreach (var url in app.Urls)
    {
        Console.WriteLine($"XTMon listening on: {url}");
    }
});

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    app.UseHsts();
}
// IMPORTANT: StatusCodePages MUST be after auth middleware.
// If placed before, it intercepts the 401 Negotiate challenge and breaks the NTLM handshake.
app.UseAuthentication();
app.UseAuthorization();

app.UseStatusCodePagesWithReExecute("/not-found", createScopeForStatusCodePages: true);

app.UseAntiforgery();

app.MapStaticAssets();
app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();
