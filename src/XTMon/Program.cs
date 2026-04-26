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
    var loggingEnabled = !bool.TryParse(context.Configuration["StoredProcedureLogging:Enabled"], out var enabled) || enabled;
    var minimumLevel = Enum.TryParse<LogEventLevel>(context.Configuration["StoredProcedureLogging:MinimumLevel"], ignoreCase: true, out var configuredMinimumLevel)
        ? configuredMinimumLevel
        : LogEventLevel.Information;
    var commandTimeoutSeconds = int.TryParse(context.Configuration["StoredProcedureLogging:CommandTimeoutSeconds"], out var timeout)
        ? Math.Max(1, timeout)
        : 5;
    var loggingConnectionString = context.Configuration.GetConnectionString(loggingConnectionStringName);

    loggerConfiguration
        .ReadFrom.Configuration(context.Configuration)
        .ReadFrom.Services(services)
        .Enrich.FromLogContext();

    if (loggingEnabled && !string.IsNullOrWhiteSpace(loggingConnectionString) && !string.IsNullOrWhiteSpace(storedProcedureName))
    {
        loggerConfiguration.WriteTo.Async(a => a.Sink(
            new StoredProcedureLogSink(loggingConnectionString, storedProcedureName, commandTimeoutSeconds),
            restrictedToMinimumLevel: minimumLevel));
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
    .AddOptions<SystemDiagnosticsOptions>()
    .Bind(builder.Configuration.GetSection(SystemDiagnosticsOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.CleanLoggingStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.CleanHistoryStoredProcedure),
        "SystemDiagnostics options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<ApplicationLogsOptions>()
    .Bind(builder.Configuration.GetSection(ApplicationLogsOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        options.DefaultTopN <= options.MaxTopN &&
        (!options.Enabled ||
         (!string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
          !string.IsNullOrWhiteSpace(options.GetApplicationLogsStoredProcedure))),
        "ApplicationLogs options must define the required connection and stored procedure names and keep DefaultTopN within MaxTopN.")
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
        !string.IsNullOrWhiteSpace(options.RecoveryConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.GetFailedFlowsStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.ReplayFlowsStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.ReplayFlowsTableTypeName) &&
        !string.IsNullOrWhiteSpace(options.ProcessReplayFlowsStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.GetReplayFlowStatusStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.GetReplayFlowProcessStatusStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.FailStaleReplayBatchesStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.FailRunningReplayBatchesStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.GetStuckReplayBatchesStoredProcedure),
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
        !string.IsNullOrWhiteSpace(options.FixJvCalculationStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.JobEnqueueStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobTakeNextStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobHeartbeatStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobSaveResultStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobMarkCompletedStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobMarkFailedStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobMarkCancelledStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobCancelActiveStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobCountActiveStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobGetStuckStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobGetByIdStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobGetLatestStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobExpireStaleStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobFailRunningStoredProcedure),
        "JvCalculation options must define all required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<MonitoringJobsOptions>()
    .Bind(builder.Configuration.GetSection(MonitoringJobsOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.JobConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.JobEnqueueStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobTakeNextStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobHeartbeatStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobSaveResultStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobMarkCompletedStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobMarkFailedStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobMarkCancelledStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobGetActiveStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobCountActiveStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobGetStuckStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobGetByIdStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobGetLatestStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobGetLatestByCategoryStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobExpireStaleStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobCancelActiveStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobRecoverOrphanedStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobGetRuntimeByDmvStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.JobSetExecutionContextStoredProcedure) &&
        options.CategoryMaxConcurrentJobs.All(limit =>
            !string.IsNullOrWhiteSpace(limit.Key) &&
            limit.Value >= 1 &&
            limit.Value <= 16),
        "MonitoringJobs options must define all required connection names, stored procedure names, and valid concurrency limits.")
    .ValidateOnStart();
builder.Services
    .AddOptions<BatchStatusOptions>()
    .Bind(builder.Configuration.GetSection(BatchStatusOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.CheckBatchStatusStoredProcedure),
        "BatchStatus options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<ReferentialDataOptions>()
    .Bind(builder.Configuration.GetSection(ReferentialDataOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.CheckReferentialDataStoredProcedure),
        "ReferentialData options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<MarketDataOptions>()
    .Bind(builder.Configuration.GetSection(MarketDataOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.MarketDataStoredProcedure),
        "MarketData options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<PricingFileReceptionOptions>()
    .Bind(builder.Configuration.GetSection(PricingFileReceptionOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.PricingFileReceptionStoredProcedure),
        "PricingFileReception options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<OutOfScopePortfolioOptions>()
    .Bind(builder.Configuration.GetSection(OutOfScopePortfolioOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.OutOfScopePortfolioStoredProcedure),
        "OutOfScopePortfolio options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<MissingSogCheckOptions>()
    .Bind(builder.Configuration.GetSection(MissingSogCheckOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.MissingSogCheckStoredProcedure),
        "MissingSogCheck options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<AdjustmentLinksCheckOptions>()
    .Bind(builder.Configuration.GetSection(AdjustmentLinksCheckOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.AdjustmentLinksCheckStoredProcedure),
        "AdjustmentLinksCheck options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<ColumnStoreCheckOptions>()
    .Bind(builder.Configuration.GetSection(ColumnStoreCheckOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.ColumnStoreCheckStoredProcedure),
        "ColumnStoreCheck options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<TradingVsFivrCheckOptions>()
    .Bind(builder.Configuration.GetSection(TradingVsFivrCheckOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.TradingVsFivrCheckStoredProcedure),
        "TradingVsFivrCheck options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<MirrorizationOptions>()
    .Bind(builder.Configuration.GetSection(MirrorizationOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.MirrorizationStoredProcedure),
        "Mirrorization options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<ResultTransferOptions>()
    .Bind(builder.Configuration.GetSection(ResultTransferOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.ResultTransferStoredProcedure),
        "ResultTransfer options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<RolloveredPortfoliosOptions>()
    .Bind(builder.Configuration.GetSection(RolloveredPortfoliosOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.RolloveredPortfoliosStoredProcedure),
        "RolloveredPortfolios options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<SasTablesOptions>()
    .Bind(builder.Configuration.GetSection(SasTablesOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.SasTablesStoredProcedure),
        "SasTables options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<NonXtgPortfolioOptions>()
    .Bind(builder.Configuration.GetSection(NonXtgPortfolioOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.NonXtgPortfolioStoredProcedure),
        "NonXtgPortfolio options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<RejectedXtgPortfolioOptions>()
    .Bind(builder.Configuration.GetSection(RejectedXtgPortfolioOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.RejectedXtgPortfolioStoredProcedure),
        "RejectedXtgPortfolio options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<FunctionalRejectionOptions>()
    .Bind(builder.Configuration.GetSection(FunctionalRejectionOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.MenuConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.StagingConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.DtmConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.SourceSystemTechnicalRejectStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.TechnicalRejectStoredProcedure),
        "FunctionalRejection options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<FeedOutExtractionOptions>()
    .Bind(builder.Configuration.GetSection(FeedOutExtractionOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.FeedOutExtractionStoredProcedure),
        "FeedOutExtraction options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<FutureCashOptions>()
    .Bind(builder.Configuration.GetSection(FutureCashOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.FutureCashStoredProcedure),
        "FutureCash options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<FactPvCaConsistencyOptions>()
    .Bind(builder.Configuration.GetSection(FactPvCaConsistencyOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.FactPvCaConsistencyStoredProcedure),
        "FactPvCaConsistency options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<MultipleFeedVersionOptions>()
    .Bind(builder.Configuration.GetSection(MultipleFeedVersionOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.MultipleFeedVersionStoredProcedure),
        "MultipleFeedVersion options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<DailyBalanceOptions>()
    .Bind(builder.Configuration.GetSection(DailyBalanceOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.DailyBalanceStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.GetAllSourceSystemsStoredProcedure),
        "DailyBalance options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<AdjustmentsOptions>()
    .Bind(builder.Configuration.GetSection(AdjustmentsOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.AdjustmentsStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.GetAllSourceSystemsStoredProcedure),
        "Adjustments options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<PricingOptions>()
    .Bind(builder.Configuration.GetSection(PricingOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.PricingStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.GetAllSourceSystemsStoredProcedure),
        "Pricing options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<ReverseConsoFileOptions>()
    .Bind(builder.Configuration.GetSection(ReverseConsoFileOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.ReverseConsoFileStoredProcedure) &&
        !string.IsNullOrWhiteSpace(options.GetAllSourceSystemsStoredProcedure),
        "ReverseConsoFile options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<PublicationConsistencyOptions>()
    .Bind(builder.Configuration.GetSection(PublicationConsistencyOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.PublicationConsistencyStoredProcedure),
        "PublicationConsistency options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<JvBalanceConsistencyOptions>()
    .Bind(builder.Configuration.GetSection(JvBalanceConsistencyOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.JvBalanceConsistencyStoredProcedure),
        "JvBalanceConsistency options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<MissingWorkflowCheckOptions>()
    .Bind(builder.Configuration.GetSection(MissingWorkflowCheckOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.MissingWorkflowCheckStoredProcedure),
        "MissingWorkflowCheck options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<PrecalcMonitoringOptions>()
    .Bind(builder.Configuration.GetSection(PrecalcMonitoringOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.PrecalcMonitoringStoredProcedure),
        "PrecalcMonitoring options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services
    .AddOptions<VrdbStatusOptions>()
    .Bind(builder.Configuration.GetSection(VrdbStatusOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.ConnectionStringName) &&
        !string.IsNullOrWhiteSpace(options.VrdbStatusStoredProcedure),
        "VrdbStatus options must define the required connection and stored procedure names.")
    .ValidateOnStart();
builder.Services.AddSingleton<SqlConnectionFactory>();
builder.Services.AddSingleton<SqlExecutionContextAccessor>();
builder.Services.AddScoped<IMonitoringRepository, MonitoringRepository>();
builder.Services.AddScoped<ISystemDiagnosticsRepository, SystemDiagnosticsRepository>();
builder.Services.AddScoped<IApplicationLogsRepository, ApplicationLogsRepository>();
builder.Services.AddScoped<IJvCalculationRepository, JvCalculationRepository>();
builder.Services.AddScoped<IMonitoringJobRepository, MonitoringJobRepository>();
builder.Services.AddSingleton<JobCancellationRegistry>();
builder.Services.AddSingleton<IBackgroundJobCancellationService, BackgroundJobCancellationService>();
builder.Services.AddScoped<IBatchStatusRepository, BatchStatusRepository>();
builder.Services.AddScoped<IReferentialDataRepository, ReferentialDataRepository>();
builder.Services.AddScoped<IMarketDataRepository, MarketDataRepository>();
builder.Services.AddScoped<IPricingFileReceptionRepository, PricingFileReceptionRepository>();
builder.Services.AddScoped<IOutOfScopePortfolioRepository, OutOfScopePortfolioRepository>();
builder.Services.AddScoped<IMissingSogCheckRepository, MissingSogCheckRepository>();
builder.Services.AddScoped<IAdjustmentLinksCheckRepository, AdjustmentLinksCheckRepository>();
builder.Services.AddScoped<IColumnStoreCheckRepository, ColumnStoreCheckRepository>();
builder.Services.AddScoped<ITradingVsFivrCheckRepository, TradingVsFivrCheckRepository>();
builder.Services.AddScoped<IMirrorizationRepository, MirrorizationRepository>();
builder.Services.AddScoped<IResultTransferRepository, ResultTransferRepository>();
builder.Services.AddScoped<IRolloveredPortfoliosRepository, RolloveredPortfoliosRepository>();
builder.Services.AddScoped<ISasTablesRepository, SasTablesRepository>();
builder.Services.AddScoped<INonXtgPortfolioRepository, NonXtgPortfolioRepository>();
builder.Services.AddScoped<IRejectedXtgPortfolioRepository, RejectedXtgPortfolioRepository>();
builder.Services.AddScoped<IFunctionalRejectionRepository, FunctionalRejectionRepository>();
builder.Services.AddScoped<IFeedOutExtractionRepository, FeedOutExtractionRepository>();
builder.Services.AddScoped<IFutureCashRepository, FutureCashRepository>();
builder.Services.AddScoped<IFactPvCaConsistencyRepository, FactPvCaConsistencyRepository>();
builder.Services.AddScoped<IMultipleFeedVersionRepository, MultipleFeedVersionRepository>();
builder.Services.AddScoped<IDailyBalanceRepository, DailyBalanceRepository>();
builder.Services.AddScoped<IAdjustmentsRepository, AdjustmentsRepository>();
builder.Services.AddScoped<IPricingRepository, PricingRepository>();
builder.Services.AddScoped<IReverseConsoFileRepository, ReverseConsoFileRepository>();
builder.Services.AddScoped<IPublicationConsistencyRepository, PublicationConsistencyRepository>();
builder.Services.AddScoped<IJvBalanceConsistencyRepository, JvBalanceConsistencyRepository>();
builder.Services.AddScoped<IMissingWorkflowCheckRepository, MissingWorkflowCheckRepository>();
builder.Services.AddScoped<IPrecalcMonitoringRepository, PrecalcMonitoringRepository>();
builder.Services.AddScoped<IVrdbStatusRepository, VrdbStatusRepository>();
builder.Services.AddScoped<IReplayFlowRepository, ReplayFlowRepository>();
builder.Services.AddScoped<IUamAuthorizationRepository, UamAuthorizationRepository>();
builder.Services.AddScoped<IMonitoringJobExecutor, BatchStatusMonitoringJobExecutor>();
builder.Services.AddScoped<IMonitoringJobExecutor, DataValidationMonitoringJobExecutor>();
builder.Services.AddScoped<IMonitoringJobExecutor, FunctionalRejectionMonitoringJobExecutor>();
builder.Services.AddScoped<AuthorizationFeedbackState>();
builder.Services.AddScoped<PnlDateState>();
builder.Services.AddScoped<DataValidationNavAlertState>();
builder.Services.AddScoped<FunctionalRejectionNavAlertState>();
builder.Services.AddScoped<FunctionalRejectionMenuState>();
builder.Services.AddScoped<JvCalculationNavAlertState>();
builder.Services.AddScoped<ReplayFlowsNavAlertState>();
builder.Services.AddScoped<DatabaseSpaceNavAlertState>();
builder.Services.AddScoped<IAuthorizationHandler, UamPermissionHandler>();
builder.Services.AddSingleton<IDeploymentCheckService, DeploymentCheckService>();
builder.Services.AddSingleton<StartupDiagnosticsState>();
builder.Services.AddSingleton<ReplayFlowProcessingQueue>();
builder.Services.AddHostedService<ReplayFlowProcessingService>();
builder.Services.AddSingleton<StartupJobRecoveryService>();
builder.Services.AddHostedService(static serviceProvider => serviceProvider.GetRequiredService<StartupJobRecoveryService>());
builder.Services.AddSingleton<JobDiagnosticsService>();
builder.Services.AddHostedService<JvCalculationProcessingService>();
builder.Services
    .RegisterMonitoringProcessor<DataValidationMonitoringJobProcessingService>()
    .RegisterMonitoringProcessor<PricingMonitoringJobProcessingService>()
    .RegisterMonitoringProcessor<DailyBalanceMonitoringJobProcessingService>()
    .RegisterMonitoringProcessor<FunctionalRejectionMonitoringJobProcessingService>();

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

internal static class MonitoringProcessorServiceCollectionExtensions
{
    public static IServiceCollection RegisterMonitoringProcessor<T>(this IServiceCollection services)
        where T : MonitoringJobProcessingService
    {
        services.AddSingleton<T>();
        services.AddHostedService(static serviceProvider => serviceProvider.GetRequiredService<T>());
        services.AddSingleton<IMonitoringJobProcessor>(static serviceProvider => serviceProvider.GetRequiredService<T>());
        return services;
    }
}
