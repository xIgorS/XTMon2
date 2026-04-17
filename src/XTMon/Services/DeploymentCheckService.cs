using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;

namespace XTMon.Services;

public sealed class DeploymentCheckService : IDeploymentCheckService
{
    private readonly SqlConnectionFactory _connectionFactory;
    private readonly IReadOnlyList<(string ConnectionStringName, string StoredProcedure)> _checks;

    public DeploymentCheckService(
        SqlConnectionFactory connectionFactory,
        IConfiguration configuration,
        IOptions<MonitoringOptions> monitoringOptions,
        IOptions<ReplayFlowsOptions> replayFlowsOptions,
        IOptions<JvCalculationOptions> jvOptions,
        IOptions<BatchStatusOptions> batchStatusOptions,
        IOptions<ReferentialDataOptions> referentialDataOptions,
        IOptions<MarketDataOptions> marketDataOptions,
        IOptions<PricingFileReceptionOptions> pricingFileReceptionOptions,
        IOptions<OutOfScopePortfolioOptions> outOfScopePortfolioOptions,
        IOptions<MissingSogCheckOptions> missingSogCheckOptions,
        IOptions<AdjustmentLinksCheckOptions> adjustmentLinksCheckOptions,
        IOptions<ColumnStoreCheckOptions> columnStoreCheckOptions,
        IOptions<TradingVsFivrCheckOptions> tradingVsFivrCheckOptions,
        IOptions<MirrorizationOptions> mirrorizationOptions,
        IOptions<ResultTransferOptions> resultTransferOptions,
        IOptions<RolloveredPortfoliosOptions> rolloveredPortfoliosOptions,
        IOptions<SasTablesOptions> sasTablesOptions,
        IOptions<NonXtgPortfolioOptions> nonXtgPortfolioOptions,
        IOptions<RejectedXtgPortfolioOptions> rejectedXtgPortfolioOptions,
        IOptions<FeedOutExtractionOptions> feedOutExtractionOptions,
        IOptions<FutureCashOptions> futureCashOptions,
        IOptions<FactPvCaConsistencyOptions> factPvCaConsistencyOptions,
        IOptions<MultipleFeedVersionOptions> multipleFeedVersionOptions,
        IOptions<DailyBalanceOptions> dailyBalanceOptions,
        IOptions<AdjustmentsOptions> adjustmentsOptions,
        IOptions<PricingOptions> pricingOptions,
        IOptions<ReverseConsoFileOptions> reverseConsoFileOptions,
        IOptions<PublicationConsistencyOptions> publicationConsistencyOptions,
        IOptions<UamAuthorizationOptions> uamOptions)
    {
        _connectionFactory = connectionFactory;
        _checks = BuildChecks(
            configuration,
            monitoringOptions.Value,
            replayFlowsOptions.Value,
            jvOptions.Value,
            batchStatusOptions.Value,
            referentialDataOptions.Value,
            marketDataOptions.Value,
            pricingFileReceptionOptions.Value,
            outOfScopePortfolioOptions.Value,
            missingSogCheckOptions.Value,
            adjustmentLinksCheckOptions.Value,
            columnStoreCheckOptions.Value,
            tradingVsFivrCheckOptions.Value,
            mirrorizationOptions.Value,
            resultTransferOptions.Value,
            rolloveredPortfoliosOptions.Value,
            sasTablesOptions.Value,
            nonXtgPortfolioOptions.Value,
            rejectedXtgPortfolioOptions.Value,
            feedOutExtractionOptions.Value,
            futureCashOptions.Value,
            factPvCaConsistencyOptions.Value,
            multipleFeedVersionOptions.Value,
            dailyBalanceOptions.Value,
            adjustmentsOptions.Value,
            pricingOptions.Value,
            reverseConsoFileOptions.Value,
            publicationConsistencyOptions.Value,
            uamOptions.Value);
    }

    private static IReadOnlyList<(string, string)> BuildChecks(
        IConfiguration configuration,
        MonitoringOptions monitoring,
        ReplayFlowsOptions replay,
        JvCalculationOptions jv,
        BatchStatusOptions batchStatus,
        ReferentialDataOptions referentialData,
        MarketDataOptions marketData,
        PricingFileReceptionOptions pricingFileReception,
        OutOfScopePortfolioOptions outOfScopePortfolio,
        MissingSogCheckOptions missingSogCheck,
        AdjustmentLinksCheckOptions adjustmentLinksCheck,
        ColumnStoreCheckOptions columnStoreCheck,
        TradingVsFivrCheckOptions tradingVsFivrCheck,
        MirrorizationOptions mirrorization,
        ResultTransferOptions resultTransfer,
        RolloveredPortfoliosOptions rolloveredPortfolios,
        SasTablesOptions sasTables,
        NonXtgPortfolioOptions nonXtgPortfolio,
        RejectedXtgPortfolioOptions rejectedXtgPortfolio,
        FeedOutExtractionOptions feedOutExtraction,
        FutureCashOptions futureCash,
        FactPvCaConsistencyOptions factPvCaConsistency,
        MultipleFeedVersionOptions multipleFeedVersion,
        DailyBalanceOptions dailyBalance,
        AdjustmentsOptions adjustments,
        PricingOptions pricing,
        ReverseConsoFileOptions reverseConsoFile,
        PublicationConsistencyOptions publicationConsistency,
        UamAuthorizationOptions uam)
    {
        var checks = new List<(string, string)>
        {
            // Monitoring
            (monitoring.ConnectionStringName, monitoring.DbSizePlusDiskStoredProcedure),
            (monitoring.ConnectionStringName, monitoring.DbBackupsStoredProcedure),

            // Logging stored procedure (configured outside of the typed options)
            (
                configuration["StoredProcedureLogging:ConnectionStringName"] ?? "LogFiAlmt",
                configuration["StoredProcedureLogging:StoredProcedure"] ?? "monitoring.UspInsertAPSActionsLog"
            ),

            // Replay flows
            (replay.ConnectionStringName, replay.GetFailedFlowsStoredProcedure),
            (replay.ConnectionStringName, replay.ReplayFlowsStoredProcedure),
            (replay.ConnectionStringName, replay.ProcessReplayFlowsStoredProcedure),
            (replay.ConnectionStringName, replay.GetReplayFlowStatusStoredProcedure),
            (replay.ConnectionStringName, replay.GetReplayFlowProcessStatusStoredProcedure),

            // JV Calculation — PnlDates
            (jv.PnlDatesConnectionStringName, jv.GetPnlDatesStoredProcedure),

            // JV Calculation — check (Publication DB)
            (jv.PublicationConnectionStringName, jv.CheckJvCalculationStoredProcedure),

            // JV Calculation — fix (Publication DB)
            (jv.PublicationConnectionStringName, jv.FixJvCalculationStoredProcedure),

            // JV Calculation — job orchestration (LogFiAlmt)
            (jv.JobConnectionStringName, jv.JobEnqueueStoredProcedure),
            (jv.JobConnectionStringName, jv.JobTakeNextStoredProcedure),
            (jv.JobConnectionStringName, jv.JobHeartbeatStoredProcedure),
            (jv.JobConnectionStringName, jv.JobSaveResultStoredProcedure),
            (jv.JobConnectionStringName, jv.JobMarkCompletedStoredProcedure),
            (jv.JobConnectionStringName, jv.JobMarkFailedStoredProcedure),
            (jv.JobConnectionStringName, jv.JobGetByIdStoredProcedure),
            (jv.JobConnectionStringName, jv.JobGetLatestStoredProcedure),
            (jv.JobConnectionStringName, jv.JobExpireStaleStoredProcedure),

            // Data validation
            (batchStatus.ConnectionStringName, batchStatus.CheckBatchStatusStoredProcedure),
            (referentialData.ConnectionStringName, referentialData.CheckReferentialDataStoredProcedure),
            (marketData.ConnectionStringName, marketData.MarketDataStoredProcedure),
            (pricingFileReception.ConnectionStringName, pricingFileReception.PricingFileReceptionStoredProcedure),
            (pricingFileReception.ConnectionStringName, pricingFileReception.GetAllSourceSystemsStoredProcedure),
            (outOfScopePortfolio.ConnectionStringName, outOfScopePortfolio.OutOfScopePortfolioStoredProcedure),
            (missingSogCheck.ConnectionStringName, missingSogCheck.MissingSogCheckStoredProcedure),
            (adjustmentLinksCheck.ConnectionStringName, adjustmentLinksCheck.AdjustmentLinksCheckStoredProcedure),
            (columnStoreCheck.ConnectionStringName, columnStoreCheck.ColumnStoreCheckStoredProcedure),
            (tradingVsFivrCheck.ConnectionStringName, tradingVsFivrCheck.TradingVsFivrCheckStoredProcedure),
            (mirrorization.ConnectionStringName, mirrorization.MirrorizationStoredProcedure),
            (resultTransfer.ConnectionStringName, resultTransfer.ResultTransferStoredProcedure),
            (rolloveredPortfolios.ConnectionStringName, rolloveredPortfolios.RolloveredPortfoliosStoredProcedure),
            (sasTables.ConnectionStringName, sasTables.SasTablesStoredProcedure),
            (nonXtgPortfolio.ConnectionStringName, nonXtgPortfolio.NonXtgPortfolioStoredProcedure),
            (rejectedXtgPortfolio.ConnectionStringName, rejectedXtgPortfolio.RejectedXtgPortfolioStoredProcedure),
            (feedOutExtraction.ConnectionStringName, feedOutExtraction.FeedOutExtractionStoredProcedure),
            (futureCash.ConnectionStringName, futureCash.FutureCashStoredProcedure),
            (factPvCaConsistency.ConnectionStringName, factPvCaConsistency.FactPvCaConsistencyStoredProcedure),
            (multipleFeedVersion.ConnectionStringName, multipleFeedVersion.MultipleFeedVersionStoredProcedure),
            (dailyBalance.ConnectionStringName, dailyBalance.DailyBalanceStoredProcedure),
            (dailyBalance.ConnectionStringName, dailyBalance.GetAllSourceSystemsStoredProcedure),
            (adjustments.ConnectionStringName, adjustments.AdjustmentsStoredProcedure),
            (adjustments.ConnectionStringName, adjustments.GetAllSourceSystemsStoredProcedure),
            (pricing.ConnectionStringName, pricing.PricingStoredProcedure),
            (pricing.ConnectionStringName, pricing.GetAllSourceSystemsStoredProcedure),
            (reverseConsoFile.ConnectionStringName, reverseConsoFile.ReverseConsoFileStoredProcedure),
            (reverseConsoFile.ConnectionStringName, reverseConsoFile.GetAllSourceSystemsStoredProcedure),
            (publicationConsistency.ConnectionStringName, publicationConsistency.PublicationConsistencyStoredProcedure),

            // UAM authorization
            (uam.ConnectionStringName, uam.GetAdminUserStoredProcedure),
        };

        // Deduplicate: same connection + same SP (e.g. if logging conn = monitoring conn and same SP)
        return checks
            .Distinct(StoredProcCheckComparer.Instance)
            .ToList()
            .AsReadOnly();
    }

    public async Task<DiagnosticsReport> RunCheckAsync(CancellationToken cancellationToken)
    {
        var groups = _checks
            .GroupBy(c => c.ConnectionStringName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var tasks = groups.Select(group =>
            CheckDatabaseAsync(
                group.Key,
                group.Select(g => g.StoredProcedure).ToList(),
                cancellationToken));

        var results = await Task.WhenAll(tasks);

        // Order by connection string name for consistent display
        return new DiagnosticsReport(DateTimeOffset.Now, results.OrderBy(r => r.ConnectionStringName).ToList());
    }

    private async Task<DatabaseCheckResult> CheckDatabaseAsync(
        string connectionStringName,
        IReadOnlyList<string> storedProcedures,
        CancellationToken cancellationToken)
    {
        var sw = Stopwatch.StartNew();
        SqlConnection connection;
        try
        {
            connection = _connectionFactory.CreateConnection(connectionStringName);
        }
        catch (Exception ex)
        {
            sw.Stop();
            return new DatabaseCheckResult(connectionStringName, null, null, false, sw.Elapsed, ex.Message,
                storedProcedures.Select(sp => new StoredProcedureCheckResult(sp, false, [], null)).ToList());
        }

        await using (connection)
        {
            try
            {
                using var connectCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                connectCts.CancelAfter(TimeSpan.FromSeconds(10));
                await connection.OpenAsync(connectCts.Token);
            }
            catch (Exception ex)
            {
                sw.Stop();
                return new DatabaseCheckResult(connectionStringName, null, null, false, sw.Elapsed, ex.Message,
                    storedProcedures.Select(sp => new StoredProcedureCheckResult(sp, false, [], null)).ToList());
            }

            sw.Stop();
            var serverName = connection.DataSource;
            var databaseName = connection.Database;

            var spResults = new List<StoredProcedureCheckResult>(storedProcedures.Count);
            foreach (var sp in storedProcedures)
            {
                spResults.Add(await CheckStoredProcedureAsync(connection, sp, cancellationToken));
            }

            return new DatabaseCheckResult(connectionStringName, serverName, databaseName, true, sw.Elapsed, null, spResults);
        }
    }

    private static async Task<StoredProcedureCheckResult> CheckStoredProcedureAsync(
        SqlConnection connection,
        string fullName,
        CancellationToken cancellationToken)
    {
        if (!TryParseProcedureName(fullName, out var schema, out var procName))
        {
            return new StoredProcedureCheckResult(fullName, false, [],
                $"Cannot parse stored procedure name '{fullName}': expected schema.ProcedureName format.");
        }

        try
        {
            using var existsCmd = connection.CreateCommand();
            existsCmd.CommandText =
                "SELECT COUNT(1) FROM sys.objects o " +
                "INNER JOIN sys.schemas s ON o.schema_id = s.schema_id " +
                "WHERE o.type IN ('P','PC') AND s.name = @schema AND o.name = @proc";
            existsCmd.Parameters.AddWithValue("@schema", schema);
            existsCmd.Parameters.AddWithValue("@proc", procName);
            existsCmd.CommandTimeout = 10;

            var exists = (int)(await existsCmd.ExecuteScalarAsync(cancellationToken))! > 0;
            if (!exists)
            {
                return new StoredProcedureCheckResult(fullName, false, [], null);
            }

            using var paramsCmd = connection.CreateCommand();
            paramsCmd.CommandText =
                "SELECT p.name, t.name AS type_name, p.is_output " +
                "FROM sys.parameters p " +
                "INNER JOIN sys.objects o ON p.object_id = o.object_id " +
                "INNER JOIN sys.schemas s ON o.schema_id = s.schema_id " +
                "INNER JOIN sys.types t ON p.user_type_id = t.user_type_id " +
                "WHERE s.name = @schema AND o.name = @proc AND p.parameter_id > 0 " +
                "ORDER BY p.parameter_id";
            paramsCmd.Parameters.AddWithValue("@schema", schema);
            paramsCmd.Parameters.AddWithValue("@proc", procName);
            paramsCmd.CommandTimeout = 10;

            using var reader = await paramsCmd.ExecuteReaderAsync(cancellationToken);
            var parameters = new List<StoredProcedureParameterInfo>();
            while (await reader.ReadAsync(cancellationToken))
            {
                parameters.Add(new StoredProcedureParameterInfo(
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetBoolean(2)));
            }

            return new StoredProcedureCheckResult(fullName, true, parameters, null);
        }
        catch (Exception ex)
        {
            return new StoredProcedureCheckResult(fullName, false, [], ex.Message);
        }
    }

    private static bool TryParseProcedureName(string fullName, out string schema, out string procName)
    {
        schema = string.Empty;
        procName = string.Empty;

        if (string.IsNullOrWhiteSpace(fullName))
        {
            return false;
        }

        // Strip SQL brackets: [schema].[name] → schema.name
        var stripped = fullName.Replace("[", string.Empty, StringComparison.Ordinal)
                               .Replace("]", string.Empty, StringComparison.Ordinal);

        var dotIndex = stripped.IndexOf('.', StringComparison.Ordinal);
        if (dotIndex < 0)
        {
            return false;
        }

        schema = stripped[..dotIndex].Trim();
        procName = stripped[(dotIndex + 1)..].Trim();
        return !string.IsNullOrWhiteSpace(schema) && !string.IsNullOrWhiteSpace(procName);
    }

    private sealed class StoredProcCheckComparer : IEqualityComparer<(string ConnectionStringName, string StoredProcedure)>
    {
        public static readonly StoredProcCheckComparer Instance = new();

        public bool Equals((string ConnectionStringName, string StoredProcedure) x, (string ConnectionStringName, string StoredProcedure) y) =>
            string.Equals(x.ConnectionStringName, y.ConnectionStringName, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(x.StoredProcedure, y.StoredProcedure, StringComparison.OrdinalIgnoreCase);

        public int GetHashCode((string ConnectionStringName, string StoredProcedure) obj) =>
            HashCode.Combine(
                obj.ConnectionStringName.ToUpperInvariant(),
                obj.StoredProcedure.ToUpperInvariant());
    }
}
