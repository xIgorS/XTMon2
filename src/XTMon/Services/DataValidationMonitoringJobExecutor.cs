using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;

namespace XTMon.Services;

public sealed class DataValidationMonitoringJobExecutor : IMonitoringJobExecutor
{
    private readonly IServiceProvider _serviceProvider;

    public DataValidationMonitoringJobExecutor(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    public bool CanExecute(MonitoringJobRecord job)
    {
        return string.Equals(job.Category, MonitoringJobHelper.DataValidationCategory, StringComparison.Ordinal);
    }

    public Task<MonitoringJobResultPayload> ExecuteAsync(MonitoringJobRecord job, CancellationToken cancellationToken)
    {
        return job.SubmenuKey switch
        {
            "batch-status" => ExecuteBatchStatusAsync(job, cancellationToken),
            "referential-data" => ExecuteResultAsync<IReferentialDataRepository, ReferentialDataResult>(repository => repository.GetReferentialDataAsync(job.PnlDate, cancellationToken)),
            "market-data" => ExecuteResultAsync<IMarketDataRepository, MarketDataResult>(repository => repository.GetMarketDataAsync(job.PnlDate, cancellationToken)),
            "out-of-scope-portfolio" => ExecuteResultAsync<IOutOfScopePortfolioRepository, OutOfScopePortfolioResult>(repository => repository.GetOutOfScopePortfolioAsync(job.PnlDate, cancellationToken)),
            "daily-balance" => ExecuteDailyBalanceAsync(job, cancellationToken),
            "adjustments" => ExecuteAdjustmentsAsync(job, cancellationToken),
            "pricing" => ExecutePricingAsync(job, cancellationToken),
            "pricing-file-reception" => ExecutePricingFileReceptionAsync(job, cancellationToken),
            "reverse-conso-file" => ExecuteReverseConsoFileAsync(job, cancellationToken),
            "missing-sog-check" => ExecuteResultAsync<IMissingSogCheckRepository, MissingSogCheckResult>(repository => repository.GetMissingSogCheckAsync(job.PnlDate, cancellationToken)),
            "adjustment-links-check" => ExecuteResultAsync<IAdjustmentLinksCheckRepository, AdjustmentLinksCheckResult>(repository => repository.GetAdjustmentLinksCheckAsync(job.PnlDate, cancellationToken)),
            "column-store-check" => ExecuteResultAsync<IColumnStoreCheckRepository, ColumnStoreCheckResult>(repository => repository.GetColumnStoreCheckAsync(job.PnlDate, cancellationToken)),
            "trading-vs-fivr-check" => ExecuteResultAsync<ITradingVsFivrCheckRepository, TradingVsFivrCheckResult>(repository => repository.GetTradingVsFivrCheckAsync(job.PnlDate, cancellationToken)),
            "mirrorization" => ExecuteResultAsync<IMirrorizationRepository, MirrorizationResult>(repository => repository.GetMirrorizationAsync(job.PnlDate, cancellationToken)),
            "result-transfer" => ExecuteResultAsync<IResultTransferRepository, ResultTransferResult>(repository => repository.GetResultTransferAsync(job.PnlDate, cancellationToken)),
            "rollovered-portfolios" => ExecuteResultAsync<IRolloveredPortfoliosRepository, RolloveredPortfoliosResult>(repository => repository.GetRolloveredPortfoliosAsync(job.PnlDate, cancellationToken)),
            "sas-tables" => ExecuteResultAsync<ISasTablesRepository, SasTablesResult>(repository => repository.GetSasTablesAsync(job.PnlDate, cancellationToken)),
            "non-xtg-portfolio" => ExecuteResultAsync<INonXtgPortfolioRepository, NonXtgPortfolioResult>(repository => repository.GetNonXtgPortfolioAsync(job.PnlDate, cancellationToken)),
            "rejected-xtg-portfolio" => ExecuteResultAsync<IRejectedXtgPortfolioRepository, RejectedXtgPortfolioResult>(repository => repository.GetRejectedXtgPortfolioAsync(job.PnlDate, cancellationToken)),
            "feedout-extraction" => ExecuteResultAsync<IFeedOutExtractionRepository, FeedOutExtractionResult>(repository => repository.GetFeedOutExtractionAsync(job.PnlDate, cancellationToken)),
            "future-cash" => ExecuteResultAsync<IFutureCashRepository, FutureCashResult>(repository => repository.GetFutureCashAsync(job.PnlDate, cancellationToken)),
            "fact-pv-ca-consistency" => ExecuteResultAsync<IFactPvCaConsistencyRepository, FactPvCaConsistencyResult>(repository => repository.GetFactPvCaConsistencyAsync(job.PnlDate, cancellationToken)),
            "multiple-feed-version" => ExecuteResultAsync<IMultipleFeedVersionRepository, MultipleFeedVersionResult>(repository => repository.GetMultipleFeedVersionAsync(job.PnlDate, cancellationToken)),
            "publication-consistency" => ExecuteResultAsync<IPublicationConsistencyRepository, PublicationConsistencyResult>(repository => repository.GetPublicationConsistencyAsync(job.PnlDate, cancellationToken)),
            "jv-balance-consistency" => ExecuteJvBalanceConsistencyAsync(job, cancellationToken),
            "missing-workflow-check" => ExecuteResultAsync<IMissingWorkflowCheckRepository, MissingWorkflowCheckResult>(repository => repository.GetMissingWorkflowCheckAsync(job.PnlDate, cancellationToken)),
            "precalc-monitoring" => ExecuteResultAsync<IPrecalcMonitoringRepository, PrecalcMonitoringResult>(repository => repository.GetPrecalcMonitoringAsync(job.PnlDate, cancellationToken)),
            "vrdb-status" => ExecuteResultAsync<IVrdbStatusRepository, VrdbStatusResult>(repository => repository.GetVrdbStatusAsync(job.PnlDate, cancellationToken)),
            _ => throw new InvalidOperationException($"Unsupported data validation submenu '{job.SubmenuKey}'.")
        };
    }

    private async Task<MonitoringJobResultPayload> ExecuteBatchStatusAsync(MonitoringJobRecord job, CancellationToken cancellationToken)
    {
        var repository = _serviceProvider.GetRequiredService<IBatchStatusRepository>();
        var table = await repository.GetBatchStatusAsync(job.PnlDate, cancellationToken);
        return new MonitoringJobResultPayload(null, table, null);
    }

    private async Task<MonitoringJobResultPayload> ExecuteDailyBalanceAsync(MonitoringJobRecord job, CancellationToken cancellationToken)
    {
        var parameters = MonitoringJobHelper.DeserializeParameters<DataValidationJobParameters>(job.ParametersJson);
        return await ExecuteResultAsync<IDailyBalanceRepository, DailyBalanceResult>(
            repository => repository.GetDailyBalanceAsync(job.PnlDate, parameters?.SourceSystemCodes, cancellationToken));
    }

    private async Task<MonitoringJobResultPayload> ExecuteAdjustmentsAsync(MonitoringJobRecord job, CancellationToken cancellationToken)
    {
        var parameters = MonitoringJobHelper.DeserializeParameters<DataValidationJobParameters>(job.ParametersJson);
        return await ExecuteResultAsync<IAdjustmentsRepository, AdjustmentsResult>(
            repository => repository.GetAdjustmentsAsync(job.PnlDate, parameters?.SourceSystemCodes, cancellationToken));
    }

    private async Task<MonitoringJobResultPayload> ExecutePricingAsync(MonitoringJobRecord job, CancellationToken cancellationToken)
    {
        var parameters = MonitoringJobHelper.DeserializeParameters<DataValidationJobParameters>(job.ParametersJson);
        return await ExecuteResultAsync<IPricingRepository, PricingResult>(
            repository => repository.GetPricingAsync(job.PnlDate, parameters?.SourceSystemCodes, cancellationToken));
    }

    private async Task<MonitoringJobResultPayload> ExecutePricingFileReceptionAsync(MonitoringJobRecord job, CancellationToken cancellationToken)
    {
        var parameters = MonitoringJobHelper.DeserializeParameters<DataValidationJobParameters>(job.ParametersJson);
        return await ExecuteResultAsync<IPricingFileReceptionRepository, PricingFileReceptionResult>(
            repository => repository.GetPricingFileReceptionAsync(job.PnlDate, parameters?.TraceAllVersions ?? false, cancellationToken));
    }

    private async Task<MonitoringJobResultPayload> ExecuteReverseConsoFileAsync(MonitoringJobRecord job, CancellationToken cancellationToken)
    {
        var parameters = MonitoringJobHelper.DeserializeParameters<DataValidationJobParameters>(job.ParametersJson);
        return await ExecuteResultAsync<IReverseConsoFileRepository, ReverseConsoFileResult>(
            repository => repository.GetReverseConsoFileAsync(job.PnlDate, parameters?.SourceSystemCodes, cancellationToken));
    }

    private async Task<MonitoringJobResultPayload> ExecuteJvBalanceConsistencyAsync(MonitoringJobRecord job, CancellationToken cancellationToken)
    {
        var parameters = MonitoringJobHelper.DeserializeParameters<DataValidationJobParameters>(job.ParametersJson);
        var options = _serviceProvider.GetRequiredService<IOptions<JvBalanceConsistencyOptions>>();
        var precision = parameters?.Precision ?? options.Value.Precision;

        return await ExecuteResultAsync<IJvBalanceConsistencyRepository, JvBalanceConsistencyResult>(
            repository => repository.GetJvBalanceConsistencyAsync(job.PnlDate, precision, cancellationToken));
    }

    private async Task<MonitoringJobResultPayload> ExecuteResultAsync<TRepository, TResult>(Func<TRepository, Task<TResult>> callback)
        where TRepository : notnull
        where TResult : class
    {
        var repository = _serviceProvider.GetRequiredService<TRepository>();
        var response = await callback(repository);
        return CreatePayload(response);
    }

    private static MonitoringJobResultPayload CreatePayload(object response)
    {
        var responseType = response.GetType();
        var parsedQueryProperty = responseType.GetProperty("ParsedQuery")
            ?? throw new InvalidOperationException($"Response type '{responseType.Name}' does not expose ParsedQuery.");
        var tableProperty = responseType.GetProperty("Table")
            ?? throw new InvalidOperationException($"Response type '{responseType.Name}' does not expose Table.");

        var parsedQuery = parsedQueryProperty.GetValue(response) as string;
        var table = tableProperty.GetValue(response) as MonitoringTableResult;

        return new MonitoringJobResultPayload(parsedQuery, table, null);
    }
}