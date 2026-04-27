using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;

namespace XTMon.Services;

public sealed class FunctionalRejectionMonitoringJobExecutor : IMonitoringJobExecutor
{
    private readonly IFunctionalRejectionRepository _repository;
    private readonly MonitoringJobsOptions _monitoringJobsOptions;

    public FunctionalRejectionMonitoringJobExecutor(
        IFunctionalRejectionRepository repository,
        IOptions<MonitoringJobsOptions> monitoringJobsOptions)
    {
        _repository = repository;
        _monitoringJobsOptions = monitoringJobsOptions.Value;
    }

    public bool CanExecute(MonitoringJobRecord job)
    {
        return string.Equals(job.Category, MonitoringJobHelper.FunctionalRejectionCategory, StringComparison.Ordinal);
    }

    public async Task<MonitoringJobResultPayload> ExecuteAsync(MonitoringJobRecord job, CancellationToken cancellationToken)
    {
        var parameters = MonitoringJobHelper.DeserializeParameters<FunctionalRejectionJobParameters>(job.ParametersJson)
            ?? throw new InvalidOperationException($"Functional Rejection job {job.JobId} is missing execution parameters.");

        var result = await _repository.GetTechnicalRejectAsync(
            job.PnlDate,
            parameters.BusinessDataTypeId,
            parameters.DbConnection,
            parameters.SourceSystemName,
            cancellationToken);

        var fullTable = new MonitoringTableResult(
            result.Columns.Select(static column => column.Name).ToArray(),
            result.Rows);
        var preview = MonitoringJobHelper.TruncateRows(
            fullTable,
            _monitoringJobsOptions.MaxPersistedRows,
            out var totalRowCount,
            out _);
        var metadataJson = MonitoringJobHelper.BuildPersistMetadataJson(
            totalRowCount,
            preview.Rows.Count,
            MonitoringJobHelper.SerializeTechnicalRejectColumns(result.Columns, hasAlerts: result.Rows.Count > 0));

        return new MonitoringJobResultPayload(
            ParsedQuery: result.ParsedQuery,
            Table: preview,
            MetadataJson: metadataJson,
            FullResultCsvGzip: MonitoringJobHelper.BuildFullResultCsvGzip(fullTable, cancellationToken));
    }
}