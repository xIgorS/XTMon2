using XTMon.Helpers;
using XTMon.Models;
using XTMon.Repositories;

namespace XTMon.Services;

public sealed class BatchStatusMonitoringJobExecutor : IMonitoringJobExecutor
{
    private readonly IBatchStatusRepository _repository;

    public BatchStatusMonitoringJobExecutor(IBatchStatusRepository repository)
    {
        _repository = repository;
    }

    public bool CanExecute(MonitoringJobRecord job)
    {
        return string.Equals(job.Category, MonitoringJobHelper.DataValidationCategory, StringComparison.Ordinal)
            && string.Equals(job.SubmenuKey, MonitoringJobHelper.BatchStatusSubmenuKey, StringComparison.Ordinal);
    }

    public async Task<MonitoringJobResultPayload> ExecuteAsync(MonitoringJobRecord job, CancellationToken cancellationToken)
    {
        var table = await _repository.GetBatchStatusAsync(job.PnlDate, cancellationToken);
        return new MonitoringJobResultPayload(
            ParsedQuery: null,
            Table: table,
            MetadataJson: null);
    }
}