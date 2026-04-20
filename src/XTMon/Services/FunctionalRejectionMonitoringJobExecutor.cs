using XTMon.Helpers;
using XTMon.Models;
using XTMon.Repositories;

namespace XTMon.Services;

public sealed class FunctionalRejectionMonitoringJobExecutor : IMonitoringJobExecutor
{
    private readonly IFunctionalRejectionRepository _repository;

    public FunctionalRejectionMonitoringJobExecutor(IFunctionalRejectionRepository repository)
    {
        _repository = repository;
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

        return new MonitoringJobResultPayload(
            ParsedQuery: result.ParsedQuery,
            Table: new MonitoringTableResult(
                result.Columns.Select(static column => column.Name).ToArray(),
                result.Rows),
            MetadataJson: MonitoringJobHelper.SerializeTechnicalRejectColumns(result.Columns));
    }
}