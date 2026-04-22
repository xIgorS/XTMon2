using XTMon.Helpers;
using XTMon.Repositories;

namespace XTMon.Services;

public sealed class BackgroundJobCancellationService : IBackgroundJobCancellationService
{
    public const string MonitoringJobCanceledMessage = "Monitoring background job was cancelled by user.";
    public const string JvJobCanceledMessage = "JV background job was cancelled by user.";

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly JobCancellationRegistry _jobCancellationRegistry;

    public BackgroundJobCancellationService(
        IServiceScopeFactory scopeFactory,
        JobCancellationRegistry jobCancellationRegistry)
    {
        _scopeFactory = scopeFactory;
        _jobCancellationRegistry = jobCancellationRegistry;
    }

    public async Task<bool> CancelMonitoringJobAsync(long jobId, CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IMonitoringJobRepository>();
        var job = await repository.GetMonitoringJobByIdAsync(jobId, cancellationToken);
        if (job is null || !MonitoringJobHelper.IsActiveStatus(job.Status))
        {
            return false;
        }

        await repository.MarkMonitoringJobFailedAsync(jobId, MonitoringJobCanceledMessage, cancellationToken);
        _jobCancellationRegistry.CancelMonitoringJob(jobId);
        return true;
    }

    public async Task<bool> CancelJvJobAsync(long jobId, CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IJvCalculationRepository>();
        var job = await repository.GetJvJobByIdAsync(jobId, cancellationToken);
        if (job is null || !MonitoringJobHelper.IsActiveStatus(job.Status))
        {
            return false;
        }

        await repository.MarkJvJobFailedAsync(jobId, JvJobCanceledMessage, cancellationToken);
        _jobCancellationRegistry.CancelJvJob(jobId);
        return true;
    }
}