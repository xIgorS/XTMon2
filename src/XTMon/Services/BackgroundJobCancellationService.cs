using XTMon.Helpers;
using XTMon.Repositories;

namespace XTMon.Services;

public sealed class BackgroundJobCancellationService : IBackgroundJobCancellationService
{
    public const string MonitoringJobCanceledMessage = "Monitoring background job was cancelled by user.";
    public const string JvJobCanceledMessage = "JV background job was cancelled by user.";
    private static readonly TimeSpan VerificationRetryDelay = TimeSpan.FromMilliseconds(100);

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly JobCancellationRegistry _jobCancellationRegistry;

    public BackgroundJobCancellationService(
        IServiceScopeFactory scopeFactory,
        JobCancellationRegistry jobCancellationRegistry)
    {
        _scopeFactory = scopeFactory;
        _jobCancellationRegistry = jobCancellationRegistry;
    }

    public async Task<BackgroundJobCancellationResult> CancelMonitoringJobAsync(long jobId, CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IMonitoringJobRepository>();
        var job = await repository.GetMonitoringJobByIdAsync(jobId, cancellationToken);
        if (job is null || !MonitoringJobHelper.IsActiveStatus(job.Status))
        {
            return BackgroundJobCancellationResult.AlreadyInactive;
        }

        await repository.MarkMonitoringJobCancelledAsync(jobId, MonitoringJobCanceledMessage, cancellationToken);
        _jobCancellationRegistry.CancelMonitoringJob(jobId);
        return await VerifyMonitoringJobCancellationAsync(repository, jobId, cancellationToken);
    }

    public async Task<BackgroundJobCancellationResult> CancelJvJobAsync(long jobId, CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IJvCalculationRepository>();
        var job = await repository.GetJvJobByIdAsync(jobId, cancellationToken);
        if (job is null || !MonitoringJobHelper.IsActiveStatus(job.Status))
        {
            return BackgroundJobCancellationResult.AlreadyInactive;
        }

        await repository.MarkJvJobCancelledAsync(jobId, JvJobCanceledMessage, cancellationToken);
        _jobCancellationRegistry.CancelJvJob(jobId);
        return await VerifyJvJobCancellationAsync(repository, jobId, cancellationToken);
    }

    public async Task<BackgroundJobBulkCancellationResult> CancelAllBackgroundJobsAsync(CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var monitoringRepository = scope.ServiceProvider.GetRequiredService<IMonitoringJobRepository>();
        var jvRepository = scope.ServiceProvider.GetRequiredService<IJvCalculationRepository>();

        var monitoringWorkersCancellationRequested = _jobCancellationRegistry.CancelAllMonitoringJobs();
        var jvWorkersCancellationRequested = _jobCancellationRegistry.CancelAllJvJobs();

        var monitoringJobsCancelled = await monitoringRepository.CancelActiveMonitoringJobsAsync(MonitoringJobCanceledMessage, cancellationToken);
        var jvJobsCancelled = await jvRepository.CancelActiveJvJobsAsync(JvJobCanceledMessage, cancellationToken);

        var activeMonitoringJobsRemaining = await monitoringRepository.CountActiveMonitoringJobsAsync(cancellationToken);
        var activeJvJobsRemaining = await jvRepository.CountActiveJvJobsAsync(cancellationToken);

        return new BackgroundJobBulkCancellationResult(
            monitoringJobsCancelled,
            jvJobsCancelled,
            monitoringWorkersCancellationRequested,
            jvWorkersCancellationRequested,
            activeMonitoringJobsRemaining,
            activeJvJobsRemaining);
    }

    private static async Task<BackgroundJobCancellationResult> VerifyMonitoringJobCancellationAsync(
        IMonitoringJobRepository repository,
        long jobId,
        CancellationToken cancellationToken)
    {
        for (var attempt = 0; attempt < 3; attempt++)
        {
            var job = await repository.GetMonitoringJobByIdAsync(jobId, cancellationToken);
            if (job is null || !MonitoringJobHelper.IsActiveStatus(job.Status))
            {
                return BackgroundJobCancellationResult.Confirmed;
            }

            if (attempt < 2)
            {
                await Task.Delay(VerificationRetryDelay, cancellationToken);
            }
        }

        return BackgroundJobCancellationResult.Pending;
    }

    private static async Task<BackgroundJobCancellationResult> VerifyJvJobCancellationAsync(
        IJvCalculationRepository repository,
        long jobId,
        CancellationToken cancellationToken)
    {
        for (var attempt = 0; attempt < 3; attempt++)
        {
            var job = await repository.GetJvJobByIdAsync(jobId, cancellationToken);
            if (job is null || !MonitoringJobHelper.IsActiveStatus(job.Status))
            {
                return BackgroundJobCancellationResult.Confirmed;
            }

            if (attempt < 2)
            {
                await Task.Delay(VerificationRetryDelay, cancellationToken);
            }
        }

        return BackgroundJobCancellationResult.Pending;
    }
}