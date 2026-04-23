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

        await repository.MarkMonitoringJobFailedAsync(jobId, MonitoringJobCanceledMessage, cancellationToken);
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

        await repository.MarkJvJobFailedAsync(jobId, JvJobCanceledMessage, cancellationToken);
        _jobCancellationRegistry.CancelJvJob(jobId);
        return await VerifyJvJobCancellationAsync(repository, jobId, cancellationToken);
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