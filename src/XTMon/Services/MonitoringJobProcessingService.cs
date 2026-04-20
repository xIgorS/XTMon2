using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;

namespace XTMon.Services;

public sealed class MonitoringJobProcessingService : BackgroundService
{
    private static readonly TimeSpan DefaultIdleDelay = TimeSpan.FromSeconds(5);
    private const string StaleRunningJobErrorMessage = "Monitoring background job timed out while in Running status and was auto-failed.";

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<MonitoringJobProcessingService> _logger;
    private readonly MonitoringJobsOptions _options;
    private readonly TimeSpan _idleDelay;

    public MonitoringJobProcessingService(
        IServiceScopeFactory scopeFactory,
        IOptions<MonitoringJobsOptions> options,
        ILogger<MonitoringJobProcessingService> logger)
        : this(scopeFactory, options, logger, DefaultIdleDelay)
    {
    }

    internal MonitoringJobProcessingService(
        IServiceScopeFactory scopeFactory,
        IOptions<MonitoringJobsOptions> options,
        ILogger<MonitoringJobProcessingService> logger,
        TimeSpan idleDelay)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
        _options = options.Value;
        _idleDelay = idleDelay;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Monitoring job processing service started.");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                MonitoringJobRecord? job;

                try
                {
                    using var scope = _scopeFactory.CreateScope();
                    var repository = scope.ServiceProvider.GetRequiredService<IMonitoringJobRepository>();
                    var staleTimeout = TimeSpan.FromSeconds(_options.JobRunningStaleTimeoutSeconds);
                    var expiredCount = await repository.ExpireStaleRunningMonitoringJobsAsync(staleTimeout, StaleRunningJobErrorMessage, stoppingToken);
                    if (expiredCount > 0)
                    {
                        _logger.LogWarning("Marked {ExpiredCount} stale monitoring job(s) as failed.", expiredCount);
                    }

                    job = await repository.TryTakeNextMonitoringJobAsync(Environment.MachineName, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    LogProcessorException(ex, "processing loop poll");
                    _logger.LogError(AppLogEvents.MonitoringProcessorBackgroundFailed, ex, "Monitoring job processing loop encountered a transient error. Retrying after delay.");
                    await Task.Delay(_idleDelay, stoppingToken);
                    continue;
                }

                if (job is null)
                {
                    await Task.Delay(_idleDelay, stoppingToken);
                    continue;
                }

                await ProcessJobAsync(job, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Monitoring job processing service cancellation received.");
        }

        _logger.LogInformation("Monitoring job processing service stopped.");
    }

    private async Task ProcessJobAsync(MonitoringJobRecord job, CancellationToken cancellationToken)
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var repository = scope.ServiceProvider.GetRequiredService<IMonitoringJobRepository>();
            var executors = scope.ServiceProvider.GetServices<IMonitoringJobExecutor>();
            var executor = executors.FirstOrDefault(candidate => candidate.CanExecute(job))
                ?? throw new InvalidOperationException($"No monitoring executor is registered for category '{job.Category}' and submenu '{job.SubmenuKey}'.");

            _logger.LogInformation("Processing monitoring job {JobId} for {Category}/{SubmenuKey}, pnl date {PnlDate}.", job.JobId, job.Category, job.SubmenuKey, job.PnlDate);

            await repository.HeartbeatMonitoringJobAsync(job.JobId, cancellationToken);
            var payload = await executor.ExecuteAsync(job, cancellationToken);
            await repository.SaveMonitoringJobResultAsync(job.JobId, payload, cancellationToken);
            await repository.MarkMonitoringJobCompletedAsync(job.JobId, cancellationToken);

            _logger.LogInformation("Monitoring job {JobId} completed.", job.JobId);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            LogProcessorException(ex, $"job {job.JobId}");
            _logger.LogError(AppLogEvents.MonitoringProcessorBackgroundFailed, ex, "Monitoring job {JobId} failed.", job.JobId);

            for (var attempt = 1; attempt <= 2; attempt++)
            {
                try
                {
                    using var scope = _scopeFactory.CreateScope();
                    var repository = scope.ServiceProvider.GetRequiredService<IMonitoringJobRepository>();
                    await repository.MarkMonitoringJobFailedAsync(job.JobId, ex.Message, CancellationToken.None);
                    break;
                }
                catch (Exception markFailedException)
                {
                    _logger.LogError(AppLogEvents.MonitoringProcessorBackgroundFailed, markFailedException,
                        "Failed to mark monitoring job {JobId} as failed (attempt {Attempt}/2).", job.JobId, attempt);
                    if (attempt < 2)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(2));
                    }
                }
            }
        }
    }

    private void LogProcessorException(Exception ex, string context)
    {
        if (ex is not SqlException sqlException)
        {
            return;
        }

        if (SqlDataHelper.IsSqlTimeout(sqlException))
        {
            _logger.LogError(AppLogEvents.MonitoringProcessorSqlTimeout, sqlException,
                "Monitoring processor SQL timeout in {Context}. SQL Number {SqlNumber}, State {SqlState}, Class {SqlClass}.",
                context,
                sqlException.Number,
                sqlException.State,
                sqlException.Class);
            return;
        }

        if (SqlDataHelper.IsSqlConnectionFailure(sqlException))
        {
            _logger.LogError(AppLogEvents.MonitoringProcessorConnectionFailed, sqlException,
                "Monitoring processor SQL connection failure in {Context}. SQL Number {SqlNumber}, State {SqlState}, Class {SqlClass}.",
                context,
                sqlException.Number,
                sqlException.State,
                sqlException.Class);
        }
    }
}