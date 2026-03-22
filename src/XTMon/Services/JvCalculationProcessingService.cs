using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;
using Microsoft.Extensions.Options;
using Microsoft.Data.SqlClient;

namespace XTMon.Services;

public sealed class JvCalculationProcessingService : BackgroundService
{
    private static readonly TimeSpan DefaultIdleDelay = TimeSpan.FromSeconds(5);
    private const string StaleRunningJobErrorMessage = "JV background job timed out while in Running status and was auto-failed.";

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<JvCalculationProcessingService> _logger;
    private readonly JvCalculationOptions _jvCalculationOptions;
    private readonly TimeSpan _idleDelay;

    public JvCalculationProcessingService(
        IServiceScopeFactory scopeFactory,
        IOptions<JvCalculationOptions> jvCalculationOptions,
        ILogger<JvCalculationProcessingService> logger)
        : this(scopeFactory, jvCalculationOptions, logger, DefaultIdleDelay)
    {
    }

    internal JvCalculationProcessingService(
        IServiceScopeFactory scopeFactory,
        IOptions<JvCalculationOptions> jvCalculationOptions,
        ILogger<JvCalculationProcessingService> logger,
        TimeSpan idleDelay)
    {
        _scopeFactory = scopeFactory;
        _jvCalculationOptions = jvCalculationOptions.Value;
        _logger = logger;
        _idleDelay = idleDelay;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("JV calculation processing service started.");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                JvJobRecord? job;
                try
                {
                    using var scope = _scopeFactory.CreateScope();
                    var repository = scope.ServiceProvider.GetRequiredService<IJvCalculationRepository>();

                    var staleTimeout = TimeSpan.FromSeconds(_jvCalculationOptions.JobRunningStaleTimeoutSeconds);
                    var expiredCount = await repository.ExpireStaleRunningJobsAsync(staleTimeout, StaleRunningJobErrorMessage, stoppingToken);
                    if (expiredCount > 0)
                    {
                        _logger.LogWarning("Marked {ExpiredCount} stale JV running job(s) as failed.", expiredCount);
                    }

                    job = await repository.TryTakeNextJvJobAsync(Environment.MachineName, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    LogProcessorException(ex, "processing loop poll");
                    _logger.LogError(AppLogEvents.JvProcessorBackgroundFailed, ex, "JV calculation processing loop encountered a transient error. Retrying after delay.");
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
            _logger.LogInformation("JV calculation processing service cancellation received.");
        }

        _logger.LogInformation("JV calculation processing service stopped.");
    }

    private async Task ProcessJobAsync(JvJobRecord job, CancellationToken cancellationToken)
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var repository = scope.ServiceProvider.GetRequiredService<IJvCalculationRepository>();

            _logger.LogInformation("Processing JV job {JobId}, request {RequestType}, pnl date {PnlDate}.", job.JobId, job.RequestType, job.PnlDate);

            await repository.HeartbeatJvJobAsync(job.JobId, cancellationToken);

            string? queryFix = null;
            if (string.Equals(job.RequestType, "FixAndCheck", StringComparison.OrdinalIgnoreCase))
            {
                queryFix = await repository.FixJvCalculationAsync(job.PnlDate, executeCatchup: true, cancellationToken);
                await repository.HeartbeatJvJobAsync(job.JobId, cancellationToken);
            }

            var checkResult = await repository.CheckJvCalculationAsync(job.PnlDate, cancellationToken);

            await repository.SaveJvJobResultAsync(job.JobId, checkResult.ParsedQuery, queryFix, checkResult.Table, cancellationToken);
            await repository.MarkJvJobCompletedAsync(job.JobId, cancellationToken);

            _logger.LogInformation("JV job {JobId} completed.", job.JobId);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            LogProcessorException(ex, $"job {job.JobId}");
            _logger.LogError(AppLogEvents.JvProcessorBackgroundFailed, ex, "JV job {JobId} failed.", job.JobId);
            for (var attempt = 1; attempt <= 2; attempt++)
            {
                try
                {
                    using var scope = _scopeFactory.CreateScope();
                    var repository = scope.ServiceProvider.GetRequiredService<IJvCalculationRepository>();
                    await repository.MarkJvJobFailedAsync(job.JobId, ex.Message, CancellationToken.None);
                    break;
                }
                catch (Exception markFailedException)
                {
                    _logger.LogError(AppLogEvents.JvProcessorBackgroundFailed, markFailedException,
                        "Failed to mark JV job {JobId} as failed (attempt {Attempt}/2).", job.JobId, attempt);
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
            _logger.LogError(AppLogEvents.JvProcessorSqlTimeout, sqlException,
                "JV processor SQL timeout in {Context}. SQL Number {SqlNumber}, State {SqlState}, Class {SqlClass}.",
                context,
                sqlException.Number,
                sqlException.State,
                sqlException.Class);
            return;
        }

        if (SqlDataHelper.IsSqlConnectionFailure(sqlException))
        {
            _logger.LogError(AppLogEvents.JvProcessorConnectionFailed, sqlException,
                "JV processor SQL connection failure in {Context}. SQL Number {SqlNumber}, State {SqlState}, Class {SqlClass}.",
                context,
                sqlException.Number,
                sqlException.State,
                sqlException.Class);
        }
    }

}
