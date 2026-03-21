using XTMon.Models;
using XTMon.Options;
using Microsoft.Extensions.Options;
using Microsoft.Data.SqlClient;

namespace XTMon.Data;

public sealed class JvCalculationProcessingService : BackgroundService
{
    private static readonly TimeSpan IdleDelay = TimeSpan.FromSeconds(5);
    private const string StaleRunningJobErrorMessage = "JV background job timed out while in Running status and was auto-failed.";

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<JvCalculationProcessingService> _logger;
    private readonly JvCalculationOptions _jvCalculationOptions;

    public JvCalculationProcessingService(
        IServiceScopeFactory scopeFactory,
        IOptions<JvCalculationOptions> jvCalculationOptions,
        ILogger<JvCalculationProcessingService> logger)
    {
        _scopeFactory = scopeFactory;
        _jvCalculationOptions = jvCalculationOptions.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("JV calculation processing service started.");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                JvJobRecord? job;
                using (var scope = _scopeFactory.CreateScope())
                {
                    var repository = scope.ServiceProvider.GetRequiredService<IJvCalculationRepository>();

                    var staleTimeout = TimeSpan.FromSeconds(_jvCalculationOptions.JobRunningStaleTimeoutSeconds);
                    var expiredCount = await repository.ExpireStaleRunningJobsAsync(staleTimeout, StaleRunningJobErrorMessage, stoppingToken);
                    if (expiredCount > 0)
                    {
                        _logger.LogWarning("Marked {ExpiredCount} stale JV running job(s) as failed.", expiredCount);
                    }

                    job = await repository.TryTakeNextJvJobAsync(Environment.MachineName, stoppingToken);
                }

                if (job is null)
                {
                    await Task.Delay(IdleDelay, stoppingToken);
                    continue;
                }

                await ProcessJobAsync(job, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("JV calculation processing service cancellation received.");
        }
        catch (Exception ex)
        {
            LogProcessorException(ex, "processing loop");
            _logger.LogError(AppLogEvents.ReplayProcessorBackgroundFailed, ex, "JV calculation processing service terminated unexpectedly.");
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
            _logger.LogError(AppLogEvents.ReplayProcessorBackgroundFailed, ex, "JV job {JobId} failed.", job.JobId);
            try
            {
                using var scope = _scopeFactory.CreateScope();
                var repository = scope.ServiceProvider.GetRequiredService<IJvCalculationRepository>();
                await repository.MarkJvJobFailedAsync(job.JobId, ex.Message, CancellationToken.None);
            }
            catch (Exception markFailedException)
            {
                _logger.LogError(AppLogEvents.ReplayProcessorBackgroundFailed, markFailedException, "Failed to mark JV job {JobId} as failed.", job.JobId);
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
