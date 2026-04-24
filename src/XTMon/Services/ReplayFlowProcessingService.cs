using Microsoft.Extensions.Options;
using XTMon.Infrastructure;
using XTMon.Options;
using XTMon.Repositories;

namespace XTMon.Services;

public sealed class ReplayFlowProcessingService : BackgroundService
{
    internal const string StaleReplayBatchErrorMessage = "Replay batch timed out while InProgress and was auto-failed.";

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ReplayFlowProcessingQueue _queue;
    private readonly ReplayFlowsOptions _options;
    private readonly ILogger<ReplayFlowProcessingService> _logger;

    public ReplayFlowProcessingService(
        IServiceScopeFactory scopeFactory,
        ReplayFlowProcessingQueue queue,
        IOptions<ReplayFlowsOptions> options,
        ILogger<ReplayFlowProcessingService> logger)
    {
        _scopeFactory = scopeFactory;
        _queue = queue;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Replay flow processing service started with RunningStaleTimeoutSeconds={RunningStaleTimeoutSeconds}.",
            _options.RunningStaleTimeoutSeconds);

        try
        {
            await foreach (var _ in _queue.DequeueAllAsync(stoppingToken))
            {
                try
                {
                    await ExpireStaleReplayBatchesAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(AppLogEvents.ReplayProcessorBackgroundFailed, ex,
                        "Replay stale-batch expiry failed before processing; continuing with the work item.");
                }

                try
                {
                    _logger.LogInformation("Processing replay flows...");
                    using var scope = _scopeFactory.CreateScope();
                    var repository = scope.ServiceProvider.GetRequiredService<IReplayFlowRepository>();
                    await repository.ProcessReplayFlowsAsync(stoppingToken);
                    _logger.LogInformation("Replay flows processing completed.");
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    _logger.LogInformation("Replay flow processing service is stopping.");
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(AppLogEvents.ReplayProcessorBackgroundFailed, ex, "Background replay flow processing failed.");

                    // After a failure, best-effort cleanup so the UI doesn't stay orange on rows
                    // the SP abandoned mid-flight. Uses a generous age threshold so a slow-but-alive
                    // worker isn't punished here.
                    try
                    {
                        await ExpireStaleReplayBatchesAsync(stoppingToken);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (Exception expireEx)
                    {
                        _logger.LogError(AppLogEvents.ReplayProcessorBackgroundFailed, expireEx,
                            "Replay stale-batch expiry after a processing failure also failed.");
                    }
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Replay flow processing service cancellation received.");
        }

        _logger.LogInformation("Replay flow processing service stopped.");
    }

    private async Task ExpireStaleReplayBatchesAsync(CancellationToken cancellationToken)
    {
        var staleTimeout = TimeSpan.FromSeconds(_options.RunningStaleTimeoutSeconds);

        using var scope = _scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IReplayFlowRepository>();
        var expired = await repository.FailStaleReplayBatchesAsync(staleTimeout, StaleReplayBatchErrorMessage, cancellationToken);
        if (expired > 0)
        {
            _logger.LogWarning("Marked {ExpiredCount} stale replay batch row(s) as failed.", expired);
        }
    }
}
