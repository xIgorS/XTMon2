namespace XTMon.Data;

public sealed class ReplayFlowProcessingService : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ReplayFlowProcessingQueue _queue;
    private readonly ILogger<ReplayFlowProcessingService> _logger;

    public ReplayFlowProcessingService(
        IServiceScopeFactory scopeFactory,
        ReplayFlowProcessingQueue queue,
        ILogger<ReplayFlowProcessingService> logger)
    {
        _scopeFactory = scopeFactory;
        _queue = queue;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Replay flow processing service started.");

        try
        {
            await foreach (var _ in _queue.DequeueAllAsync(stoppingToken))
            {
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
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Replay flow processing service cancellation received.");
        }

        _logger.LogInformation("Replay flow processing service stopped.");
    }
}
