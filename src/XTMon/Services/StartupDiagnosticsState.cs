using XTMon.Infrastructure;
using XTMon.Models;

namespace XTMon.Services;

public sealed class StartupDiagnosticsState
{
    private static readonly TimeSpan DefaultSlowRunThreshold = TimeSpan.FromSeconds(15);

    private readonly IDeploymentCheckService _deploymentCheckService;
    private readonly ILogger<StartupDiagnosticsState> _logger;
    private readonly SemaphoreSlim _runLock = new(1, 1);
    private readonly TimeSpan _slowRunThreshold;
    private CancellationTokenSource? _slowRunCts;

    public StartupDiagnosticsState(
        IDeploymentCheckService deploymentCheckService,
        ILogger<StartupDiagnosticsState> logger)
        : this(deploymentCheckService, logger, DefaultSlowRunThreshold)
    {
    }

    internal StartupDiagnosticsState(
        IDeploymentCheckService deploymentCheckService,
        ILogger<StartupDiagnosticsState> logger,
        TimeSpan slowRunThreshold)
    {
        _deploymentCheckService = deploymentCheckService;
        _logger = logger;
        _slowRunThreshold = slowRunThreshold;
    }

    public event Action? StatusChanged;

    public DiagnosticsReport? Report { get; private set; }

    public bool IsRunning { get; private set; }

    public bool HasCompleted { get; private set; }

    public bool IsSlow { get; private set; }

    public string? Error { get; private set; }

    public bool IsHealthy => Report?.AllPassed == true && string.IsNullOrWhiteSpace(Error);

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        await _runLock.WaitAsync(cancellationToken);

        try
        {
            IsRunning = true;
            HasCompleted = false;
            IsSlow = false;
            Error = null;
            Report = null;
            _slowRunCts?.Cancel();
            _slowRunCts?.Dispose();
            _slowRunCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _ = MonitorSlowRunAsync(_slowRunCts.Token);
            NotifyStatusChanged();

            try
            {
                Report = await _deploymentCheckService.RunCheckAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(AppLogEvents.DiagnosticsCheckFailed, ex, "System diagnostics check failed unexpectedly during startup or refresh.");
                Error = "An unexpected error occurred while running the diagnostics check. Please try again.";
            }
            finally
            {
                _slowRunCts?.Cancel();
                _slowRunCts?.Dispose();
                _slowRunCts = null;
                IsRunning = false;
                IsSlow = false;
                HasCompleted = Report is not null || !string.IsNullOrWhiteSpace(Error);
                NotifyStatusChanged();
            }
        }
        finally
        {
            _runLock.Release();
        }
    }

    private void NotifyStatusChanged()
    {
        StatusChanged?.Invoke();
    }

    private async Task MonitorSlowRunAsync(CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(_slowRunThreshold, cancellationToken);
            if (!cancellationToken.IsCancellationRequested && IsRunning)
            {
                IsSlow = true;
                NotifyStatusChanged();
            }
        }
        catch (OperationCanceledException)
        {
        }
    }
}