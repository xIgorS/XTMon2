using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Services;
using XTMon.Models;
using XTMon.Options;

namespace XTMon.Components.Pages;

public partial class SystemDiagnostics : ComponentBase, IAsyncDisposable
{
    [Inject]
    private StartupDiagnosticsState StartupDiagnosticsState { get; set; } = default!;

    [Inject]
    private IOptions<MonitoringJobsOptions> MonitoringJobsOptions { get; set; } = default!;

    [Inject]
    private IBackgroundJobCancellationService BackgroundJobCancellationService { get; set; } = default!;

    [Inject]
    private ILogger<SystemDiagnostics> Logger { get; set; } = default!;

    private readonly CancellationTokenSource disposeCts = new();

    private DiagnosticsReport? report => StartupDiagnosticsState.Report;
    private bool isRunning => StartupDiagnosticsState.IsRunning;
    private string? runError => StartupDiagnosticsState.Error;
    private bool isCancellingAllJobs;
    private string? bulkCancellationMessage;
    private bool bulkCancellationIsError;
    private MonitoringJobConcurrencyPolicy EffectiveMonitoringJobConcurrencyPolicy => BuildMonitoringJobConcurrencyPolicy(MonitoringJobsOptions.Value);

    protected override void OnInitialized()
    {
        StartupDiagnosticsState.StatusChanged += OnDiagnosticsStatusChanged;
    }

    private async Task RunCheckAsync()
    {
        await StartupDiagnosticsState.RunAsync(disposeCts.Token);
    }

    private async Task CancelAllBackgroundJobsAsync()
    {
        isCancellingAllJobs = true;
        bulkCancellationMessage = null;
        bulkCancellationIsError = false;

        try
        {
            var result = await BackgroundJobCancellationService.CancelAllBackgroundJobsAsync(disposeCts.Token);
            bulkCancellationMessage = result.TotalJobsCancelled == 0
                ? "No active monitoring or JV background jobs were found."
                : BuildBulkCancellationMessage(result);
        }
        catch (OperationCanceledException) when (disposeCts.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to cancel all background jobs from System Diagnostics.");
            bulkCancellationMessage = "Unable to cancel all background jobs right now.";
            bulkCancellationIsError = true;
        }
        finally
        {
            isCancellingAllJobs = false;
        }
    }

    private static string FormatDuration(TimeSpan duration) =>
        duration.TotalMilliseconds < 1000
            ? $"{duration.TotalMilliseconds:0} ms"
            : $"{duration.TotalSeconds:0.0} s";

    private static string FormatParameters(IReadOnlyList<StoredProcedureParameterInfo> parameters) =>
        string.Join(", ", parameters.Select(p => p.IsOutput ? $"{p.Name} ({p.TypeName} OUT)" : $"{p.Name} ({p.TypeName})"));

    private static string BuildBulkCancellationMessage(BackgroundJobBulkCancellationResult result)
    {
        var summary = $"Marked {result.MonitoringJobsCancelled} monitoring job(s) and {result.JvJobsCancelled} JV job(s) as cancelled.";
        var workers = $" Cancellation was requested for {result.TotalWorkersCancellationRequested} active worker(s).";

        if (result.CancellationConfirmed)
        {
            return summary + workers + " Status verification shows no active background jobs remain.";
        }

        return summary + workers + $" Status verification still reports {result.TotalActiveJobsRemaining} active job(s), so another refresh may be needed while long-running queries unwind.";
    }

    public ValueTask DisposeAsync()
    {
        StartupDiagnosticsState.StatusChanged -= OnDiagnosticsStatusChanged;
        disposeCts.Cancel();
        disposeCts.Dispose();
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    private void OnDiagnosticsStatusChanged()
    {
        _ = InvokeAsync(StateHasChanged);
    }

    private static MonitoringJobConcurrencyPolicy BuildMonitoringJobConcurrencyPolicy(MonitoringJobsOptions options)
    {
        var configuredLimits = options.CategoryMaxConcurrentJobs;
        var rows = new List<MonitoringJobCategoryConcurrencyRow>
        {
            BuildCategoryConcurrencyRow("Data Validation", MonitoringJobHelper.DataValidationCategory, configuredLimits, options.MaxConcurrentJobs),
            BuildCategoryConcurrencyRow("Functional Rejection", MonitoringJobHelper.FunctionalRejectionCategory, configuredLimits, options.MaxConcurrentJobs)
        };

        foreach (var configuredLimit in configuredLimits
            .Where(limit => !string.Equals(limit.Key, MonitoringJobHelper.DataValidationCategory, StringComparison.OrdinalIgnoreCase)
                && !string.Equals(limit.Key, MonitoringJobHelper.FunctionalRejectionCategory, StringComparison.OrdinalIgnoreCase))
            .OrderBy(limit => limit.Key, StringComparer.OrdinalIgnoreCase))
        {
            rows.Add(BuildCategoryConcurrencyRow(configuredLimit.Key, configuredLimit.Key, configuredLimits, options.MaxConcurrentJobs));
        }

        return new MonitoringJobConcurrencyPolicy(options.MaxConcurrentJobs, rows);
    }

    private static MonitoringJobCategoryConcurrencyRow BuildCategoryConcurrencyRow(
        string displayName,
        string categoryKey,
        IReadOnlyDictionary<string, int> configuredLimits,
        int globalLimit)
    {
        if (configuredLimits.TryGetValue(categoryKey, out var effectiveLimit))
        {
            return new MonitoringJobCategoryConcurrencyRow(
                displayName,
                effectiveLimit,
                $"Configured cap: up to {effectiveLimit} running job{(effectiveLimit == 1 ? string.Empty : "s")} from this menu at the same time.");
        }

        return new MonitoringJobCategoryConcurrencyRow(
            displayName,
            globalLimit,
            $"No category-specific cap is configured, so this menu can use any free slot up to the global limit of {globalLimit}.");
    }

    private sealed record MonitoringJobConcurrencyPolicy(int GlobalLimit, IReadOnlyList<MonitoringJobCategoryConcurrencyRow> CategoryLimits);

    private sealed record MonitoringJobCategoryConcurrencyRow(string DisplayName, int EffectiveLimit, string Description);
}
