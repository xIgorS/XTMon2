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

    private readonly CancellationTokenSource disposeCts = new();

    private DiagnosticsReport? report => StartupDiagnosticsState.Report;
    private bool isRunning => StartupDiagnosticsState.IsRunning;
    private string? runError => StartupDiagnosticsState.Error;
    private MonitoringJobConcurrencyPolicy EffectiveMonitoringJobConcurrencyPolicy => BuildMonitoringJobConcurrencyPolicy(MonitoringJobsOptions.Value);

    protected override void OnInitialized()
    {
        StartupDiagnosticsState.StatusChanged += OnDiagnosticsStatusChanged;
    }

    private async Task RunCheckAsync()
    {
        await StartupDiagnosticsState.RunAsync(disposeCts.Token);
    }

    private static string FormatDuration(TimeSpan duration) =>
        duration.TotalMilliseconds < 1000
            ? $"{duration.TotalMilliseconds:0} ms"
            : $"{duration.TotalSeconds:0.0} s";

    private static string FormatParameters(IReadOnlyList<StoredProcedureParameterInfo> parameters) =>
        string.Join(", ", parameters.Select(p => p.IsOutput ? $"{p.Name} ({p.TypeName} OUT)" : $"{p.Name} ({p.TypeName})"));

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
