using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Services;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;

namespace XTMon.Components.Pages;

public partial class SystemDiagnostics : ComponentBase, IAsyncDisposable
{
    [Inject]
    private StartupDiagnosticsState StartupDiagnosticsState { get; set; } = default!;

    [Inject]
    private IOptions<MonitoringJobsOptions> MonitoringJobsOptions { get; set; } = default!;

    [Inject]
    private IOptions<SystemDiagnosticsOptions> SystemDiagnosticsOptions { get; set; } = default!;

    [Inject]
    private IOptions<ApplicationLogsOptions> ApplicationLogsOptions { get; set; } = default!;

    [Inject]
    private IBackgroundJobCancellationService BackgroundJobCancellationService { get; set; } = default!;

    [Inject]
    private ISystemDiagnosticsRepository SystemDiagnosticsRepository { get; set; } = default!;

    [Inject]
    private StartupJobRecoveryService StartupJobRecoveryService { get; set; } = default!;

    [Inject]
    private JobDiagnosticsService JobDiagnosticsService { get; set; } = default!;

    [Inject]
    private ILogger<SystemDiagnostics> Logger { get; set; } = default!;

    private readonly CancellationTokenSource disposeCts = new();

    private DiagnosticsReport? report => StartupDiagnosticsState.Report;
    private bool isRunning => StartupDiagnosticsState.IsRunning;
    private string? runError => StartupDiagnosticsState.Error;
    private bool isCancellingAllJobs;
    private string? bulkCancellationMessage;
    private bool bulkCancellationIsError;
    private bool isCleaningLogging;
    private bool isCleaningHistory;
    private bool isRecoveringStartupJobs;
    private string? cleanupMessage;
    private bool cleanupIsError;
    private string? recoveryMessage;
    private bool recoveryIsError;
    private bool isLoadingStuckJobs;
    private bool isForceExpiring;
    private StuckJobsReport? stuckJobsReport;
    private string? stuckJobsMessage;
    private bool stuckJobsIsError;
    private bool isLoadingProcessorHealth;
    private MonitoringProcessorHealthReport? processorHealthReport;
    private string? processorHealthMessage;
    private bool processorHealthIsError;
    private bool ShowCleanupButtons => SystemDiagnosticsOptions.Value.ShowCleanupButtons;
    private bool ShowApplicationLogsViewer => ApplicationLogsOptions.Value.Enabled;
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
            bulkCancellationMessage = SystemDiagnosticsBulkCancellationHelper.BuildMessage(result);
        }
        catch (OperationCanceledException) when (disposeCts.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to cancel all background jobs from System Diagnostics.");
            bulkCancellationMessage = SystemDiagnosticsErrorHelper.BuildFailureMessage(ex, "Unable to cancel all background jobs right now.");
            bulkCancellationIsError = true;
        }
        finally
        {
            isCancellingAllJobs = false;
        }
    }

    private async Task CleanLoggingAsync()
    {
        isCleaningLogging = true;
        cleanupMessage = null;
        cleanupIsError = false;

        try
        {
            var deletedRows = await SystemDiagnosticsRepository.CleanLoggingAsync(disposeCts.Token);
            cleanupMessage = deletedRows == 0
                ? "No APS Actions log rows were found to delete."
                : $"Deleted {deletedRows} APS Actions log row(s).";
        }
        catch (OperationCanceledException) when (disposeCts.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to clean logging from System Diagnostics.");
            cleanupMessage = SystemDiagnosticsErrorHelper.BuildFailureMessage(ex, "Unable to clean logging right now.");
            cleanupIsError = true;
        }
        finally
        {
            isCleaningLogging = false;
        }
    }

    private async Task RecoverStartupJobsAsync()
    {
        isRecoveringStartupJobs = true;
        recoveryMessage = null;
        recoveryIsError = false;

        try
        {
            var result = await StartupJobRecoveryService.RecoverAsync(disposeCts.Token);
            recoveryMessage = SystemDiagnosticsRecoveryHelper.BuildRecoveryMessage(result);
        }
        catch (OperationCanceledException) when (disposeCts.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to run startup-style job recovery from System Diagnostics.");
            recoveryMessage = SystemDiagnosticsErrorHelper.BuildFailureMessage(ex, "Unable to reset running job statuses right now.");
            recoveryIsError = true;
        }
        finally
        {
            isRecoveringStartupJobs = false;
        }
    }

    private async Task LoadStuckJobsAsync()
    {
        isLoadingStuckJobs = true;
        stuckJobsMessage = null;
        stuckJobsIsError = false;

        try
        {
            stuckJobsReport = await JobDiagnosticsService.GetStuckJobsReportAsync(disposeCts.Token);
            if (stuckJobsReport.TotalStuckCount == 0)
            {
                stuckJobsMessage = "No stuck background jobs found.";
            }
        }
        catch (OperationCanceledException) when (disposeCts.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to load stuck jobs report.");
            stuckJobsMessage = SystemDiagnosticsErrorHelper.BuildFailureMessage(ex, "Unable to load stuck jobs right now.");
            stuckJobsIsError = true;
        }
        finally
        {
            isLoadingStuckJobs = false;
        }
    }

    private async Task ForceExpireStuckJobsAsync()
    {
        isForceExpiring = true;
        stuckJobsMessage = null;
        stuckJobsIsError = false;

        try
        {
            var result = await JobDiagnosticsService.ForceExpireAllStuckAsync(disposeCts.Token);
            stuckJobsMessage = result.TotalExpired == 0
                ? "No stuck rows were force-expired."
                : $"Force-expired {result.MonitoringJobsExpired} monitoring job(s), {result.JvJobsExpired} JV job(s), and {result.ReplayBatchesExpired} replay batch row(s).";

            // Refresh the panel so the operator sees the cleared state immediately.
            stuckJobsReport = await JobDiagnosticsService.GetStuckJobsReportAsync(disposeCts.Token);
        }
        catch (OperationCanceledException) when (disposeCts.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to force-expire stuck jobs.");
            stuckJobsMessage = SystemDiagnosticsErrorHelper.BuildFailureMessage(ex, "Unable to force-expire stuck jobs right now.");
            stuckJobsIsError = true;
        }
        finally
        {
            isForceExpiring = false;
        }
    }

    private async Task LoadProcessorHealthAsync()
    {
        isLoadingProcessorHealth = true;
        processorHealthMessage = null;
        processorHealthIsError = false;

        try
        {
            processorHealthReport = await JobDiagnosticsService.GetMonitoringProcessorHealthReportAsync(disposeCts.Token);
            processorHealthMessage = BuildProcessorHealthMessage(processorHealthReport);
        }
        catch (OperationCanceledException) when (disposeCts.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to load monitoring processor health.");
            processorHealthMessage = SystemDiagnosticsErrorHelper.BuildFailureMessage(ex, "Unable to load monitoring processor health right now.");
            processorHealthIsError = true;
        }
        finally
        {
            isLoadingProcessorHealth = false;
        }
    }

    private async Task CleanHistoryAsync()
    {
        isCleaningHistory = true;
        cleanupMessage = null;
        cleanupIsError = false;

        try
        {
            var result = await SystemDiagnosticsRepository.CleanHistoryAsync(disposeCts.Token);
            cleanupMessage = result.TotalDeleted == 0
                ? "No monitoring or JV history rows were found to delete."
                : BuildHistoryCleanupMessage(result);
        }
        catch (OperationCanceledException) when (disposeCts.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to clean monitoring and JV history from System Diagnostics.");
            cleanupMessage = SystemDiagnosticsErrorHelper.BuildFailureMessage(ex, "Unable to clean history right now.");
            cleanupIsError = true;
        }
        finally
        {
            isCleaningHistory = false;
        }
    }

    private static string FormatDuration(TimeSpan duration) =>
        duration.TotalMilliseconds < 1000
            ? $"{duration.TotalMilliseconds:0} ms"
            : $"{duration.TotalSeconds:0.0} s";

    private static string FormatParameters(IReadOnlyList<StoredProcedureParameterInfo> parameters) =>
        string.Join(", ", parameters.Select(p => p.IsOutput ? $"{p.Name} ({p.TypeName} OUT)" : $"{p.Name} ({p.TypeName})"));

    private static string BuildHistoryCleanupMessage(SystemDiagnosticsHistoryCleanupResult result)
    {
        return $"Deleted {result.MonitoringLatestResultsDeleted} monitoring latest result row(s), {result.MonitoringJobsDeleted} monitoring job row(s), {result.JvCalculationJobResultsDeleted} JV result row(s), and {result.JvCalculationJobsDeleted} JV job row(s).";
    }

    private static IReadOnlyList<string> BuildDiagnosticsIssueSummary(DiagnosticsReport report)
    {
        var issues = new List<string>();

        foreach (var database in report.Databases)
        {
            if (!database.Connected)
            {
                issues.Add($"{database.ConnectionStringName}: connection failed{BuildIssueSuffix(database.ConnectionError)}");
                continue;
            }

            foreach (var storedProcedure in database.StoredProcedures.Where(sp => !sp.Exists || sp.Error is not null))
            {
                var issueText = storedProcedure.Error is not null
                    ? $"{storedProcedure.FullName}{BuildIssueSuffix(storedProcedure.Error)}"
                    : $"{storedProcedure.FullName}: missing";

                issues.Add($"{database.ConnectionStringName}: {issueText}");
            }
        }

        return issues;
    }

    private static string BuildIssueSuffix(string? detail)
    {
        if (string.IsNullOrWhiteSpace(detail))
        {
            return string.Empty;
        }

        var normalized = detail.Replace(Environment.NewLine, " ", StringComparison.Ordinal).Trim();
        return $" ({normalized})";
    }

    private static string BuildProcessorHealthMessage(MonitoringProcessorHealthReport report)
    {
        if (!report.DmvAvailable)
        {
            return "DMV runtime lookup is unavailable, so live worker health could not be verified.";
        }

        if (!report.HasIssues)
        {
            return $"No underfilled monitoring processors detected. Queued-job backlog is only flagged after {report.QueueBacklogGracePeriod.TotalSeconds:0} seconds.";
        }

        return $"Detected {report.IssueCount} processor issue(s). A processor is flagged when queued work waits longer than {report.QueueBacklogGracePeriod.TotalSeconds:0} seconds while live DMV runtime stays below the configured worker count.";
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
