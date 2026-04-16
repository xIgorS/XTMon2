using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Authorization;
using Microsoft.JSInterop;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Globalization;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Repositories;
using XTMon.Models;
using XTMon.Options;

namespace XTMon.Components.Pages;

public partial class JvCalculationCheck : ComponentBase, IAsyncDisposable
{
    private const string CheckOnlyRequestType = "CheckOnly";
    private const string FixAndCheckRequestType = "FixAndCheck";
    private const string UnknownUserId = "Unknown";
    private const string DisplayDateFormat = "dd-MM-yyyy";
    private const string DisplayDateTimeFormat = "dd-MM-yyyy HH:mm:ss";
    private const string LoadErrorMessage = "Unable to load COB dates right now. Please try again.";
    private const string CheckErrorMessage = "Unable to run JV calculation check right now. Please try again.";

    [Inject]
    private IJvCalculationRepository Repository { get; set; } = default!;

    [Inject]
    private IOptions<JvCalculationOptions> JvOptions { get; set; } = default!;

    [Inject]
    private ILogger<JvCalculationCheck> Logger { get; set; } = default!;

    [Inject]
    private IJSRuntime JsRuntime { get; set; } = default!;

    [CascadingParameter]
    private Task<AuthenticationState> AuthenticationStateTask { get; set; } = default!;

    private readonly HashSet<DateOnly> availableDates = new();
    private DateOnly? selectedCobDate;
    private string? loadError;
    private string? checkError;
    private bool isLoadingDates;
    private bool isChecking;
    private bool isFixing;
    private string? activeJobStatus;
    private string? activeJobRequestType;
    private DateTime? activeJobEnqueuedAt;
    private DateTime? activeJobStartedAt;
    private DateTime? activeJobCompletedAt;
    private long? activeJobId;
    private string parsedQuery = string.Empty;
    private string parsedFixQuery = string.Empty;
    private MonitoringTableResult? result;
    private string? copyMessage;
    private bool copySucceeded;
    private string? copyFixMessage;
    private bool copyFixSucceeded;
    private bool showJobStatusDetails;
    private bool showCheckQuery;
    private bool showFixQuery;
    private PeriodicTimer? pollTimer;
    private CancellationTokenSource? pollCts;
    private readonly CancellationTokenSource disposeCts = new();

    private string CobDatesProcedureName => JvOptions.Value.GetPnlDatesStoredProcedure;
    private string JvCheckProcedureName => JvOptions.Value.CheckJvCalculationStoredProcedure;
    private string FullyQualifiedCobDatesProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(JvOptions.Value.PnlDatesConnectionStringName, CobDatesProcedureName);
    private string FullyQualifiedJvCheckProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(JvOptions.Value.PublicationConnectionStringName, JvCheckProcedureName);
    private TimeSpan JobRunningStaleTimeout => TimeSpan.FromSeconds(JvOptions.Value.JobRunningStaleTimeoutSeconds);
    private string? JobStatusText => string.IsNullOrWhiteSpace(activeJobStatus) ? null : $"Status: {activeJobStatus}";
    private string JobStatusDetailsText => $"JobId: {(activeJobId?.ToString(CultureInfo.InvariantCulture) ?? "-")} | Latest execution: {LatestExecutionText}";
    private DateTime? LatestExecutionAt => activeJobCompletedAt ?? activeJobStartedAt ?? activeJobEnqueuedAt;
    private string LatestExecutionText => LatestExecutionAt.HasValue
        ? $"{LatestExecutionAt.Value.ToString(DisplayDateTimeFormat, CultureInfo.InvariantCulture)} UTC"
        : "-";

    private string JobStatusClass => activeJobStatus?.ToUpperInvariant() switch
    {
        "QUEUED" => "jv-status-badge--queued",
        "RUNNING" => "jv-status-badge--running",
        "COMPLETED" => "jv-status-badge--completed",
        "FAILED" => "jv-status-badge--failed",
        _ => "jv-status-badge--queued"
    };

    protected override async Task OnInitializedAsync()
    {
        await LoadCobDatesAsync();
        await RestoreLatestJobAsync();
        StartPollingIfNeeded();
    }

    private async Task LoadCobDatesAsync()
    {
        isLoadingDates = true;
        loadError = null;
        checkError = null;

        try
        {
            var response = await Repository.GetJvPnlDatesAsync(disposeCts.Token);

            availableDates.Clear();
            foreach (var date in response.AvailableDates)
            {
                availableDates.Add(date);
            }

            var selectedDate = response.DefaultDate;
            if (!selectedDate.HasValue && response.AvailableDates.Count > 0)
            {
                selectedDate = response.AvailableDates[0];
            }

            if (selectedDate.HasValue)
            {
                selectedCobDate = selectedDate.Value;
            }
        }
        catch (Exception ex)
        {
            Logger.LogError(AppLogEvents.JvPageLoadFailed, ex, "Failed to load JV COB dates from procedure {ProcedureName}.", CobDatesProcedureName);
            loadError = LoadErrorMessage;
        }
        finally
        {
            isLoadingDates = false;
        }
    }

    private Task OnCobDateSelected(DateOnly date)
    {
        selectedCobDate = date;
        checkError = null;
        return Task.CompletedTask;
    }

    private async Task RunJvCalculationCheckAsync()
    {
        await EnqueueJobAsync(CheckOnlyRequestType);
    }

    private async Task RunFixCalculationAsync()
    {
        await EnqueueJobAsync(FixAndCheckRequestType);
    }

    private async Task EnqueueJobAsync(string requestType)
    {
        if (!TryGetSelectedDate(out var selectedDate))
        {
            return;
        }

        var authState = await AuthenticationStateTask;
        var userId = authState.User.Identity?.Name ?? UnknownUserId;

        isChecking = string.Equals(requestType, CheckOnlyRequestType, StringComparison.Ordinal);
        isFixing = string.Equals(requestType, FixAndCheckRequestType, StringComparison.Ordinal);
        activeJobRequestType = requestType;
        activeJobStatus = "Queued";

        checkError = null;
        copyMessage = null;
        copyFixMessage = null;
        showCheckQuery = false;
        showFixQuery = false;

        try
        {
            var enqueueResult = await Repository.EnqueueJvJobAsync(userId, selectedDate, requestType, disposeCts.Token);
            activeJobId = enqueueResult.JobId;
            await RefreshActiveJobAsync(disposeCts.Token);
            StartPollingIfNeeded();
        }
        catch (Exception ex)
        {
            Logger.LogError(AppLogEvents.JvPageActionFailed, ex, "Failed to enqueue JV job for request type {RequestType} and PnlDate {PnlDate}.", requestType, selectedDate);
            checkError = string.Equals(requestType, CheckOnlyRequestType, StringComparison.Ordinal)
                ? CheckErrorMessage
                : "Unable to run Fix calculation right now. Please try again.";
            isChecking = false;
            isFixing = false;
        }
    }

    private async Task RestoreLatestJobAsync()
    {
        if (!selectedCobDate.HasValue)
        {
            return;
        }

        try
        {
            var authState = await AuthenticationStateTask;
            var userId = authState.User.Identity?.Name ?? UnknownUserId;
            var latestJob = await Repository.GetLatestJvJobAsync(userId, selectedCobDate.Value, requestType: null, disposeCts.Token);
            if (latestJob is null)
            {
                return;
            }

            activeJobId = latestJob.JobId;
            ApplyJob(latestJob);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Unable to restore latest JV job state for selected PnlDate {PnlDate}.", selectedCobDate);
        }
    }

    private void StartPollingIfNeeded()
    {
        StopPolling();

        if (!activeJobId.HasValue)
        {
            return;
        }

        if (activeJobStatus is "Completed" or "Failed")
        {
            return;
        }

        pollCts = CancellationTokenSource.CreateLinkedTokenSource(disposeCts.Token);
        pollTimer = new PeriodicTimer(TimeSpan.FromSeconds(JvOptions.Value.JobPollIntervalSeconds));
        _ = PollJobAsync(pollTimer, pollCts.Token);
    }

    private async Task PollJobAsync(PeriodicTimer timer, CancellationToken cancellationToken)
    {
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                await RefreshActiveJobAsync(cancellationToken);
                await InvokeAsync(StateHasChanged);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (ObjectDisposedException)
        {
        }
    }

    private async Task RefreshActiveJobAsync(CancellationToken cancellationToken)
    {
        if (!activeJobId.HasValue)
        {
            return;
        }

        var job = await Repository.GetJvJobByIdAsync(activeJobId.Value, cancellationToken);
        if (job is null)
        {
            return;
        }

        job = await ResolveStaleRunningJobAsync(job, cancellationToken);
        ApplyJob(job);
    }

    private async Task<JvJobRecord> ResolveStaleRunningJobAsync(JvJobRecord job, CancellationToken cancellationToken)
    {
        if (!IsStaleRunningJob(job))
        {
            return job;
        }

        var timeoutMinutes = Math.Ceiling(JobRunningStaleTimeout.TotalMinutes);
        var errorMessage = $"JV background job was marked as failed after exceeding the running timeout of {timeoutMinutes:0} minute(s).";

        try
        {
            Logger.LogWarning(
                "Detected stale JV running job {JobId}. Last heartbeat: {LastHeartbeatAt}, started at: {StartedAt}, timeout seconds: {TimeoutSeconds}. Marking as failed.",
                job.JobId,
                job.LastHeartbeatAt,
                job.StartedAt,
                JvOptions.Value.JobRunningStaleTimeoutSeconds);

            await Repository.MarkJvJobFailedAsync(job.JobId, errorMessage, cancellationToken);
            var refreshed = await Repository.GetJvJobByIdAsync(job.JobId, cancellationToken);
            return refreshed ?? job with
            {
                Status = "Failed",
                FailedAt = DateTime.UtcNow,
                CompletedAt = null,
                ErrorMessage = errorMessage
            };
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Failed to mark stale JV running job {JobId} as failed.", job.JobId);
            return job;
        }
    }

    private bool IsStaleRunningJob(JvJobRecord job)
    {
        if (!string.Equals(job.Status, "Running", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var lastActivityUtc = JvCalculationHelper.ToUtc(job.LastHeartbeatAt ?? job.StartedAt ?? job.EnqueuedAt);
        return JvCalculationHelper.IsStaleRunningJob(lastActivityUtc, JobRunningStaleTimeout);
    }

    private void ApplyJob(JvJobRecord job)
    {
        activeJobId = job.JobId;
        activeJobStatus = job.Status;
        activeJobRequestType = job.RequestType;
        activeJobEnqueuedAt = job.EnqueuedAt;
        activeJobStartedAt = job.StartedAt;
        activeJobCompletedAt = job.CompletedAt;

        if (string.Equals(job.Status, "Running", StringComparison.OrdinalIgnoreCase))
        {
            showJobStatusDetails = true;
        }
        else if (string.Equals(job.Status, "Completed", StringComparison.OrdinalIgnoreCase) ||
                 string.Equals(job.Status, "Failed", StringComparison.OrdinalIgnoreCase))
        {
            showJobStatusDetails = false;
        }

        var isInProgress = string.Equals(job.Status, "Queued", StringComparison.OrdinalIgnoreCase) ||
                           string.Equals(job.Status, "Running", StringComparison.OrdinalIgnoreCase);

        isChecking = isInProgress && string.Equals(job.RequestType, CheckOnlyRequestType, StringComparison.OrdinalIgnoreCase);
        isFixing = isInProgress && string.Equals(job.RequestType, FixAndCheckRequestType, StringComparison.OrdinalIgnoreCase);

        parsedQuery = job.QueryCheck ?? string.Empty;
        parsedFixQuery = job.QueryFix ?? string.Empty;
        result = DeserializeMonitoringTable(job.GridColumnsJson, job.GridRowsJson);

        if (string.Equals(job.Status, "Failed", StringComparison.OrdinalIgnoreCase))
        {
            checkError = string.IsNullOrWhiteSpace(job.ErrorMessage)
                ? "JV background job failed."
                : job.ErrorMessage;
        }
    }

    private void ToggleJobStatusDetails()
    {
        showJobStatusDetails = !showJobStatusDetails;
    }

    private void ToggleCheckQueryVisibility()
    {
        if (string.IsNullOrWhiteSpace(parsedQuery))
        {
            return;
        }

        showCheckQuery = !showCheckQuery;
    }

    private void ToggleFixQueryVisibility()
    {
        if (string.IsNullOrWhiteSpace(parsedFixQuery))
        {
            return;
        }

        showFixQuery = !showFixQuery;
    }

    private static MonitoringTableResult? DeserializeMonitoringTable(string? columnsJson, string? rowsJson) =>
        JvCalculationHelper.DeserializeMonitoringTable(columnsJson, rowsJson);

    private void StopPolling()
    {
        pollCts?.Cancel();
        pollCts?.Dispose();
        pollCts = null;

        pollTimer?.Dispose();
        pollTimer = null;
    }

    public ValueTask DisposeAsync()
    {
        StopPolling();
        disposeCts.Cancel();
        disposeCts.Dispose();
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    private async Task CopySqlToClipboardAsync()
    {
        if (string.IsNullOrWhiteSpace(parsedQuery))
        {
            copyMessage = "No SQL statement available to copy.";
            copySucceeded = false;
            return;
        }

        try
        {
            await JsRuntime.InvokeVoidAsync("navigator.clipboard.writeText", parsedQuery);
            copyMessage = "SQL copied to clipboard.";
            copySucceeded = true;
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Unable to copy parsed SQL statement to clipboard.");
            copyMessage = "Failed to copy SQL to clipboard.";
            copySucceeded = false;
        }
    }

    private async Task CopyFixSqlToClipboardAsync()
    {
        if (string.IsNullOrWhiteSpace(parsedFixQuery))
        {
            copyFixMessage = "No SQL statement available to copy.";
            copyFixSucceeded = false;
            return;
        }

        try
        {
            await JsRuntime.InvokeVoidAsync("navigator.clipboard.writeText", parsedFixQuery);
            copyFixMessage = "SQL copied to clipboard.";
            copyFixSucceeded = true;
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Unable to copy fix SQL statement to clipboard.");
            copyFixMessage = "Failed to copy SQL to clipboard.";
            copyFixSucceeded = false;
        }
    }

    private bool TryGetSelectedDate(out DateOnly selectedDate)
    {
        if (!selectedCobDate.HasValue)
        {
            checkError = "Select a COB date first.";
            selectedDate = default;
            return false;
        }

        selectedDate = selectedCobDate.Value;

        if (availableDates.Count > 0 && !availableDates.Contains(selectedDate))
        {
            checkError = "Selected date is not available from replay.UspGetPnlDates.";
            return false;
        }

        return true;
    }

    private sealed record GridColumn(int Index, string Name);

    private IReadOnlyList<GridColumn> GetGridColumns()
    {
        if (result is null)
        {
            return Array.Empty<GridColumn>();
        }

        var columns = new List<GridColumn>(result.Columns.Count);
        for (var i = 0; i < result.Columns.Count; i++)
        {
            columns.Add(new GridColumn(i, result.Columns[i]));
        }

        return columns;
    }

    private static string ToHeaderLabel(string? columnName) =>
        JvCalculationHelper.ToHeaderLabel(columnName);

    private static string GetColumnAlignmentClass(string columnName) =>
        JvCalculationHelper.GetColumnAlignmentClass(columnName);

    private static string FormatCellValue(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? "-" : value;
    }
}
