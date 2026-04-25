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
using XTMon.Services;

namespace XTMon.Components.Pages;

public partial class JvCalculationCheck : PnlDateAwarePageBase<JvCalculationCheck>
{
    private const string CheckOnlyRequestType = "CheckOnly";
    private const string FixAndCheckRequestType = "FixAndCheck";
    private const string UnknownUserId = "Unknown";
    private const string LoadErrorMessage = "Unable to load COB dates right now. Please try again.";
    private const string CheckErrorMessage = "Unable to run JV calculation check right now. Please try again.";

    [Inject]
    private IJvCalculationRepository Repository { get; set; } = default!;

    [Inject]
    private IOptions<JvCalculationOptions> JvOptions { get; set; } = default!;

    [Inject]
    private IJSRuntime JsRuntime { get; set; } = default!;

    [Inject]
    private IBackgroundJobCancellationService BackgroundJobCancellationService { get; set; } = default!;

    [Inject]
    private JvCalculationNavAlertState JvCalculationNavAlertState { get; set; } = default!;

    [CascadingParameter]
    private Task<AuthenticationState> AuthenticationStateTask { get; set; } = default!;

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
    private bool isCancellingJob;
    private PeriodicTimer? pollTimer;
    private CancellationTokenSource? pollCts;

    private DateOnly? selectedCobDate
    {
        get => selectedPnlDate;
        set => selectedPnlDate = value;
    }

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
    private bool CanCancelJob => activeJobId.HasValue && MonitoringJobHelper.IsActiveStatus(activeJobStatus) && !isCancellingJob;

    private string JobStatusClass => activeJobStatus?.ToUpperInvariant() switch
    {
        "QUEUED" => "jv-status-badge--queued",
        "RUNNING" => "jv-status-badge--running",
        "COMPLETED" => "jv-status-badge--completed",
        "CANCELLED" => "jv-status-badge--queued",
        "FAILED" => "jv-status-badge--failed",
        _ => "jv-status-badge--queued"
    };

    protected override async Task EnsurePnlDatesLoadedAsync(CancellationToken cancellationToken)
    {
        await PnlDateState.EnsureLoadedAsync(Repository, cancellationToken);
    }

    protected override void HandlePnlDateLoadException(Exception exception)
    {
        Logger.LogError(AppLogEvents.JvPageLoadFailed, exception, "Failed to load JV COB dates from procedure {ProcedureName}.", CobDatesProcedureName);
        loadError = LoadErrorMessage;
    }

    protected override async Task OnInitializedCoreAsync()
    {
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
            await ReloadPnlDatesAsync();
        }
        finally
        {
            isLoadingDates = false;
        }
    }

    protected override async Task OnGlobalPnlDateChangedCoreAsync()
    {
        checkError = null;

        if (!selectedCobDate.HasValue)
        {
            ClearLoadedState();
            return;
        }

        await RestoreLatestJobAsync();
        StartPollingIfNeeded();
    }

    private Task OnCobDateSelected(DateOnly date)
    {
        checkError = null;
        return SetGlobalPnlDateAsync(date);
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
            ClearLoadedState();
            return;
        }

        try
        {
            var authState = await AuthenticationStateTask;
            var userId = authState.User.Identity?.Name ?? UnknownUserId;
            var latestJob = await Repository.GetLatestJvJobAsync(userId, selectedCobDate.Value, requestType: null, disposeCts.Token);
            if (latestJob is null)
            {
                ClearLoadedState();
                return;
            }

            if (MonitoringJobHelper.ShouldTreatAsNotRun(latestJob.Status, latestJob.StartedAt))
            {
                ClearLoadedState();
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

    private void ClearLoadedState()
    {
        StopPolling();
        activeJobStatus = null;
        activeJobRequestType = null;
        activeJobEnqueuedAt = null;
        activeJobStartedAt = null;
        activeJobCompletedAt = null;
        activeJobId = null;
        parsedQuery = string.Empty;
        parsedFixQuery = string.Empty;
        result = null;
        copyMessage = null;
        copySucceeded = false;
        copyFixMessage = null;
        copyFixSucceeded = false;
        showJobStatusDetails = false;
        showCheckQuery = false;
        showFixQuery = false;
        isChecking = false;
        isFixing = false;
        JvCalculationNavAlertState.ApplyStatus(selectedCobDate, job: null);
    }

    private void StartPollingIfNeeded()
    {
        StopPolling();

        if (!activeJobId.HasValue)
        {
            return;
        }

        if (MonitoringJobHelper.IsTerminalStatus(activeJobStatus))
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
            isCancellingJob = false;
            return;
        }

        job = await ResolveStaleRunningJobAsync(job, cancellationToken);

        if (MonitoringJobHelper.ShouldTreatAsNotRun(job.Status, job.StartedAt))
        {
            ClearLoadedState();
            isCancellingJob = false;
            return;
        }

        ApplyJob(job);
        if (!MonitoringJobHelper.IsActiveStatus(activeJobStatus))
        {
            isCancellingJob = false;
            StopPolling();
        }
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

        if (MonitoringJobHelper.IsRunningStatus(job.Status))
        {
            showJobStatusDetails = true;
        }
        else if (MonitoringJobHelper.IsTerminalStatus(job.Status))
        {
            showJobStatusDetails = false;
        }

        var isInProgress = MonitoringJobHelper.IsActiveStatus(job.Status);

        isChecking = isInProgress && string.Equals(job.RequestType, CheckOnlyRequestType, StringComparison.OrdinalIgnoreCase);
        isFixing = isInProgress && string.Equals(job.RequestType, FixAndCheckRequestType, StringComparison.OrdinalIgnoreCase);

        parsedQuery = job.QueryCheck ?? string.Empty;
        parsedFixQuery = job.QueryFix ?? string.Empty;
        result = DeserializeMonitoringTable(job.GridColumnsJson, job.GridRowsJson);

        if (MonitoringJobHelper.IsFailedStatus(job.Status))
        {
            checkError = MonitoringDisplayHelper.GetSafeBackgroundJobMessage(job.ErrorMessage, "JV background job failed.");
        }
        else if (MonitoringJobHelper.IsCancelledStatus(job.Status))
        {
            checkError = MonitoringDisplayHelper.GetSafeBackgroundJobMessage(job.ErrorMessage, "JV background job was cancelled.");
        }
        else
        {
            checkError = null;
        }

        JvCalculationNavAlertState.ApplyStatus(selectedCobDate ?? job.PnlDate, job);
    }

    private void ToggleJobStatusDetails()
    {
        showJobStatusDetails = !showJobStatusDetails;
    }

    private async Task CancelJobAsync()
    {
        if (!activeJobId.HasValue)
        {
            return;
        }

        isCancellingJob = true;
        var keepCancellationPending = false;

        try
        {
            var cancelled = await BackgroundJobCancellationService.CancelJvJobAsync(activeJobId.Value, disposeCts.Token);
            checkError = cancelled switch
            {
                { WasActive: false } => "JV job is no longer active.",
                { CancellationConfirmed: true } => "JV cancellation was recorded. Long-running queries can still take up to 3 minutes to stop completely.",
                _ => "JV cancellation was requested. Long-running queries can take up to 3 minutes to stop."
            };
            keepCancellationPending = cancelled.WasActive && !cancelled.CancellationConfirmed;

            await RefreshActiveJobAsync(disposeCts.Token);
            await InvokeAsync(StateHasChanged);
        }
        catch (OperationCanceledException) when (disposeCts.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Unable to cancel JV job {JobId}.", activeJobId.Value);
            checkError = "Unable to cancel JV job right now. Please try again.";
        }
        finally
        {
            if (!keepCancellationPending)
            {
                isCancellingJob = false;
            }
        }
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

    protected override void DisposeCore()
    {
        StopPolling();
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

    private static string FormatCellValue(string columnName, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "-";
        }

        var normalizedColumnName = MonitoringDisplayHelper.NormalizeColumnName(columnName);
        if (normalizedColumnName is "pnldate" or "cobdate")
        {
            if (DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsedDateTime))
            {
                return DateOnly.FromDateTime(parsedDateTime).ToString(DisplayDateFormat, CultureInfo.InvariantCulture);
            }

            if (DateOnly.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsedDateOnly))
            {
                return parsedDateOnly.ToString(DisplayDateFormat, CultureInfo.InvariantCulture);
            }
        }

        return value;
    }
}
