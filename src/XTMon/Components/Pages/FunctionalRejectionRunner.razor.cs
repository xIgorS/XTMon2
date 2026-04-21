using System.Globalization;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;
using XTMon.Services;

namespace XTMon.Components.Pages;

public partial class FunctionalRejectionRunner : ComponentBase, IDisposable
{
    private const string DisplayDateFormat = "dd-MM-yyyy";
    private const string DisplayDateTimeFormat = "dd-MM-yyyy HH:mm:ss";

    [Inject]
    private IFunctionalRejectionRepository FunctionalRejectionRepository { get; set; } = default!;

    [Inject]
    private IMonitoringJobRepository MonitoringJobRepository { get; set; } = default!;

    [Inject]
    private IJvCalculationRepository PnlDateRepository { get; set; } = default!;

    [Inject]
    private PnlDateState PnlDateState { get; set; } = default!;

    [Inject]
    private IOptions<MonitoringJobsOptions> MonitoringJobsOptions { get; set; } = default!;

    [Inject]
    private ILogger<FunctionalRejectionRunner> Logger { get; set; } = default!;

    [Inject]
    private FunctionalRejectionNavAlertState FunctionalRejectionNavAlertState { get; set; } = default!;

    private readonly List<BatchRunRow> rows = [];
    private readonly HashSet<DateOnly> availableDates = [];
    private readonly CancellationTokenSource disposeCts = new();
    private PeriodicTimer? pollTimer;
    private CancellationTokenSource? pollCts;
    private DateOnly? selectedPnlDate;
    private bool isSubmitting;
    private bool isRefreshing;
    private bool isLoadingRows;
    private string? loadError;
    private int selectedRowsCount;
    private int activeChecksCount;
    private string? statusMessage;
    private bool statusIsError;
    private DateTime? lastRefreshAt;

    private string SelectedPnlDateText => selectedPnlDate.HasValue
        ? selectedPnlDate.Value.ToString(DisplayDateFormat, CultureInfo.InvariantCulture)
        : "-";

    private string LastRefreshText => lastRefreshAt.HasValue
        ? lastRefreshAt.Value.ToString(DisplayDateTimeFormat, CultureInfo.InvariantCulture)
        : "Not loaded";

    private bool canRunSelected => !isSubmitting && !isRefreshing && selectedPnlDate.HasValue && selectedRowsCount > 0;

    protected override async Task OnInitializedAsync()
    {
        await LoadPnlDatesAsync();
        await LoadMenuItemsAsync();
        PnlDateState.OnDateChanged += OnGlobalPnlDateChanged;
        await RefreshStatusesCoreAsync();
        StartPollingIfNeeded();
    }

    public void Dispose()
    {
        PnlDateState.OnDateChanged -= OnGlobalPnlDateChanged;
        StopPolling();
        disposeCts.Cancel();
        disposeCts.Dispose();
    }

    private async Task LoadPnlDatesAsync()
    {
        await PnlDateState.EnsureLoadedAsync(PnlDateRepository, CancellationToken.None);
        selectedPnlDate = PnlDateState.SelectedDate;

        availableDates.Clear();
        foreach (var date in PnlDateState.AvailableDates)
        {
            availableDates.Add(date);
        }
    }

    private async Task LoadMenuItemsAsync()
    {
        isLoadingRows = true;
        loadError = null;

        try
        {
            var items = await FunctionalRejectionRepository.GetMenuItemsAsync(disposeCts.Token);
            rows.Clear();
            foreach (var item in items)
            {
                rows.Add(new BatchRunRow(item));
            }

            UpdateSelectedRowsCount();
        }
        catch (OperationCanceledException) when (disposeCts.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Logger.LogError(
                AppLogEvents.MonitoringLoadFailed,
                ex,
                "Failed to load Functional Rejection menu items for batch runner.");
            loadError = "Unable to load Functional Rejection items right now.";
            rows.Clear();
            UpdateSelectedRowsCount();
        }
        finally
        {
            isLoadingRows = false;
        }
    }

    private Task OnPnlDateSelected(DateOnly date)
    {
        PnlDateState.SetDate(date);
        return Task.CompletedTask;
    }

    private void OnGlobalPnlDateChanged()
    {
        _ = InvokeAsync(async () =>
        {
            selectedPnlDate = PnlDateState.SelectedDate;
            ClearSubmissionStates();
            await RefreshStatusesCoreAsync();
            StateHasChanged();
        });
    }

    private async Task RefreshStatusesAsync()
    {
        isRefreshing = true;
        try
        {
            await RefreshStatusesCoreAsync();
        }
        finally
        {
            isRefreshing = false;
        }
    }

    private async Task RefreshStatusesCoreAsync()
    {
        StopPolling();

        foreach (var row in rows)
        {
            row.LatestJob = null;
        }

        if (!selectedPnlDate.HasValue)
        {
            lastRefreshAt = DateTime.Now;
            activeChecksCount = 0;
            FunctionalRejectionNavAlertState.ApplyStatuses(null, Array.Empty<MonitoringJobRecord>());
            return;
        }

        try
        {
            var jobs = await MonitoringJobRepository.GetLatestMonitoringJobsByCategoryAsync(
                MonitoringJobHelper.FunctionalRejectionCategory,
                selectedPnlDate.Value,
                disposeCts.Token);

            var jobLookup = new Dictionary<string, MonitoringJobRecord>(StringComparer.OrdinalIgnoreCase);
            foreach (var job in jobs)
            {
                jobLookup[job.SubmenuKey] = job;
            }

            foreach (var row in rows)
            {
                jobLookup.TryGetValue(row.SubmenuKey, out var latestJob);
                row.LatestJob = latestJob;
                UpdateSubmissionState(row);
            }

            FunctionalRejectionNavAlertState.ApplyStatuses(selectedPnlDate.Value, jobs);

            lastRefreshAt = DateTime.Now;
            activeChecksCount = rows.Count(row => MonitoringJobHelper.IsActiveStatus(row.LatestJob?.Status));
            StartPollingIfNeeded();
        }
        catch (OperationCanceledException) when (disposeCts.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Unable to refresh Functional Rejection batch-run statuses for PnlDate {PnlDate}.", selectedPnlDate);
            statusMessage = "Unable to refresh check statuses right now.";
            statusIsError = true;
        }
    }

    private void StartPollingIfNeeded()
    {
        StopPolling();

        if (!rows.Any(row => MonitoringJobHelper.IsActiveStatus(row.LatestJob?.Status)))
        {
            return;
        }

        pollCts = CancellationTokenSource.CreateLinkedTokenSource(disposeCts.Token);
        pollTimer = new PeriodicTimer(TimeSpan.FromSeconds(MonitoringJobsOptions.Value.JobPollIntervalSeconds));
        _ = PollStatusesAsync(pollTimer, pollCts.Token);
    }

    private async Task PollStatusesAsync(PeriodicTimer timer, CancellationToken cancellationToken)
    {
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                await RefreshStatusesCoreAsync();
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

    private void StopPolling()
    {
        pollCts?.Cancel();
        pollCts?.Dispose();
        pollCts = null;

        pollTimer?.Dispose();
        pollTimer = null;
    }

    private void SelectAll()
    {
        if (isSubmitting)
        {
            return;
        }

        foreach (var row in rows)
        {
            row.IsSelected = true;
        }

        UpdateSelectedRowsCount();
    }

    private void ClearSelection()
    {
        foreach (var row in rows)
        {
            row.IsSelected = false;
        }

        UpdateSelectedRowsCount();
    }

    private void ClearSubmissionStates()
    {
        foreach (var row in rows)
        {
            row.LastSubmissionOutcome = null;
            row.LastSubmissionMessage = null;
            row.LastSubmissionJobId = null;
        }
    }

    private void OnRowSelectionChanged(BatchRunRow row, bool isSelected)
    {
        row.IsSelected = isSelected;
        UpdateSelectedRowsCount();
    }

    private void UpdateSelectedRowsCount()
    {
        selectedRowsCount = rows.Count(row => row.IsSelected);
    }

    private async Task RunSelectedAsync()
    {
        if (!selectedPnlDate.HasValue)
        {
            statusMessage = "Select a PNL date first.";
            statusIsError = true;
            return;
        }

        var selectedRows = rows.Where(row => row.IsSelected).ToList();
        if (selectedRows.Count == 0)
        {
            statusMessage = "Select at least one check to run.";
            statusIsError = true;
            return;
        }

        ClearSubmissionStates();
        isSubmitting = true;
        statusMessage = null;
        statusIsError = false;

        var queuedCount = 0;
        var alreadyActiveCount = 0;
        var failedCount = 0;

        foreach (var row in selectedRows)
        {
            try
            {
                var parameters = new FunctionalRejectionJobParameters(
                    row.Item.SourceSystemBusinessDataTypeCode,
                    row.Item.BusinessDataTypeId,
                    row.Item.SourceSystemName,
                    row.Item.DbConnection);

                var enqueueResult = await MonitoringJobRepository.EnqueueMonitoringJobAsync(
                    MonitoringJobHelper.FunctionalRejectionCategory,
                    row.SubmenuKey,
                    row.Item.SourceSystemBusinessDataTypeCode,
                    selectedPnlDate.Value,
                    MonitoringJobHelper.SerializeParameters(parameters),
                    MonitoringJobHelper.BuildFunctionalRejectionParameterSummary(parameters),
                    disposeCts.Token);

                if (enqueueResult.AlreadyActive)
                {
                    row.LastSubmissionOutcome = BatchSubmissionOutcome.AlreadyActive;
                    row.LastSubmissionMessage = "Already running";
                    row.LastSubmissionJobId = enqueueResult.JobId;
                    alreadyActiveCount++;
                }
                else
                {
                    row.LastSubmissionOutcome = BatchSubmissionOutcome.Queued;
                    row.LastSubmissionMessage = "Queued";
                    row.LastSubmissionJobId = enqueueResult.JobId;
                    queuedCount++;
                }

                row.IsSelected = false;
            }
            catch (OperationCanceledException) when (disposeCts.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                Logger.LogError(
                    ex,
                    "Failed to enqueue Functional Rejection batch item for Code {Code}, BusinessDataTypeId {BusinessDataTypeId}, PnlDate {PnlDate}.",
                    row.Item.SourceSystemBusinessDataTypeCode,
                    row.Item.BusinessDataTypeId,
                    selectedPnlDate.Value);
                row.LastSubmissionOutcome = BatchSubmissionOutcome.Failed;
                row.LastSubmissionMessage = "Submission failed";
                row.LastSubmissionJobId = null;
                failedCount++;
            }
        }

        UpdateSelectedRowsCount();
        await RefreshStatusesCoreAsync();

        statusMessage = BuildSubmissionSummary(queuedCount, alreadyActiveCount, failedCount);
        statusIsError = failedCount > 0 && queuedCount == 0 && alreadyActiveCount == 0;
        isSubmitting = false;
    }

    private static string BuildSubmissionSummary(int queuedCount, int alreadyActiveCount, int failedCount)
    {
        var parts = new List<string>(3);

        if (queuedCount > 0)
        {
            parts.Add($"Queued {queuedCount}");
        }

        if (alreadyActiveCount > 0)
        {
            parts.Add($"Skipped {alreadyActiveCount} already active");
        }

        if (failedCount > 0)
        {
            parts.Add($"Failed {failedCount}");
        }

        return parts.Count == 0
            ? "No checks were submitted."
            : string.Join(". ", parts) + ".";
    }

    private static string GetStatusBadgeClass(MonitoringJobRecord? job)
    {
        return FunctionalRejectionNavAlertHelper.GetRunState(job) switch
        {
            DataValidationNavRunState.Failed => "submenu-status-badge--failed",
            DataValidationNavRunState.Alert => "submenu-status-badge--failed",
            DataValidationNavRunState.Succeeded => "submenu-status-badge--succeeded",
            DataValidationNavRunState.Running => "submenu-status-badge--running",
            _ => "submenu-status-badge--not-run"
        };
    }

    private static string GetStatusLabel(MonitoringJobRecord? job)
    {
        if (job is null)
        {
            return "Not run";
        }

        var runState = FunctionalRejectionNavAlertHelper.GetRunState(job);
        if (runState == DataValidationNavRunState.Failed)
        {
            return "Failed";
        }

        if (runState == DataValidationNavRunState.Alert)
        {
            return "Alert";
        }

        if (runState == DataValidationNavRunState.Succeeded)
        {
            return "Completed";
        }

        if (MonitoringJobHelper.IsActiveStatus(job.Status))
        {
            return string.Equals(job.Status, "Queued", StringComparison.OrdinalIgnoreCase)
                ? "Queued"
                : "Running";
        }

        return string.IsNullOrWhiteSpace(job.Status) ? "Not run" : job.Status;
    }

    private static string GetLastRunText(MonitoringJobRecord? job)
    {
        if (job is null)
        {
            return "-";
        }

        var latestExecution = job.CompletedAt ?? job.StartedAt ?? job.EnqueuedAt;
        return JvCalculationHelper.ToUtc(latestExecution).ToLocalTime().ToString(DisplayDateTimeFormat, CultureInfo.InvariantCulture);
    }

    private static string GetSubmissionText(BatchRunRow row)
    {
        return string.IsNullOrWhiteSpace(row.LastSubmissionMessage)
            ? "-"
            : row.LastSubmissionMessage;
    }

    private static string GetSubmissionTextClass(BatchRunRow row)
    {
        return row.LastSubmissionOutcome switch
        {
            BatchSubmissionOutcome.Completed => "text-success",
            BatchSubmissionOutcome.Queued => "text-success",
            BatchSubmissionOutcome.Running => "text-amber-600",
            BatchSubmissionOutcome.AlreadyActive => "text-amber-600",
            BatchSubmissionOutcome.Failed => "text-error",
            _ => string.Empty
        };
    }

    private static void UpdateSubmissionState(BatchRunRow row)
    {
        if (!row.LastSubmissionJobId.HasValue || row.LatestJob is null || row.LatestJob.JobId != row.LastSubmissionJobId.Value)
        {
            return;
        }

        if (string.Equals(row.LatestJob.Status, "Completed", StringComparison.OrdinalIgnoreCase) || row.LatestJob.CompletedAt is not null)
        {
            row.LastSubmissionOutcome = BatchSubmissionOutcome.Completed;
            row.LastSubmissionMessage = "Completed";
            return;
        }

        if (string.Equals(row.LatestJob.Status, "Failed", StringComparison.OrdinalIgnoreCase) || row.LatestJob.FailedAt is not null)
        {
            row.LastSubmissionOutcome = BatchSubmissionOutcome.Failed;
            row.LastSubmissionMessage = "Failed";
            return;
        }

        if (MonitoringJobHelper.IsActiveStatus(row.LatestJob.Status))
        {
            row.LastSubmissionOutcome = string.Equals(row.LatestJob.Status, "Queued", StringComparison.OrdinalIgnoreCase)
                ? BatchSubmissionOutcome.Queued
                : BatchSubmissionOutcome.Running;
            row.LastSubmissionMessage = GetStatusLabel(row.LatestJob);
        }
    }

    private sealed class BatchRunRow
    {
        public BatchRunRow(FunctionalRejectionMenuItem item)
        {
            Item = item;
            SubmenuKey = MonitoringJobHelper.BuildFunctionalRejectionSubmenuKey(
                item.BusinessDataTypeId,
                item.SourceSystemName,
                item.DbConnection,
                item.SourceSystemBusinessDataTypeCode);
        }

        public FunctionalRejectionMenuItem Item { get; }

        public string SubmenuKey { get; }

        public MonitoringJobRecord? LatestJob { get; set; }

        public bool IsSelected { get; set; }

        public BatchSubmissionOutcome? LastSubmissionOutcome { get; set; }

        public string? LastSubmissionMessage { get; set; }

        public long? LastSubmissionJobId { get; set; }
    }

    private enum BatchSubmissionOutcome
    {
        Completed,
        Queued,
        Running,
        AlreadyActive,
        Failed
    }
}
