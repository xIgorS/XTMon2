using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Authorization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Globalization;
using XTMon.Data;
using XTMon.Models;
using XTMon.Options;

namespace XTMon.Components.Pages;

public partial class ReplayFlows : ComponentBase, IAsyncDisposable
{
    private const string DisplayDateFormat = "dd-MM-yyyy";
    private const string DisplayDateTimeFormat = "dd-MM-yyyy HH:mm:ss";
    private const string UnknownUserId = "Unknown";
    private const int ReplayGridColumnCount = 17;
    private const string DataLoadErrorMessage = "Unable to load replay flows right now. Please try again.";
    private const string SubmitErrorMessage = "Unable to submit replay flows right now. Please try again.";

[Inject]
    private IReplayFlowRepository Repository { get; set; } = default!;

    [Inject]
    private ReplayFlowProcessingQueue ProcessingQueue { get; set; } = default!;

    [Inject]
    private IOptions<ReplayFlowsOptions> ReplayFlowsOptions { get; set; } = default!;

    [Inject]
    private ILogger<ReplayFlows> Logger { get; set; } = default!;

    [CascadingParameter]
    private Task<AuthenticationState> AuthenticationStateTask { get; set; } = default!;

    private readonly List<ReplayFlowGridRow> rows = new();
    private readonly List<ReplayFlowResultRow> replayResults = new();
    private readonly List<ReplayFlowStatusRow> statusRows = new();
    private readonly CancellationTokenSource disposeCts = new();
    private DateOnly? selectedPnlDate;
    private string replayFlowSetInput = string.Empty;
    private string? feedSourceFilter;
    private string? typeOfCalculationFilter;
    private string? pnlDateError;
    private string? replayFlowSetError;
    private bool isLoading;
    private bool isSubmitting;
    private bool hasLoaded;
    private string? loadError;
    private string? statusMessage;
    private bool statusIsError;
    private DateTimeOffset? lastRefresh;
    private DateOnly? lastPnlDate;
    private PeriodicTimer? pollTimer;
    private CancellationTokenSource? pollCts;
    private bool isPolling;
    private int pendingCount;
    private int inProgressCount;
    private int completedCount;
    private int selectedRowsCount;
    private List<ReplayFlowGridRow> filteredRows = new();
    private ElementReference replayFlowSetInputRef;

    private bool canSubmit => !isSubmitting && !isLoading && selectedRowsCount > 0;
    private bool canSelectAll => !isSubmitting && !isLoading && filteredRows.Count > 0;

    private bool CanCheckProcessingStatus =>
        statusRows.Count > 0;

    private string ReplayFlowSetInput
    {
        get => replayFlowSetInput;
        set
        {
            replayFlowSetInput = value;
            if (!string.IsNullOrWhiteSpace(replayFlowSetError))
            {
                replayFlowSetError = null;
            }

            if (!string.IsNullOrWhiteSpace(statusMessage))
            {
                statusMessage = null;
                statusIsError = false;
            }
        }
    }

    private string? FeedSourceFilter
    {
        get => feedSourceFilter;
        set
        {
            feedSourceFilter = value;
            UpdateFilteredRows();
        }
    }

    private string? TypeOfCalculationFilter
    {
        get => typeOfCalculationFilter;
        set
        {
            typeOfCalculationFilter = value;
            UpdateFilteredRows();
        }
    }

    protected override async Task OnInitializedAsync()
    {
        await LoadDataAsync(pnlDate: null, replayFlowSet: null);
        await LoadStatusAsync();
        StartPollingIfNeeded();
    }

    private async Task ReloadAsync()
    {
        if (!TryGetPnlDate(out var pnlDate))
        {
            return;
        }

        if (!TryGetReplayFlowSet(out var replayFlowSet))
        {
            return;
        }

        await LoadDataAsync(pnlDate, replayFlowSet);
        await LoadStatusAsync();
        StartPollingIfNeeded();
    }

    private async Task LoadDataAsync(DateOnly? pnlDate, string? replayFlowSet)
    {
        isLoading = true;
        loadError = null;
        statusMessage = null;
        statusIsError = false;

        try
        {
            var data = await Repository.GetFailedFlowsAsync(pnlDate, replayFlowSet, disposeCts.Token);
            rows.Clear();
            rows.AddRange(data.Select(row => new ReplayFlowGridRow(row)));
            replayResults.Clear();
            hasLoaded = true;
            lastRefresh = DateTimeOffset.Now;

            var resultPnlDate = rows.FirstOrDefault()?.Source.PnlDate;
            if (resultPnlDate.HasValue)
            {
                lastPnlDate = resultPnlDate.Value;
                selectedPnlDate = resultPnlDate.Value;
            }
            else
            {
                lastPnlDate = pnlDate;
            }

            UpdateFilteredRows();
        }
        catch (OperationCanceledException) when (disposeCts.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            Logger.LogError(AppLogEvents.ReplayDataLoadFailed, ex, "Replay flow data load failed for PnlDate {PnlDate} and ReplayFlowSet {ReplayFlowSet}.", pnlDate, replayFlowSet);
            loadError = DataLoadErrorMessage;
            rows.Clear();
            hasLoaded = false;
        }
        finally
        {
            isLoading = false;
        }
    }

    private async Task SubmitAsync()
    {
        if (!hasLoaded)
        {
            SetStatus("Load data first by entering a Pnl date and pressing Enter.", isError: true);
            return;
        }

        var selectedRows = rows.Where(row => row.IsSelected).ToList();
        if (selectedRows.Count == 0)
        {
            SetStatus("Select at least one row to submit.", isError: true);
            return;
        }

        var submissionRows = new List<ReplayFlowSubmissionRow>(selectedRows.Count);
        foreach (var row in selectedRows)
        {
            if (!row.Source.FlowId.HasValue || !row.Source.FlowIdDerivedFrom.HasValue)
            {
                SetStatus("Selected rows must have FlowId and FlowIdDerivedFrom values.", isError: true);
                return;
            }

            submissionRows.Add(new ReplayFlowSubmissionRow(
                row.Source.FlowIdDerivedFrom.Value,
                row.Source.FlowId.Value,
                row.Source.PnlDate,
                row.Source.PackageGuid,
                row.Source.WithBackdated,
                row.SkipCoreProcess,
                row.DropTableTmp));
        }

        isSubmitting = true;
        SetStatus(null, isError: false);

        try
        {
            var authState = await AuthenticationStateTask;
            var userId = authState.User.Identity?.Name ?? UnknownUserId;
            var results = await Repository.ReplayFlowsAsync(submissionRows, userId, disposeCts.Token);
            replayResults.Clear();
            replayResults.AddRange(results);
            rows.RemoveAll(row => row.IsSelected);
            UpdateFilteredRows();
            ReplayFlowSetInput = string.Empty;
            replayFlowSetError = null;
            SetStatus($"Submitted {submissionRows.Count} row(s) for replay.", isError: false);
            await InvokeAsync(StateHasChanged);
            await replayFlowSetInputRef.FocusAsync();

            await ProcessingQueue.EnqueueAsync(CancellationToken.None);
            await LoadStatusAsync();
            StartPollingIfNeeded();
        }
        catch (OperationCanceledException) when (disposeCts.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            Logger.LogError(AppLogEvents.ReplaySubmitFailed, ex, "Replay flow submission failed for {SubmissionCount} row(s).", submissionRows.Count);
            SetStatus(SubmitErrorMessage, isError: true);
        }
        finally
        {
            isSubmitting = false;
        }
    }

    private bool TryGetPnlDate(out DateOnly pnlDate)
    {
        if (!selectedPnlDate.HasValue)
        {
            pnlDateError = "Select a Pnl date first.";
            pnlDate = default;
            return false;
        }

        pnlDate = selectedPnlDate.Value;
        pnlDateError = null;
        return true;
    }

    private Task OnPnlDateSelected(DateOnly date)
    {
        selectedPnlDate = date;
        pnlDateError = null;
        statusMessage = null;
        statusIsError = false;
        return Task.CompletedTask;
    }

    private bool TryGetReplayFlowSet(out string? replayFlowSet)
    {
        if (!TryNormalizeReplayFlowSet(replayFlowSetInput, out var normalizedReplayFlowSet))
        {
            replayFlowSetError = "Replay Flow Set must contain comma-separated integers only.";
            replayFlowSet = null;
            return false;
        }

        replayFlowSetInput = normalizedReplayFlowSet ?? string.Empty;
        replayFlowSetError = null;
        replayFlowSet = normalizedReplayFlowSet;
        return true;
    }

    private bool IsReplayFlowSetValid()
    {
        return TryNormalizeReplayFlowSet(replayFlowSetInput, out _);
    }

    private void SelectAll()
    {
        if (isSubmitting || isLoading)
        {
            return;
        }

        if (filteredRows.Count == 0)
        {
            return;
        }

        foreach (var row in filteredRows)
        {
            row.IsSelected = true;
        }
        UpdateSelectedRowsCount();
    }

    private void UpdateFilteredRows()
    {
        filteredRows = rows
            .Where(row => MatchesFilter(row.Source.FeedSource, feedSourceFilter))
            .Where(row => MatchesFilter(row.Source.TypeOfCalculation, typeOfCalculationFilter))
            .ToList();
        UpdateSelectedRowsCount();
    }

    private void UpdateSelectedRowsCount()
    {
        selectedRowsCount = rows.Count(row => row.IsSelected);
    }

    private void OnRowSelectionChanged(ReplayFlowGridRow row, bool isSelected)
    {
        row.IsSelected = isSelected;
        UpdateSelectedRowsCount();
    }

    private static bool MatchesFilter(string? value, string? filter)
    {
        if (string.IsNullOrWhiteSpace(filter))
        {
            return true;
        }

        return !string.IsNullOrWhiteSpace(value) &&
               value.Equals(filter.Trim(), StringComparison.OrdinalIgnoreCase);
    }

    private IReadOnlyList<string> GetFeedSourceOptions()
    {
        return rows
            .Select(row => row.Source.FeedSource)
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .Select(value => value!)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(value => value, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private IReadOnlyList<string> GetTypeOfCalculationOptions()
    {
        return rows
            .Select(row => row.Source.TypeOfCalculation)
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .Select(value => value!)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(value => value, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private void NormalizeReplayFlowSetInput()
    {
        if (!TryNormalizeReplayFlowSet(replayFlowSetInput, out var normalizedReplayFlowSet))
        {
            replayFlowSetError = "Replay Flow Set must contain comma-separated integers only.";
            return;
        }

        replayFlowSetInput = normalizedReplayFlowSet ?? string.Empty;
        replayFlowSetError = null;
        statusMessage = null;
        statusIsError = false;
    }

    private static bool TryNormalizeReplayFlowSet(string? replayFlowSet, out string? normalizedReplayFlowSet) =>
        ReplayFlowsHelper.TryNormalizeReplayFlowSet(replayFlowSet, out normalizedReplayFlowSet);

    private void SetStatus(string? message, bool isError)
    {
        statusMessage = message;
        statusIsError = isError;
    }

    private static string FormatText(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? "-" : value;
    }

    private static string FormatDate(DateOnly value) =>
        value.ToString(DisplayDateFormat, CultureInfo.InvariantCulture);

    private static string FormatDate(DateOnly? value) =>
        ReplayFlowsHelper.FormatDate(value);

    private static string FormatDate(DateTime? value) =>
        value is { } v ? DateOnly.FromDateTime(v).ToString(DisplayDateFormat, CultureInfo.InvariantCulture) : "-";

    private static string FormatDateTime(DateTime value) =>
        value.ToString(DisplayDateTimeFormat, CultureInfo.InvariantCulture);

    private static string FormatNullableDateTime(DateTime? value) =>
        ReplayFlowsHelper.FormatDateTime(value);

    private static string FormatNumber(long value) =>
        value.ToString("N0", CultureInfo.InvariantCulture).Replace(",", " ");

    private static string FormatNumber(long? value) =>
        ReplayFlowsHelper.FormatNumber(value);

    private sealed class ReplayFlowGridRow
    {
        public ReplayFlowGridRow(FailedFlowRow source)
        {
            Source = source;
            SkipCoreProcess = source.SkipCoreProcess;
            DropTableTmp = source.DropTableTmp;
        }

        public FailedFlowRow Source { get; }

        public bool IsSelected { get; set; }

        public bool SkipCoreProcess { get; set; }

        public bool DropTableTmp { get; set; }
    }

    private async Task LoadStatusAsync()
    {
        try
        {
            var data = await Repository.GetReplayFlowStatusAsync(lastPnlDate, disposeCts.Token);
            statusRows.Clear();
            statusRows.AddRange(data);
            pendingCount = statusRows.Count(r => ReplayFlowsHelper.GetStatusKind(r) == ReplayStatusKind.Pending);
            inProgressCount = statusRows.Count(r => ReplayFlowsHelper.GetStatusKind(r) == ReplayStatusKind.InProgress);
            completedCount = statusRows.Count(r => ReplayFlowsHelper.GetStatusKind(r) == ReplayStatusKind.Completed);
        }
        catch (OperationCanceledException) when (disposeCts.IsCancellationRequested)
        {
            // Page is being disposed.
        }
        catch (Exception ex)
        {
            Logger.LogWarning(AppLogEvents.ReplayStatusLoadFailed, ex, "Replay flow status load failed for PnlDate {PnlDate}.", lastPnlDate);
            // Status loading is non-critical; don't disrupt the main page.
        }
    }

    private async Task CheckProcessingStatusAsync()
    {
        try
        {
            await Repository.RefreshReplayFlowProcessStatusAsync(disposeCts.Token);
        }
        catch (OperationCanceledException) when (disposeCts.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            Logger.LogWarning(AppLogEvents.ReplayStatusLoadFailed, ex, "Replay flow process status refresh failed.");
        }

        await LoadStatusAsync();
        StartPollingIfNeeded();
    }

    private void StartPollingIfNeeded()
    {
        StopPolling();

        pollCts = CancellationTokenSource.CreateLinkedTokenSource(disposeCts.Token);
        var timer = new PeriodicTimer(TimeSpan.FromSeconds(ReplayFlowsOptions.Value.StatusPollIntervalSeconds));
        pollTimer = timer;
        isPolling = true;

        _ = PollStatusAsync(timer, pollCts.Token);
    }

    private async Task PollStatusAsync(PeriodicTimer timer, CancellationToken cancellationToken)
    {
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                await LoadStatusAsync();

                await InvokeAsync(StateHasChanged);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on dispose or navigation.
        }
        catch (ObjectDisposedException)
        {
            // Expected when timer is disposed during shutdown.
        }
        catch (Exception ex)
        {
            Logger.LogWarning(AppLogEvents.ReplayPollingLoopFailed, ex, "Replay flow polling loop terminated unexpectedly.");
        }
    }

    private void StopPolling()
    {
        isPolling = false;
        pollCts?.Cancel();
        pollCts?.Dispose();
        pollCts = null;
        pollTimer?.Dispose();
        pollTimer = null;
    }

    private static string GetStatusBadgeClass(ReplayFlowStatusRow row)
    {
        return GetStatusKind(row) switch
        {
            ReplayStatusKind.Completed => "status-completed",
            ReplayStatusKind.InProgress => "status-in-progress",
            _ => "status-pending"
        };
    }

    private static string GetStatusText(ReplayFlowStatusRow row)
    {
        if (!string.IsNullOrWhiteSpace(row.Status))
        {
            return row.Status;
        }

        return GetStatusKind(row) switch
        {
            ReplayStatusKind.Completed => "Completed",
            ReplayStatusKind.InProgress => "In Progress",
            _ => "Pending"
        };
    }

    private static string FormatDuration(ReplayFlowStatusRow row) =>
        ReplayFlowsHelper.FormatDuration(row.Duration, row.DateStarted, row.DateCompleted);

    private static ReplayStatusKind GetStatusKind(ReplayFlowStatusRow row) =>
        ReplayFlowsHelper.GetStatusKind(row);

    private static bool IsSubmissionCompleted(ReplayFlowStatusRow row) =>
        ReplayFlowsHelper.GetStatusKind(row) == ReplayStatusKind.Completed;

    public ValueTask DisposeAsync()
    {
        StopPolling();
        disposeCts.Cancel();
        disposeCts.Dispose();
        return ValueTask.CompletedTask;
    }
}
