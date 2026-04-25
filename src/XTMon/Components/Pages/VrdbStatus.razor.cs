using System.Globalization;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.Options;
using Microsoft.JSInterop;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;
using XTMon.Services;

namespace XTMon.Components.Pages;

public partial class VrdbStatus : ComponentBase, IDisposable
{
    private const string DisplayDateFormat = "dd-MM-yyyy";
    private const string DisplayDateTimeFormat = "dd-MM-yyyy HH:mm:ss";
    private const string GridDateFormat = "dd-MM-yyyy";
    private const string GridDateTimeFormat = "dd-MM-yyyy HH:mm:ss";
    private const string LoadErrorMessage = "Unable to load VRDB Status right now. Please try again.";
    private const string MonitoringSubmenuKey = "vrdb-status";
    private static readonly IReadOnlyList<GridColumnDefinition> PreferredColumns =
    [
        new("Pnl Date", ["PnlDate", "PNLDate", "Pnl Date"]),
        new("File Name", ["FileName", "File Name"]),
        new("Global Business Line", ["GlobalBusinessLine", "Global Business Line"]),
        new("Adjustment Type", ["AdjustmentType", "Adjustment Type"]),
        new("Recurrence Type", ["RecurrenceType", "Recurrence Type"]),
        new("Region", ["Region"]),
        new("Integration Status", ["IntegrationStatus", "Integration Status"]),
        new("Date Time Start", ["DateTimeStart", "StartDate", "Date Time Start"]),
        new("Date Time End", ["DateTimeEnd", "EndDate", "Date Time End"]),
        new("Is Reload", ["IsReload", "Is Reload"]),
        new("Is Failed", ["IsFailed", "Is Failed"]),
        new("Status", ["Status"])
    ];

    private readonly HashSet<DateOnly> availableDates = [];

    [Inject]
    private IVrdbStatusRepository Repository { get; set; } = default!;

    [Inject]
    private IMonitoringJobRepository MonitoringJobRepository { get; set; } = default!;

    [Inject]
    private IJvCalculationRepository PnlDateRepository { get; set; } = default!;

    [Inject]
    private PnlDateState PnlDateState { get; set; } = default!;

    [Inject]
    private IOptions<VrdbStatusOptions> VrdbStatusOptions { get; set; } = default!;

    [Inject]
    private IOptions<MonitoringJobsOptions> MonitoringJobsOptions { get; set; } = default!;

    [Inject]
    private ILogger<VrdbStatus> Logger { get; set; } = default!;

    [Inject]
    private IJSRuntime JsRuntime { get; set; } = default!;

    private DateOnly? selectedPnlDate;
    private bool isLoading;
    private bool hasRun;
    private string? validationError;
    private string? runError;
    private string parsedQuery = string.Empty;
    private string? copyMessage;
    private bool copySucceeded;
    private DateTime? lastRunAt;
    private MonitoringTableResult? result;
    private bool showQuery;
    private long? activeJobId;
    private string? activeJobStatus;
    private PeriodicTimer? pollTimer;
    private CancellationTokenSource? pollCts;
    private readonly CancellationTokenSource disposeCts = new();

    private string ProcedureName => VrdbStatusOptions.Value.VrdbStatusStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(VrdbStatusOptions.Value.ConnectionStringName, ProcedureName);
    private string QueryDisplayText => string.IsNullOrWhiteSpace(parsedQuery) ? string.Empty : parsedQuery;
    private string SelectedPnlDateText => selectedPnlDate.HasValue
        ? selectedPnlDate.Value.ToString(DisplayDateFormat, CultureInfo.InvariantCulture)
        : "-";
    private string LastRunText => lastRunAt.HasValue
        ? lastRunAt.Value.ToString(DisplayDateTimeFormat, CultureInfo.InvariantCulture)
        : "-";
    private bool IsJobActive => MonitoringJobHelper.IsActiveStatus(activeJobStatus);

    protected override async Task OnInitializedAsync()
    {
        await LoadPnlDatesAsync();
        PnlDateState.OnDateChanged += OnGlobalPnlDateChanged;
        await RestoreLatestJobAsync();
        StartPollingIfNeeded();
    }

    private async Task LoadPnlDatesAsync()
    {
        try
        {
            await PnlDateState.EnsureLoadedAsync(PnlDateRepository, disposeCts.Token);
            selectedPnlDate = PnlDateState.SelectedDate;

            availableDates.Clear();
            foreach (var date in PnlDateState.AvailableDates)
            {
                availableDates.Add(date);
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Unable to load default PNL dates.");
        }
    }

    private void OnGlobalPnlDateChanged()
    {
        _ = InvokeAsync(async () =>
        {
            selectedPnlDate = PnlDateState.SelectedDate;
            await RestoreLatestJobAsync();
            StateHasChanged();
        });
    }

    public void Dispose()
    {
        PnlDateState.OnDateChanged -= OnGlobalPnlDateChanged;
        StopPolling();
        disposeCts.Cancel();
        disposeCts.Dispose();
    }

    private async Task OnPnlDateSelected(DateOnly date)
    {
        selectedPnlDate = date;
        validationError = null;
        runError = null;
        await RestoreLatestJobAsync();
    }

    private async Task RunAsync()
    {
        if (!selectedPnlDate.HasValue)
        {
            validationError = "PNL DATE is required.";
            return;
        }

        isLoading = true;
        hasRun = true;
        validationError = null;
        runError = null;
        copyMessage = null;
        showQuery = false;

        try
        {
            var enqueueResult = await MonitoringJobRepository.EnqueueMonitoringJobAsync(
                MonitoringJobHelper.DataValidationCategory,
                MonitoringSubmenuKey,
                "VRDB Status",
                selectedPnlDate.Value,
                parametersJson: null,
                parameterSummary: null,
                disposeCts.Token);

            activeJobId = enqueueResult.JobId;
            await RefreshActiveJobAsync(disposeCts.Token);
            StartPollingIfNeeded();
        }
        catch (Exception ex)
        {
            Logger.LogError(
                AppLogEvents.MonitoringLoadFailed,
                ex,
                "Failed to load VRDB Status for PnlDate {PnlDate}.",
                selectedPnlDate.Value);
            runError = LoadErrorMessage;
            parsedQuery = string.Empty;
            result = null;
        }
        finally
        {
            isLoading = false;
        }
    }

    private async Task RestoreLatestJobAsync()
    {
        StopPolling();

        if (!selectedPnlDate.HasValue)
        {
            ClearLoadedState();
            return;
        }

        try
        {
            var latestJob = await MonitoringJobRepository.GetLatestMonitoringJobAsync(
                MonitoringJobHelper.DataValidationCategory,
                MonitoringSubmenuKey,
                selectedPnlDate.Value,
                disposeCts.Token);

            if (latestJob is null)
            {
                ClearLoadedState();
                return;
            }

            ApplyJob(latestJob);
            StartPollingIfNeeded();
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Unable to restore latest VRDB Status job for PnlDate {PnlDate}.", selectedPnlDate.Value);
        }
    }

    private void StartPollingIfNeeded()
    {
        StopPolling();

        if (!activeJobId.HasValue || !IsJobActive)
        {
            return;
        }

        pollCts = CancellationTokenSource.CreateLinkedTokenSource(disposeCts.Token);
        pollTimer = new PeriodicTimer(TimeSpan.FromSeconds(MonitoringJobsOptions.Value.JobPollIntervalSeconds));
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

        var job = await MonitoringJobRepository.GetMonitoringJobByIdAsync(activeJobId.Value, cancellationToken);
        if (job is null)
        {
            return;
        }

        ApplyJob(job);
        if (!IsJobActive)
        {
            StopPolling();
        }
    }

    private void ApplyJob(MonitoringJobRecord job)
    {
        activeJobId = job.JobId;
        activeJobStatus = job.Status;
        parsedQuery = job.ParsedQuery ?? string.Empty;
        result = JvCalculationHelper.DeserializeMonitoringTable(job.GridColumnsJson, job.GridRowsJson);
        hasRun = true;

        var latestExecution = job.CompletedAt ?? job.StartedAt ?? job.EnqueuedAt;
        lastRunAt = JvCalculationHelper.ToUtc(latestExecution).ToLocalTime();

        if (string.Equals(job.Status, "Failed", StringComparison.OrdinalIgnoreCase) && result is null)
        {
            runError = MonitoringDisplayHelper.GetSafeBackgroundJobMessage(job.ErrorMessage, LoadErrorMessage);
        }
        else
        {
            runError = null;
        }
    }

    private void ClearLoadedState()
    {
        activeJobId = null;
        activeJobStatus = null;
        hasRun = false;
        parsedQuery = string.Empty;
        result = null;
        lastRunAt = null;
        runError = null;
    }

    private void StopPolling()
    {
        pollCts?.Cancel();
        pollCts?.Dispose();
        pollCts = null;

        pollTimer?.Dispose();
        pollTimer = null;
    }

    private void ToggleQueryVisibility()
    {
        if (string.IsNullOrWhiteSpace(parsedQuery))
        {
            return;
        }

        showQuery = !showQuery;
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
            Logger.LogWarning(ex, "Unable to copy VRDB Status SQL statement to clipboard.");
            copyMessage = "Failed to copy SQL to clipboard.";
            copySucceeded = false;
        }
    }

    private IReadOnlyList<GridColumn> GetGridColumns()
    {
        if (result is null)
        {
            return Array.Empty<GridColumn>();
        }

        var sourceIndexes = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < result.Columns.Count; i++)
        {
            sourceIndexes[result.Columns[i]] = i;
        }

        var columns = new List<GridColumn>(PreferredColumns.Count);
        foreach (var definition in PreferredColumns)
        {
            foreach (var alias in definition.Aliases)
            {
                if (!sourceIndexes.TryGetValue(alias, out var index))
                {
                    continue;
                }

                columns.Add(new GridColumn(alias, definition.Header, index));
                break;
            }
        }

        return columns;
    }

    private static string? GetCellValue(GridColumn column, IReadOnlyList<string?> row)
    {
        return column.Index >= 0 && column.Index < row.Count ? row[column.Index] : null;
    }

    private static string GetColumnAlignmentClass(string columnName) => JvCalculationHelper.GetColumnAlignmentClass(columnName);

    private static string FormatCellValue(string columnName, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "-";
        }

        if ((string.Equals(columnName, "PnlDate", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(columnName, "PNLDate", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(columnName, "Pnl Date", StringComparison.OrdinalIgnoreCase)) &&
            DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsedDate))
        {
            return parsedDate.ToString(GridDateFormat, CultureInfo.InvariantCulture);
        }

        if ((string.Equals(columnName, "StartDate", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(columnName, "EndDate", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(columnName, "DateTimeStart", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(columnName, "DateTimeEnd", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(columnName, "Date Time Start", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(columnName, "Date Time End", StringComparison.OrdinalIgnoreCase)) &&
            DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsedDateTime))
        {
            return parsedDateTime.ToString(GridDateTimeFormat, CultureInfo.InvariantCulture);
        }

        if (string.Equals(columnName, "IsReload", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(columnName, "IsFailed", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(columnName, "Is Reload", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(columnName, "Is Failed", StringComparison.OrdinalIgnoreCase))
        {
            return value switch
            {
                "1" => "Yes",
                "0" => "No",
                _ => value
            };
        }

        return value;
    }

    private sealed record GridColumnDefinition(string Header, IReadOnlyList<string> Aliases);

    private sealed record GridColumn(string Name, string Header, int Index);
}