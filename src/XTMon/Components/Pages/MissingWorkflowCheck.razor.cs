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

public partial class MissingWorkflowCheck : ComponentBase, IDisposable
{
    private const string DisplayDateFormat = "dd-MM-yyyy";
    private const string DisplayDateTimeFormat = "dd-MM-yyyy HH:mm:ss";
    private const string LoadErrorMessage = "Unable to load Missing Workflow Check right now. Please try again.";
    private const string MonitoringSubmenuKey = "missing-workflow-check";
    private static readonly IReadOnlyList<GridColumnDefinition> PreferredColumns =
    [
        new("Porfolio Id", ["PortfolioId", "Portfolio Id"]),
        new("Sk Porfolio", ["SkPortfolio", "Sk Portfolio"]),
        new("Book Id", ["BookId", "Book Id"]),
        new("Book Name", ["BookName", "Book Name"]),
        new("Is Daily Asset", ["DailyValidatedAsset", "Is Daily Asset"])
    ];

    private readonly HashSet<DateOnly> availableDates = [];

    [Inject]
    private IMissingWorkflowCheckRepository Repository { get; set; } = default!;

    [Inject]
    private IMonitoringJobRepository MonitoringJobRepository { get; set; } = default!;

    [Inject]
    private IJvCalculationRepository PnlDateRepository { get; set; } = default!;

    [Inject]
    private PnlDateState PnlDateState { get; set; } = default!;

    [Inject]
    private IOptions<MissingWorkflowCheckOptions> MissingWorkflowCheckOptions { get; set; } = default!;

    [Inject]
    private IOptions<MonitoringJobsOptions> MonitoringJobsOptions { get; set; } = default!;

    [Inject]
    private ILogger<MissingWorkflowCheck> Logger { get; set; } = default!;

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

    private string ProcedureName => MissingWorkflowCheckOptions.Value.MissingWorkflowCheckStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(MissingWorkflowCheckOptions.Value.ConnectionStringName, ProcedureName);
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
            await PnlDateState.EnsureLoadedAsync(PnlDateRepository, CancellationToken.None);
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
                "Missing Workflow Check",
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
                "Failed to load Missing Workflow Check for PnlDate {PnlDate}.",
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
            Logger.LogWarning(ex, "Unable to restore latest Missing Workflow Check job for PnlDate {PnlDate}.", selectedPnlDate.Value);
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
            runError = string.IsNullOrWhiteSpace(job.ErrorMessage) ? LoadErrorMessage : job.ErrorMessage;
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
            Logger.LogWarning(ex, "Unable to copy Missing Workflow Check SQL statement to clipboard.");
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

        if (string.Equals(columnName, "DailyValidatedAsset", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(columnName, "Is Daily Asset", StringComparison.OrdinalIgnoreCase))
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