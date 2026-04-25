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

public partial class JvBalanceConsistency : ComponentBase, IDisposable
{
    private const string DisplayDateFormat = "dd-MM-yyyy";
    private const string DisplayDateTimeFormat = "dd-MM-yyyy HH:mm:ss";
    private const string GridDateFormat = "dd-MM-yyyy";
    private const string LoadErrorMessage = "Unable to load JV Balance Consistency right now. Please try again.";
    private const string MonitoringSubmenuKey = "jv-balance-consistency";
    private static readonly IReadOnlyList<GridColumnDefinition> PreferredColumns =
    [
        new("Pnl Date", ["PnlDate", "Pnl Date"]),
        new("Sk Porfolio", ["SkPortfolio", "portfolioid", "PortfolioId"]),
        new("Mtd Amount HO", ["MtdAmountHO", "Mtd Amount HO"]),
        new("Ytd Amount HO", ["YtdAmountHO", "Ytd Amount HO"]),
        new("Mtd Amount Paradigm", ["MtdAmountParadigm", "Mtd Amount Paradigm"]),
        new("Qtd Amount Paradigm", ["QtdAmountParadigm", "Qtd Amount Paradigm"]),
        new("Ydt Amount Paradigm", ["YtdAmountParadigm", "Ydt Amount Paradigm", "Ytd Amount Paradigm"]),
        new("Jv Check Balance", ["JvCheckBalance", "JvCheck", "Jv Check Balance"])
    ];

    private readonly HashSet<DateOnly> availableDates = [];

    [Inject]
    private IMonitoringJobRepository MonitoringJobRepository { get; set; } = default!;

    [Inject]
    private IJvCalculationRepository PnlDateRepository { get; set; } = default!;

    [Inject]
    private PnlDateState PnlDateState { get; set; } = default!;

    [Inject]
    private IOptions<JvBalanceConsistencyOptions> JvBalanceConsistencyOptions { get; set; } = default!;

    [Inject]
    private IOptions<MonitoringJobsOptions> MonitoringJobsOptions { get; set; } = default!;

    [Inject]
    private ILogger<JvBalanceConsistency> Logger { get; set; } = default!;

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
    private string precisionText = string.Empty;
    private string? savedParameterSummary;
    private long? activeJobId;
    private string? activeJobStatus;
    private PeriodicTimer? pollTimer;
    private CancellationTokenSource? pollCts;
    private readonly CancellationTokenSource disposeCts = new();

    private string ProcedureName => JvBalanceConsistencyOptions.Value.JvBalanceConsistencyStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(JvBalanceConsistencyOptions.Value.ConnectionStringName, ProcedureName);
    private string QueryDisplayText => string.IsNullOrWhiteSpace(parsedQuery) ? string.Empty : parsedQuery;
    private string SelectedPnlDateText => selectedPnlDate.HasValue
        ? selectedPnlDate.Value.ToString(DisplayDateFormat, CultureInfo.InvariantCulture)
        : "-";
    private string SavedParameterSummaryText => string.IsNullOrWhiteSpace(savedParameterSummary) ? $"Precision: {precisionText}" : savedParameterSummary;
    private string JobStatusText => string.IsNullOrWhiteSpace(activeJobStatus) ? "-" : activeJobStatus;
    private string LastRunText => lastRunAt.HasValue
        ? lastRunAt.Value.ToString(DisplayDateTimeFormat, CultureInfo.InvariantCulture)
        : "-";
    private bool IsJobActive => MonitoringJobHelper.IsActiveStatus(activeJobStatus);

    protected override async Task OnInitializedAsync()
    {
        precisionText = JvBalanceConsistencyOptions.Value.Precision.ToString("0.00", CultureInfo.InvariantCulture);
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

    private void OnPrecisionInput(ChangeEventArgs args)
    {
        precisionText = Convert.ToString(args.Value, CultureInfo.InvariantCulture) ?? string.Empty;
        validationError = null;
        runError = null;
    }

    private async Task RunAsync()
    {
        if (!selectedPnlDate.HasValue)
        {
            validationError = "PNL DATE is required.";
            return;
        }

        if (!TryParsePrecision(out var precision, out var precisionError))
        {
            validationError = precisionError;
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
                "JV Balance Consistency",
                selectedPnlDate.Value,
                MonitoringJobHelper.SerializeParameters(BuildCurrentParameters(precision)),
                $"Precision: {FormatPrecision(precision)}",
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
                "Failed to load JV Balance Consistency for PnlDate {PnlDate} and Precision {Precision}.",
                selectedPnlDate.Value,
                precision);
            runError = LoadErrorMessage;
            parsedQuery = string.Empty;
            result = null;
        }
        finally
        {
            isLoading = false;
        }
    }

    private DataValidationJobParameters BuildCurrentParameters(decimal precision)
    {
        return new DataValidationJobParameters(
            SourceSystemCodes: null,
            TraceAllVersions: null,
            Precision: precision);
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
            Logger.LogWarning(ex, "Unable to restore latest JV Balance Consistency job for PnlDate {PnlDate}.", selectedPnlDate.Value);
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
        savedParameterSummary = job.ParameterSummary;
        parsedQuery = job.ParsedQuery ?? string.Empty;
        result = JvCalculationHelper.DeserializeMonitoringTable(job.GridColumnsJson, job.GridRowsJson);
        hasRun = true;

        var savedParameters = MonitoringJobHelper.DeserializeParameters<DataValidationJobParameters>(job.ParametersJson);
        if (savedParameters?.Precision is decimal precision)
        {
            precisionText = FormatPrecision(precision);
        }

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
        savedParameterSummary = null;
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

    private bool TryParsePrecision(out decimal precision, out string error)
    {
        var normalized = precisionText.Trim();
        if (string.IsNullOrWhiteSpace(normalized))
        {
            precision = default;
            error = "Precision is required.";
            return false;
        }

        if (!decimal.TryParse(normalized, NumberStyles.Number, CultureInfo.InvariantCulture, out precision) &&
            !decimal.TryParse(normalized, NumberStyles.Number, CultureInfo.CurrentCulture, out precision))
        {
            error = "Precision must be a valid decimal number.";
            return false;
        }

        if (precision is < 0m or > 99.99m)
        {
            error = "Precision must be between 0 and 99.99.";
            return false;
        }

        error = string.Empty;
        return true;
    }

    private static string FormatPrecision(decimal precision)
    {
        return Math.Round(precision, 2, MidpointRounding.AwayFromZero).ToString("0.00", CultureInfo.InvariantCulture);
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
            Logger.LogWarning(ex, "Unable to copy JV Balance Consistency SQL statement to clipboard.");
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

        if (MonitoringDisplayHelper.IsDateLikeColumn(columnName) &&
            DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsedDate))
        {
            return parsedDate.ToString(GridDateFormat, CultureInfo.InvariantCulture);
        }

        return value;
    }

    private sealed record GridColumnDefinition(string Header, IReadOnlyList<string> Aliases);

    private sealed record GridColumn(string Name, string Header, int Index);
}