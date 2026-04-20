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

public partial class Pricing : ComponentBase, IDisposable
{
    private const string DisplayDateFormat = "dd-MM-yyyy";
    private const string DisplayDateTimeFormat = "dd-MM-yyyy HH:mm:ss";
    private const string GridDateFormat = "dd-MM-yyyy";
    private const string SourceSystemsLoadErrorMessage = "Unable to load Pricing source systems right now. Please try again.";
    private const string PricingLoadErrorMessage = "Unable to load Pricing right now. Please try again.";
    private const string MonitoringSubmenuKey = "pricing";

    private readonly HashSet<DateOnly> availableDates = [];

    [Inject]
    private IPricingRepository Repository { get; set; } = default!;

    [Inject]
    private IMonitoringJobRepository MonitoringJobRepository { get; set; } = default!;

    [Inject]
    private IJvCalculationRepository PnlDateRepository { get; set; } = default!;

    [Inject]
    private PnlDateState PnlDateState { get; set; } = default!;

    [Inject]
    private IOptions<PricingOptions> PricingOptions { get; set; } = default!;

    [Inject]
    private IOptions<MonitoringJobsOptions> MonitoringJobsOptions { get; set; } = default!;

    [Inject]
    private ILogger<Pricing> Logger { get; set; } = default!;

    [Inject]
    private IJSRuntime JsRuntime { get; set; } = default!;

    private readonly List<SourceSystemSelection> sourceSystems = [];
    private DateOnly? selectedPnlDate;
    private bool isLoading;
    private bool isLoadingSourceSystems;
    private bool hasRun;
    private string? validationError;
    private string? sourceSystemsError;
    private string? runError;
    private string parsedQuery = string.Empty;
    private string? copyMessage;
    private bool copySucceeded;
    private DateTime? lastRunAt;
    private MonitoringTableResult? result;
    private bool showQuery;
    private string? savedParameterSummary;
    private long? activeJobId;
    private string? activeJobStatus;
    private PeriodicTimer? pollTimer;
    private CancellationTokenSource? pollCts;
    private readonly CancellationTokenSource disposeCts = new();

    private string ProcedureName => PricingOptions.Value.PricingStoredProcedure;
    private string SourceSystemsProcedureName => PricingOptions.Value.GetAllSourceSystemsStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(PricingOptions.Value.ConnectionStringName, ProcedureName);
    private string FullyQualifiedSourceSystemsProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(PricingOptions.Value.ConnectionStringName, SourceSystemsProcedureName);
    private string QueryDisplayText => string.IsNullOrWhiteSpace(parsedQuery) ? string.Empty : parsedQuery;
    private string SelectedPnlDateText => selectedPnlDate.HasValue
        ? selectedPnlDate.Value.ToString(DisplayDateFormat, CultureInfo.InvariantCulture)
        : "-";
    private string LastRunText => lastRunAt.HasValue
        ? lastRunAt.Value.ToString(DisplayDateTimeFormat, CultureInfo.InvariantCulture)
        : "-";
    private string SavedParameterSummaryText => string.IsNullOrWhiteSpace(savedParameterSummary) ? "Current source system selection" : savedParameterSummary;
    private bool IsJobActive => MonitoringJobHelper.IsActiveStatus(activeJobStatus);
    private bool AreAllSourceSystemsSelected => sourceSystems.Count > 0 && sourceSystems.All(static sourceSystem => sourceSystem.IsSelected);
    private int SelectedSourceSystemsCount => sourceSystems.Count(static sourceSystem => sourceSystem.IsSelected);
    private string SelectedSourceSystemsCountText => sourceSystems.Count == 0
        ? "0 / 0"
        : $"{SelectedSourceSystemsCount} / {sourceSystems.Count}";
    private string SelectedSourceSystemsSummary => BuildSelectedSourceSystemsSummary();

    protected override async Task OnInitializedAsync()
    {
        await LoadPnlDatesAsync();
        PnlDateState.OnDateChanged += OnGlobalPnlDateChanged;
        await LoadSourceSystemsAsync();
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

    private async Task LoadSourceSystemsAsync()
    {
        isLoadingSourceSystems = true;
        sourceSystemsError = null;

        try
        {
            var availableSourceSystems = await Repository.GetSourceSystemsAsync(CancellationToken.None);
            sourceSystems.Clear();
            sourceSystems.AddRange(availableSourceSystems.Select(static sourceSystem => new SourceSystemSelection(sourceSystem.Code, true)));
        }
        catch (Exception ex)
        {
            Logger.LogError(
                AppLogEvents.MonitoringLoadFailed,
                ex,
                "Failed to load Pricing source systems from procedure {ProcedureName}.",
                SourceSystemsProcedureName);
            sourceSystemsError = SourceSystemsLoadErrorMessage;
            sourceSystems.Clear();
        }
        finally
        {
            isLoadingSourceSystems = false;
        }
    }

    private async Task OnPnlDateSelected(DateOnly date)
    {
        selectedPnlDate = date;
        validationError = null;
        runError = null;
        await RestoreLatestJobAsync();
    }

    private void OnAllSourceSystemsChanged(ChangeEventArgs args)
    {
        var isSelected = (bool)(args.Value ?? false);
        foreach (var sourceSystem in sourceSystems)
        {
            sourceSystem.IsSelected = isSelected;
        }
    }

    private void OnSourceSystemChanged(string code, bool isSelected)
    {
        var sourceSystem = sourceSystems.FirstOrDefault(item => string.Equals(item.Code, code, StringComparison.OrdinalIgnoreCase));
        if (sourceSystem is null)
        {
            return;
        }

        sourceSystem.IsSelected = isSelected;
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
                "Pricing",
                selectedPnlDate.Value,
                MonitoringJobHelper.SerializeParameters(BuildCurrentParameters()),
                SelectedSourceSystemsSummary,
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
                "Failed to load Pricing for PnlDate {PnlDate}.",
                selectedPnlDate.Value);
            runError = PricingLoadErrorMessage;
            parsedQuery = string.Empty;
            result = null;
        }
        finally
        {
            isLoading = false;
        }
    }

    private DataValidationJobParameters BuildCurrentParameters() => new(GetSelectedSourceSystemCodes());

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
            Logger.LogWarning(ex, "Unable to restore latest Pricing job for PnlDate {PnlDate}.", selectedPnlDate.Value);
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

        var latestExecution = job.CompletedAt ?? job.StartedAt ?? job.EnqueuedAt;
        lastRunAt = JvCalculationHelper.ToUtc(latestExecution).ToLocalTime();

        if (string.Equals(job.Status, "Failed", StringComparison.OrdinalIgnoreCase) && result is null)
        {
            runError = string.IsNullOrWhiteSpace(job.ErrorMessage) ? PricingLoadErrorMessage : job.ErrorMessage;
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
            Logger.LogWarning(ex, "Unable to copy Pricing SQL statement to clipboard.");
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

        var columns = new List<GridColumn>(result.Columns.Count);
        for (var i = 0; i < result.Columns.Count; i++)
        {
            columns.Add(new GridColumn(result.Columns[i], i));
        }

        return columns;
    }

    private string BuildSelectedSourceSystemsSummary()
    {
        if (sourceSystems.Count == 0)
        {
            return isLoadingSourceSystems
                ? "Loading source systems..."
                : "No source systems loaded";
        }

        var selectedCodes = sourceSystems
            .Where(static sourceSystem => sourceSystem.IsSelected)
            .Select(static sourceSystem => sourceSystem.Code)
            .ToList();

        if (selectedCodes.Count == 0)
        {
            return "No source systems selected";
        }

        if (selectedCodes.Count == sourceSystems.Count)
        {
            return "All source systems";
        }

        return selectedCodes.Count <= 3
            ? string.Join(", ", selectedCodes)
            : $"{selectedCodes.Count} source systems selected";
    }

    private string? GetSelectedSourceSystemCodes()
    {
        return PricingHelper.BuildSourceSystemCodes(
            sourceSystems
                .Where(static sourceSystem => sourceSystem.IsSelected)
                .Select(static sourceSystem => sourceSystem.Code),
            quoteEachValue: true);
    }

    private static string ToHeaderLabel(string columnName) => JvCalculationHelper.ToHeaderLabel(columnName);

    private static string GetColumnAlignmentClass(string columnName) => JvCalculationHelper.GetColumnAlignmentClass(columnName);

    private static string FormatCellValue(string columnName, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "-";
        }

        var normalizedColumnName = MonitoringDisplayHelper.NormalizeColumnName(columnName);

        if ((normalizedColumnName is "pnldate" or "lastupdate" or "lastupdated" or "lastupdateddate") &&
            DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsedDate))
        {
            return parsedDate.ToString(GridDateFormat, CultureInfo.InvariantCulture);
        }

        return value;
    }

    private sealed record GridColumn(string Name, int Index);

    private sealed class SourceSystemSelection
    {
        public SourceSystemSelection(string code, bool isSelected)
        {
            Code = code;
            IsSelected = isSelected;
        }

        public string Code { get; }

        public bool IsSelected { get; set; }
    }
}