using System.Globalization;
using Microsoft.Data.SqlClient;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Options;
using Microsoft.JSInterop;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;
using XTMon.Services;

namespace XTMon.Components.Pages;

public partial class FunctionalRejection : ComponentBase, IDisposable
{
    private const string DisplayDateFormat = "dd-MM-yyyy";
    private const string DisplayDateTimeFormat = "dd-MM-yyyy HH:mm:ss";
    private const string GridDateFormat = "dd-MM-yyyy";
    private const string GridDateTimeFormat = "dd-MM-yyyy HH:mm:ss";
    private const string LoadErrorMessage = "Unable to load Functional Rejection right now. Please try again.";

    private readonly HashSet<DateOnly> availableDates = [];

    [Inject]
    private IFunctionalRejectionRepository Repository { get; set; } = default!;

    [Inject]
    private IMonitoringJobRepository MonitoringJobRepository { get; set; } = default!;

    [Inject]
    private IJvCalculationRepository PnlDateRepository { get; set; } = default!;

    [Inject]
    private PnlDateState PnlDateState { get; set; } = default!;

    [Inject]
    private IOptions<FunctionalRejectionOptions> FunctionalRejectionOptions { get; set; } = default!;

    [Inject]
    private IOptions<MonitoringJobsOptions> MonitoringJobsOptions { get; set; } = default!;

    [Inject]
    private ILogger<FunctionalRejection> Logger { get; set; } = default!;

    [Inject]
    private IJSRuntime JsRuntime { get; set; } = default!;

    [Inject]
    private NavigationManager NavigationManager { get; set; } = default!;

    [Inject]
    private FunctionalRejectionMenuState FunctionalRejectionMenuState { get; set; } = default!;

    [Parameter]
    [SupplyParameterFromQuery(Name = "code")]
    public string? SourceSystemBusinessDataTypeCode { get; set; }

    [Parameter]
    [SupplyParameterFromQuery(Name = "businessDatatypeId")]
    public int? BusinessDataTypeId { get; set; }

    [Parameter]
    [SupplyParameterFromQuery(Name = "sourceSystemName")]
    public string? SourceSystemName { get; set; }

    [Parameter]
    [SupplyParameterFromQuery(Name = "dbConnection")]
    public string? DbConnection { get; set; }

    [Parameter]
    [SupplyParameterFromQuery(Name = "pnlDate")]
    public string? PnlDate { get; set; }

    private DateOnly? selectedPnlDate;
    private bool isLoading;
    private bool hasRun;
    private string? validationError;
    private string? selectionError;
    private string? runError;
    private string parsedQuery = string.Empty;
    private string? copyMessage;
    private bool copySucceeded;
    private bool showQuery;
    private DateTime? lastRunAt;
    private TechnicalRejectResult? result;
    private string? lastAutoLoadKey;
    private string? activeJobStatus;
    private long? activeJobId;
    private DateTime? activeJobEnqueuedAt;
    private DateTime? activeJobStartedAt;
    private DateTime? activeJobCompletedAt;
    private string? savedParameterSummary;
    private IReadOnlyList<FunctionalRejectionMenuItem>? menuItemsForValidation;
    private PeriodicTimer? pollTimer;
    private CancellationTokenSource? pollCts;
    private readonly CancellationTokenSource disposeCts = new();

    private bool HasValidSelection =>
        BusinessDataTypeId.HasValue &&
        !string.IsNullOrWhiteSpace(DbConnection) &&
        !string.IsNullOrWhiteSpace(SourceSystemName) &&
        !string.IsNullOrWhiteSpace(SourceSystemBusinessDataTypeCode);

    private bool CanRun => HasValidSelection && selectedPnlDate.HasValue;
    private bool IsJobActive => MonitoringJobHelper.IsActiveStatus(activeJobStatus);
    private string MenuProcedureName => FunctionalRejectionOptions.Value.SourceSystemTechnicalRejectStoredProcedure;
    private string ProcedureName => FunctionalRejectionOptions.Value.TechnicalRejectStoredProcedure;
    private string FullyQualifiedMenuProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(FunctionalRejectionOptions.Value.MenuConnectionStringName, MenuProcedureName);
    private string FullyQualifiedProcedureName => TryResolveDetailConnectionStringName(out var connectionStringName)
        ? JvCalculationHelper.BuildFullyQualifiedProcedureName(connectionStringName, ProcedureName)
        : "-";
    private string QueryDisplayText => string.IsNullOrWhiteSpace(parsedQuery) ? string.Empty : parsedQuery;
    private string SelectedPnlDateText => selectedPnlDate.HasValue
        ? selectedPnlDate.Value.ToString(DisplayDateFormat, CultureInfo.InvariantCulture)
        : "-";
    private string LastRunText => lastRunAt.HasValue
        ? lastRunAt.Value.ToString(DisplayDateTimeFormat, CultureInfo.InvariantCulture)
        : "-";
    private string SelectedCodeText => string.IsNullOrWhiteSpace(SourceSystemBusinessDataTypeCode)
        ? "-"
        : SourceSystemBusinessDataTypeCode;
    private string SelectedSourceSystemText => string.IsNullOrWhiteSpace(SourceSystemName)
        ? "-"
        : SourceSystemName;
    private string SelectedBusinessDataTypeIdText => BusinessDataTypeId.HasValue
        ? BusinessDataTypeId.Value.ToString(CultureInfo.InvariantCulture)
        : "-";
    private string JobStatusText => string.IsNullOrWhiteSpace(activeJobStatus) ? "-" : activeJobStatus;
    private string SavedParameterSummaryText => string.IsNullOrWhiteSpace(savedParameterSummary) ? "Current submenu selection" : savedParameterSummary;

    protected override async Task OnInitializedAsync()
    {
        await LoadPnlDatesAsync();
        PnlDateState.OnDateChanged += OnGlobalPnlDateChanged;
    }

    protected override async Task OnParametersSetAsync()
    {
        selectedPnlDate = ResolveSelectedPnlDate();

        if (!HasValidSelection)
        {
            selectionError = "Choose a Functional Rejection submenu item from the navigation menu.";
            runError = null;
            validationError = null;
            hasRun = false;
            result = null;
            parsedQuery = string.Empty;
            lastAutoLoadKey = null;
            savedParameterSummary = null;
            activeJobStatus = null;
            activeJobId = null;
            StopPolling();
            return;
        }

        if (!await ValidateCurrentSelectionAsync(disposeCts.Token))
        {
            return;
        }

        await EnsureLoadedForCurrentSelectionAsync(force: false);
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
            if (!string.IsNullOrWhiteSpace(PnlDate))
            {
                return;
            }

            selectedPnlDate = PnlDateState.SelectedDate;
            await EnsureLoadedForCurrentSelectionAsync(force: true);
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

    private Task OnPnlDateSelected(DateOnly date)
    {
        selectedPnlDate = date;
        validationError = null;
        runError = null;
        UpdatePnlDateQuery(date);
        return Task.CompletedTask;
    }

    private async Task RunAsync()
    {
        if (!CanRun || !selectedPnlDate.HasValue)
        {
            validationError = "PNL DATE is required.";
            return;
        }

        var businessDataTypeId = BusinessDataTypeId ?? 0;

        if (!await ValidateCurrentSelectionAsync(disposeCts.Token))
        {
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
            var parameters = BuildCurrentParameters();
            var enqueueResult = await MonitoringJobRepository.EnqueueMonitoringJobAsync(
                MonitoringJobHelper.FunctionalRejectionCategory,
                BuildCurrentSubmenuKey(),
                SourceSystemBusinessDataTypeCode,
                selectedPnlDate.Value,
                MonitoringJobHelper.SerializeParameters(parameters),
                MonitoringJobHelper.BuildFunctionalRejectionParameterSummary(parameters),
                disposeCts.Token);

            activeJobId = enqueueResult.JobId;
            await RefreshActiveJobAsync(disposeCts.Token);
            StartPollingIfNeeded();
        }
        catch (SqlException ex) when (SqlDataHelper.IsMissingStoredProcedure(ex))
        {
            Logger.LogWarning(ex,
                "Monitoring job procedures are unavailable. Falling back to direct Functional Rejection execution for DbConnection {DbConnection}, PnlDate {PnlDate}, BusinessDataTypeId {BusinessDataTypeId}, SourceSystemName {SourceSystemName}.",
                DbConnection,
                selectedPnlDate.Value,
                businessDataTypeId,
                SourceSystemName);

            await LoadDirectResultAsync(selectedPnlDate.Value, businessDataTypeId, disposeCts.Token);
        }
        catch (Exception ex)
        {
            Logger.LogError(
                AppLogEvents.MonitoringLoadFailed,
                ex,
                "Failed to enqueue Functional Rejection for DbConnection {DbConnection}, PnlDate {PnlDate}, BusinessDataTypeId {BusinessDataTypeId}, SourceSystemName {SourceSystemName}.",
                DbConnection,
                selectedPnlDate.Value,
                businessDataTypeId,
                SourceSystemName);
            runError = LoadErrorMessage;
        }
        finally
        {
            isLoading = false;
        }
    }

    private async Task LoadDirectResultAsync(DateOnly pnlDate, int businessDataTypeId, CancellationToken cancellationToken)
    {
        var directResult = await Repository.GetTechnicalRejectAsync(
            pnlDate,
            businessDataTypeId,
            DbConnection ?? string.Empty,
            SourceSystemName ?? string.Empty,
            cancellationToken);

        activeJobId = null;
        activeJobStatus = null;
        activeJobEnqueuedAt = null;
        activeJobStartedAt = null;
        activeJobCompletedAt = null;
        savedParameterSummary = MonitoringJobHelper.BuildFunctionalRejectionParameterSummary(BuildCurrentParameters());
        parsedQuery = directResult.ParsedQuery;
        result = directResult;
        lastRunAt = DateTime.Now;
        runError = null;
    }

    private async Task EnsureLoadedForCurrentSelectionAsync(bool force)
    {
        if (!HasValidSelection)
        {
            return;
        }

        if (!await ValidateCurrentSelectionAsync(disposeCts.Token))
        {
            return;
        }

        if (!selectedPnlDate.HasValue)
        {
            validationError = "PNL DATE is required.";
            return;
        }

        var requestKey = string.Create(
            CultureInfo.InvariantCulture,
            $"{selectedPnlDate.Value:yyyy-MM-dd}|{BusinessDataTypeId}|{DbConnection}|{SourceSystemName}|{SourceSystemBusinessDataTypeCode}");

        if (!force && string.Equals(lastAutoLoadKey, requestKey, StringComparison.Ordinal))
        {
            return;
        }

        lastAutoLoadKey = requestKey;
        await RestoreLatestJobForCurrentSelectionAsync();
    }

    private async Task RestoreLatestJobForCurrentSelectionAsync()
    {
        StopPolling();

        if (!selectedPnlDate.HasValue)
        {
            ClearLoadedState();
            return;
        }

        if (!BusinessDataTypeId.HasValue || string.IsNullOrWhiteSpace(SourceSystemName) || string.IsNullOrWhiteSpace(DbConnection))
        {
            ClearLoadedState();
            return;
        }

        try
        {
            var latestJob = await MonitoringJobRepository.GetLatestMonitoringJobAsync(
                MonitoringJobHelper.FunctionalRejectionCategory,
                BuildCurrentSubmenuKey(),
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
        catch (SqlException ex) when (SqlDataHelper.IsMissingStoredProcedure(ex))
        {
            ClearLoadedState();
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Unable to restore latest Functional Rejection job for DbConnection {DbConnection}, PnlDate {PnlDate}, BusinessDataTypeId {BusinessDataTypeId}, SourceSystemName {SourceSystemName}.",
                DbConnection,
                selectedPnlDate.Value,
                BusinessDataTypeId.Value,
                SourceSystemName);
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
        activeJobEnqueuedAt = job.EnqueuedAt;
        activeJobStartedAt = job.StartedAt;
        activeJobCompletedAt = job.CompletedAt;
        savedParameterSummary = job.ParameterSummary;

        var table = JvCalculationHelper.DeserializeMonitoringTable(job.GridColumnsJson, job.GridRowsJson);
        var columns = MonitoringJobHelper.DeserializeTechnicalRejectColumns(job.MetadataJson);
        if (table is null)
        {
            result = null;
        }
        else
        {
            var effectiveColumns = columns.Count == table.Columns.Count
                ? columns
                : table.Columns.Select(static columnName => new TechnicalRejectColumn(columnName, string.Empty)).ToArray();
            result = new TechnicalRejectResult(job.ParsedQuery ?? string.Empty, effectiveColumns, table.Rows);
        }

        parsedQuery = job.ParsedQuery ?? string.Empty;
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
        activeJobEnqueuedAt = null;
        activeJobStartedAt = null;
        activeJobCompletedAt = null;
        savedParameterSummary = null;
        hasRun = false;
        lastAutoLoadKey = null;
        lastRunAt = null;
        parsedQuery = string.Empty;
        result = null;
        runError = null;
    }

    private async Task<bool> ValidateCurrentSelectionAsync(CancellationToken cancellationToken)
    {
        if (!HasValidSelection)
        {
            selectionError = "Choose a Functional Rejection submenu item from the navigation menu.";
            ClearLoadedState();
            StopPolling();
            return false;
        }

        try
        {
            var menuItems = await GetMenuItemsForValidationAsync(cancellationToken);
            if (FunctionalRejectionSelectionHelper.ContainsSelection(
                menuItems,
                SourceSystemBusinessDataTypeCode,
                BusinessDataTypeId,
                SourceSystemName,
                DbConnection))
            {
                selectionError = null;
                return true;
            }

            selectionError = "Choose a valid Functional Rejection submenu item from the navigation menu.";
            ClearLoadedState();
            StopPolling();
            return false;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Unable to validate Functional Rejection selection from navigation parameters.");
            selectionError = "Unable to validate Functional Rejection selection right now.";
            ClearLoadedState();
            StopPolling();
            return false;
        }
    }

    private async Task<IReadOnlyList<FunctionalRejectionMenuItem>> GetMenuItemsForValidationAsync(CancellationToken cancellationToken)
    {
        if (menuItemsForValidation is not null)
        {
            return menuItemsForValidation;
        }

        await FunctionalRejectionMenuState.RefreshAsync(cancellationToken);
        if (!string.IsNullOrWhiteSpace(FunctionalRejectionMenuState.ErrorMessage)
            && FunctionalRejectionMenuState.MenuItems.Count == 0)
        {
            throw new InvalidOperationException(FunctionalRejectionMenuState.ErrorMessage);
        }

        menuItemsForValidation = FunctionalRejectionMenuState.MenuItems;
        return menuItemsForValidation;
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
            Logger.LogWarning(ex, "Unable to copy Functional Rejection SQL statement to clipboard.");
            copyMessage = "Failed to copy SQL to clipboard.";
            copySucceeded = false;
        }
    }

    private DateOnly? ResolveSelectedPnlDate()
    {
        if (DateOnly.TryParseExact(PnlDate, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var routeDate))
        {
            return routeDate;
        }

        return PnlDateState.SelectedDate;
    }

    private void UpdatePnlDateQuery(DateOnly date)
    {
        var currentUri = NavigationManager.ToAbsoluteUri(NavigationManager.Uri);
        var currentQuery = QueryHelpers.ParseQuery(currentUri.Query);
        var currentPnlDate = currentQuery.TryGetValue("pnlDate", out var pnlDateValue)
            ? pnlDateValue.ToString()
            : null;
        var nextPnlDate = date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

        if (string.Equals(currentPnlDate, nextPnlDate, StringComparison.Ordinal))
        {
            return;
        }

        var queryParameters = new Dictionary<string, string?>
        {
            ["code"] = SourceSystemBusinessDataTypeCode,
            ["businessDatatypeId"] = BusinessDataTypeId?.ToString(CultureInfo.InvariantCulture),
            ["sourceSystemName"] = SourceSystemName,
            ["dbConnection"] = DbConnection,
            ["pnlDate"] = nextPnlDate
        };

        var targetUri = QueryHelpers.AddQueryString("functional-rejection", queryParameters);
        NavigationManager.NavigateTo(targetUri, replace: true);
    }

    private string BuildCurrentSubmenuKey()
    {
        return MonitoringJobHelper.BuildFunctionalRejectionSubmenuKey(
            BusinessDataTypeId ?? 0,
            SourceSystemName ?? string.Empty,
            DbConnection ?? string.Empty,
            SourceSystemBusinessDataTypeCode);
    }

    private FunctionalRejectionJobParameters BuildCurrentParameters()
    {
        return new FunctionalRejectionJobParameters(
            SourceSystemBusinessDataTypeCode,
            BusinessDataTypeId ?? 0,
            SourceSystemName ?? string.Empty,
            DbConnection ?? string.Empty);
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
            var column = result.Columns[i];
            columns.Add(new GridColumn(
                column.Name,
                column.TypeName,
                BuildColumnHeader(column),
                i));
        }

        return columns;
    }

    private static string BuildColumnHeader(TechnicalRejectColumn column)
    {
        return string.IsNullOrWhiteSpace(column.TypeName)
            ? column.Name
            : $"{column.Name} ({column.TypeName})";
    }

    private string ResolveDetailConnectionStringName()
    {
        if (TryResolveDetailConnectionStringName(out var connectionStringName))
        {
            return connectionStringName;
        }

        throw new InvalidOperationException($"Unsupported Functional Rejection dbconnexion value '{DbConnection}'.");
    }

    private bool TryResolveDetailConnectionStringName(out string connectionStringName)
    {
        if (string.Equals(DbConnection, "DTM", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(DbConnection, "DTM_FI", StringComparison.OrdinalIgnoreCase))
        {
            connectionStringName = FunctionalRejectionOptions.Value.DtmConnectionStringName;
            return true;
        }

        if (string.Equals(DbConnection, "STAGING", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(DbConnection, "STAGING_FI_ALMT", StringComparison.OrdinalIgnoreCase))
        {
            connectionStringName = FunctionalRejectionOptions.Value.StagingConnectionStringName;
            return true;
        }

        connectionStringName = string.Empty;
        return false;
    }

    private static string GetColumnAlignmentClass(string columnName) => JvCalculationHelper.GetColumnAlignmentClass(columnName);

    private static string FormatCellValue(string columnName, string typeName, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "-";
        }

        var normalizedTypeName = typeName.Trim();
        if (normalizedTypeName.Contains("datetime", StringComparison.OrdinalIgnoreCase) &&
            DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsedDateTime))
        {
            return parsedDateTime.ToString(GridDateTimeFormat, CultureInfo.InvariantCulture);
        }

        if ((normalizedTypeName.Contains("date", StringComparison.OrdinalIgnoreCase) || MonitoringDisplayHelper.IsDateLikeColumn(columnName)) &&
            DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsedDate))
        {
            return parsedDate.ToString(GridDateFormat, CultureInfo.InvariantCulture);
        }

        return value;
    }

    private sealed record GridColumn(string Name, string TypeName, string Header, int Index);
}