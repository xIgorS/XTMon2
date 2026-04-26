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

public partial class FunctionalRejection : MonitoringJobPageBase<FunctionalRejection>
{
    private const string GridDateFormat = "dd-MM-yyyy";
    private const string GridDateTimeFormat = "dd-MM-yyyy HH:mm:ss";
    private const string LoadErrorMessage = "Unable to load Functional Rejection right now. Please try again.";

    protected override string MonitoringCategory => MonitoringJobHelper.FunctionalRejectionCategory;
    protected override bool ShouldRestoreAfterPnlDateSelected => false;
    protected override bool ShouldRestoreOnGlobalPnlDateChanged => string.IsNullOrWhiteSpace(PnlDate);
    protected override string MonitoringSubmenuKey => BuildCurrentSubmenuKey();
    protected override string MonitoringJobName => SourceSystemBusinessDataTypeCode ?? "Functional Rejection";
    protected override string DefaultLoadErrorMessage => LoadErrorMessage;

    [Inject]
    private IFunctionalRejectionRepository Repository { get; set; } = default!;

    [Inject]
    private IOptions<FunctionalRejectionOptions> FunctionalRejectionOptions { get; set; } = default!;

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

    private string? selectionError;
    private string parsedQuery = string.Empty;
    private string? copyMessage;
    private bool copySucceeded;
    private bool showQuery;
    private TechnicalRejectResult? result;
    private string? lastAutoLoadKey;
    private DateTime? activeJobEnqueuedAt;
    private DateTime? activeJobStartedAt;
    private DateTime? activeJobCompletedAt;
    private string? savedParameterSummary;
    private IReadOnlyList<FunctionalRejectionMenuItem>? menuItemsForValidation;

    private bool HasValidSelection =>
        BusinessDataTypeId.HasValue &&
        !string.IsNullOrWhiteSpace(DbConnection) &&
        !string.IsNullOrWhiteSpace(SourceSystemName) &&
        !string.IsNullOrWhiteSpace(SourceSystemBusinessDataTypeCode);

    private bool CanRun => HasValidSelection && selectedPnlDate.HasValue;
    private string MenuProcedureName => FunctionalRejectionOptions.Value.SourceSystemTechnicalRejectStoredProcedure;
    private string ProcedureName => FunctionalRejectionOptions.Value.TechnicalRejectStoredProcedure;
    private string FullyQualifiedMenuProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(FunctionalRejectionOptions.Value.MenuConnectionStringName, MenuProcedureName);
    private string FullyQualifiedProcedureName => TryResolveDetailConnectionStringName(out var connectionStringName)
        ? JvCalculationHelper.BuildFullyQualifiedProcedureName(connectionStringName, ProcedureName)
        : "-";
    private string QueryDisplayText => string.IsNullOrWhiteSpace(parsedQuery) ? string.Empty : parsedQuery;
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

    protected override DateOnly? GetCurrentPnlDateFromState()
    {
        return ResolveSelectedPnlDate();
    }

    protected override bool CanRestoreLatestJob() => HasValidSelection;

    protected override async Task<bool> ValidateBeforeRunAsync()
    {
        await ValidateCurrentSelectionAsync(disposeCts.Token);
        return string.IsNullOrWhiteSpace(selectionError);
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
            await StopPollingAsync();
            ClearLoadedState();
            return;
        }

        if (!await ValidateCurrentSelectionAsync(disposeCts.Token))
        {
            return;
        }

        await EnsureLoadedForCurrentSelectionAsync(force: false);
    }

    protected override Task OnPnlDateSelectedCoreAsync()
    {
        validationError = null;
        runError = null;
        if (selectedPnlDate.HasValue)
        {
            UpdatePnlDateQuery(selectedPnlDate.Value);
        }

        return Task.CompletedTask;
    }

    protected override async Task<bool> TryHandleRunExceptionAsync(Exception exception)
    {
        if (exception is not SqlException sqlException || !SqlDataHelper.IsMissingStoredProcedure(sqlException) || !selectedPnlDate.HasValue)
        {
            return false;
        }

        var businessDataTypeId = BusinessDataTypeId ?? 0;

        Logger.LogWarning(sqlException,
            "Monitoring job procedures are unavailable. Falling back to direct Functional Rejection execution for DbConnection {DbConnection}, PnlDate {PnlDate}, BusinessDataTypeId {BusinessDataTypeId}, SourceSystemName {SourceSystemName}.",
            DbConnection,
            selectedPnlDate.Value,
            businessDataTypeId,
            SourceSystemName);

        await LoadDirectResultAsync(selectedPnlDate.Value, businessDataTypeId, disposeCts.Token);
        return true;
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

    protected override bool TryPrepareRun(out string? parametersJson, out string? parameterSummary)
    {
        if (!CanRun)
        {
            validationError = "PNL DATE is required.";
            parametersJson = null;
            parameterSummary = null;
            return false;
        }

        var parameters = BuildCurrentParameters();
        parametersJson = MonitoringJobHelper.SerializeParameters(parameters);
        parameterSummary = MonitoringJobHelper.BuildFunctionalRejectionParameterSummary(parameters);
        return true;
    }

    protected override void OnBeforeRun()
    {
        copyMessage = null;
        showQuery = false;
    }

    protected override void LogRunFailure(Exception exception)
    {
        Logger.LogError(
            AppLogEvents.MonitoringLoadFailed,
            exception,
            "Failed to enqueue Functional Rejection for DbConnection {DbConnection}, PnlDate {PnlDate}, BusinessDataTypeId {BusinessDataTypeId}, SourceSystemName {SourceSystemName}.",
            DbConnection,
            selectedPnlDate,
            BusinessDataTypeId,
            SourceSystemName);
    }

    protected override bool TryHandleRestoreException(Exception exception)
    {
        if (exception is SqlException sqlException && SqlDataHelper.IsMissingStoredProcedure(sqlException))
        {
            ClearLoadedState();
            return true;
        }

        return false;
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
        await RestoreLatestJobAsync();
    }

    protected override void ApplyJobCore(MonitoringJobRecord job)
    {
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
    }

    protected override void ClearLoadedStateCore()
    {
        activeJobEnqueuedAt = null;
        activeJobStartedAt = null;
        activeJobCompletedAt = null;
        savedParameterSummary = null;
        lastAutoLoadKey = null;
        parsedQuery = string.Empty;
        result = null;
    }

    protected override bool HasLoadedResult() => result is not null;

    private async Task<bool> ValidateCurrentSelectionAsync(CancellationToken cancellationToken)
    {
        if (!HasValidSelection)
        {
            selectionError = "Choose a Functional Rejection submenu item from the navigation menu.";
            await StopPollingAsync();
            ClearLoadedState();
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
            await StopPollingAsync();
            ClearLoadedState();
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
            await StopPollingAsync();
            ClearLoadedState();
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

        var targetUri = FunctionalRejectionUrlHelper.BuildHref(
            SourceSystemBusinessDataTypeCode,
            BusinessDataTypeId,
            SourceSystemName,
            DbConnection,
            nextPnlDate);
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