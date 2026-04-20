using System.Globalization;
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
    private IJvCalculationRepository PnlDateRepository { get; set; } = default!;

    [Inject]
    private PnlDateState PnlDateState { get; set; } = default!;

    [Inject]
    private IOptions<FunctionalRejectionOptions> FunctionalRejectionOptions { get; set; } = default!;

    [Inject]
    private ILogger<FunctionalRejection> Logger { get; set; } = default!;

    [Inject]
    private IJSRuntime JsRuntime { get; set; } = default!;

    [Inject]
    private NavigationManager NavigationManager { get; set; } = default!;

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

    private bool HasValidSelection =>
        BusinessDataTypeId.HasValue &&
        !string.IsNullOrWhiteSpace(DbConnection) &&
        !string.IsNullOrWhiteSpace(SourceSystemName) &&
        !string.IsNullOrWhiteSpace(SourceSystemBusinessDataTypeCode);

    private bool CanRun => HasValidSelection && selectedPnlDate.HasValue;
    private string MenuProcedureName => FunctionalRejectionOptions.Value.SourceSystemTechnicalRejectStoredProcedure;
    private string ProcedureName => FunctionalRejectionOptions.Value.TechnicalRejectStoredProcedure;
    private string FullyQualifiedMenuProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(FunctionalRejectionOptions.Value.MenuConnectionStringName, MenuProcedureName);
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(ResolveDetailConnectionStringName(), ProcedureName);
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
            return;
        }

        selectionError = null;
        await EnsureLoadedForCurrentSelectionAsync(force: false);
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
        await EnsureLoadedForCurrentSelectionAsync(force: true);
    }

    private async Task EnsureLoadedForCurrentSelectionAsync(bool force)
    {
        if (!HasValidSelection)
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
        await ExecuteLoadAsync();
    }

    private async Task ExecuteLoadAsync()
    {
        if (!selectedPnlDate.HasValue)
        {
            validationError = "PNL DATE is required.";
            return;
        }

        if (!BusinessDataTypeId.HasValue || string.IsNullOrWhiteSpace(SourceSystemName) || string.IsNullOrWhiteSpace(DbConnection))
        {
            selectionError = "Choose a Functional Rejection submenu item from the navigation menu.";
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
            result = await Repository.GetTechnicalRejectAsync(
                selectedPnlDate.Value,
                BusinessDataTypeId.Value,
                DbConnection,
                SourceSystemName,
                CancellationToken.None);
            parsedQuery = result.ParsedQuery;
            lastRunAt = DateTime.Now;
        }
        catch (Exception ex)
        {
            Logger.LogError(
                AppLogEvents.MonitoringLoadFailed,
                ex,
                "Failed to load Functional Rejection for DbConnection {DbConnection}, PnlDate {PnlDate}, BusinessDataTypeId {BusinessDataTypeId}, SourceSystemName {SourceSystemName}.",
                DbConnection,
                selectedPnlDate.Value,
                BusinessDataTypeId.Value,
                SourceSystemName);
            runError = LoadErrorMessage;
            parsedQuery = string.Empty;
            result = null;
        }
        finally
        {
            isLoading = false;
        }
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
        if (string.Equals(DbConnection, "DTM", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(DbConnection, "DTM_FI", StringComparison.OrdinalIgnoreCase))
        {
            return FunctionalRejectionOptions.Value.DtmConnectionStringName;
        }

        return FunctionalRejectionOptions.Value.StagingConnectionStringName;
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