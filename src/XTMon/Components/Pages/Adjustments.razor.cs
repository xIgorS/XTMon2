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

public partial class Adjustments : ComponentBase, IDisposable
{
    private const string DisplayDateFormat = "dd-MM-yyyy";
    private const string DisplayDateTimeFormat = "dd-MM-yyyy HH:mm:ss";
    private const string GridDateFormat = "dd-MM-yyyy";
    private const string SourceSystemsLoadErrorMessage = "Unable to load Adjustments source systems right now. Please try again.";
    private const string AdjustmentsLoadErrorMessage = "Unable to load Adjustments right now. Please try again.";

    private readonly HashSet<DateOnly> availableDates = [];

    [Inject]
    private IAdjustmentsRepository Repository { get; set; } = default!;

    [Inject]
    private IJvCalculationRepository PnlDateRepository { get; set; } = default!;

    [Inject]
    private PnlDateState PnlDateState { get; set; } = default!;

    [Inject]
    private IOptions<AdjustmentsOptions> AdjustmentsOptions { get; set; } = default!;

    [Inject]
    private ILogger<Adjustments> Logger { get; set; } = default!;

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

    private string ProcedureName => AdjustmentsOptions.Value.AdjustmentsStoredProcedure;
    private string SourceSystemsProcedureName => AdjustmentsOptions.Value.GetAllSourceSystemsStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(AdjustmentsOptions.Value.ConnectionStringName, ProcedureName);
    private string FullyQualifiedSourceSystemsProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(AdjustmentsOptions.Value.ConnectionStringName, SourceSystemsProcedureName);
    private string QueryDisplayText => string.IsNullOrWhiteSpace(parsedQuery) ? string.Empty : parsedQuery;
    private string SelectedPnlDateText => selectedPnlDate.HasValue
        ? selectedPnlDate.Value.ToString(DisplayDateFormat, CultureInfo.InvariantCulture)
        : "-";
    private string LastRunText => lastRunAt.HasValue
        ? lastRunAt.Value.ToString(DisplayDateTimeFormat, CultureInfo.InvariantCulture)
        : "-";
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
        InvokeAsync(() =>
        {
            selectedPnlDate = PnlDateState.SelectedDate;
            StateHasChanged();
        });
    }

    public void Dispose()
    {
        PnlDateState.OnDateChanged -= OnGlobalPnlDateChanged;
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
                "Failed to load Adjustments source systems from procedure {ProcedureName}.",
                SourceSystemsProcedureName);
            sourceSystemsError = SourceSystemsLoadErrorMessage;
            sourceSystems.Clear();
        }
        finally
        {
            isLoadingSourceSystems = false;
        }
    }

    private Task OnPnlDateSelected(DateOnly date)
    {
        selectedPnlDate = date;
        validationError = null;
        runError = null;
        return Task.CompletedTask;
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
            var response = await Repository.GetAdjustmentsAsync(selectedPnlDate.Value, GetSelectedSourceSystemCodes(), CancellationToken.None);
            parsedQuery = response.ParsedQuery;
            result = response.Table;
            lastRunAt = DateTime.Now;
        }
        catch (Exception ex)
        {
            Logger.LogError(
                AppLogEvents.MonitoringLoadFailed,
                ex,
                "Failed to load Adjustments for PnlDate {PnlDate}.",
                selectedPnlDate.Value);
            runError = AdjustmentsLoadErrorMessage;
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
            Logger.LogWarning(ex, "Unable to copy Adjustments SQL statement to clipboard.");
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
