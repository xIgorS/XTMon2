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
    private static readonly IReadOnlyList<GridColumnDefinition> PreferredColumns =
    [
        new("Pnl Date", ["PnlDate", "Pnl Date"]),
        new("Sk Portfolio", ["SkPortfolio", "portfolioid", "PortfolioId"]),
        new("Mtd Amount HO", ["MtdAmountHO", "Mtd Amount HO"]),
        new("Ytd Amount HO", ["YtdAmountHO", "Ytd Amount HO"]),
        new("Mtd Amount Paradigm", ["MtdAmountParadigm", "Mtd Amount Paradigm"]),
        new("Qtd Amount Paradigm", ["QtdAmountParadigm", "Qtd Amount Paradigm"]),
        new("Ydt Amount Paradigm", ["YtdAmountParadigm", "Ydt Amount Paradigm", "Ytd Amount Paradigm"]),
        new("Jv Check Balance", ["JvCheckBalance", "JvCheck", "Jv Check Balance"])
    ];

    private readonly HashSet<DateOnly> availableDates = [];

    [Inject]
    private IJvBalanceConsistencyRepository Repository { get; set; } = default!;

    [Inject]
    private IJvCalculationRepository PnlDateRepository { get; set; } = default!;

    [Inject]
    private PnlDateState PnlDateState { get; set; } = default!;

    [Inject]
    private IOptions<JvBalanceConsistencyOptions> JvBalanceConsistencyOptions { get; set; } = default!;

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

    private string ProcedureName => JvBalanceConsistencyOptions.Value.JvBalanceConsistencyStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(JvBalanceConsistencyOptions.Value.ConnectionStringName, ProcedureName);
    private string QueryDisplayText => string.IsNullOrWhiteSpace(parsedQuery) ? string.Empty : parsedQuery;
    private string SelectedPnlDateText => selectedPnlDate.HasValue
        ? selectedPnlDate.Value.ToString(DisplayDateFormat, CultureInfo.InvariantCulture)
        : "-";
    private string LastRunText => lastRunAt.HasValue
        ? lastRunAt.Value.ToString(DisplayDateTimeFormat, CultureInfo.InvariantCulture)
        : "-";

    protected override async Task OnInitializedAsync()
    {
        precisionText = JvBalanceConsistencyOptions.Value.Precision.ToString("0.00", CultureInfo.InvariantCulture);
        await LoadPnlDatesAsync();
        PnlDateState.OnDateChanged += OnGlobalPnlDateChanged;
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

    private Task OnPnlDateSelected(DateOnly date)
    {
        selectedPnlDate = date;
        validationError = null;
        runError = null;
        return Task.CompletedTask;
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
            var response = await Repository.GetJvBalanceConsistencyAsync(selectedPnlDate.Value, precision, CancellationToken.None);
            parsedQuery = response.ParsedQuery;
            result = response.Table;
            lastRunAt = DateTime.Now;
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