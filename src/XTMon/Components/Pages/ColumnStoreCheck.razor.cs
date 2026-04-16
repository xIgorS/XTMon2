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

public partial class ColumnStoreCheck : ComponentBase, IDisposable
{
    private const string DisplayDateFormat = "dd-MM-yyyy";
    private const string DisplayDateTimeFormat = "dd-MM-yyyy HH:mm:ss";
    private const string GridDateTimeFormat = "dd-MM-yyyy HH:mm:ss";
    private const string LoadErrorMessage = "Unable to load Column Store Check right now. Please try again.";
    private static readonly IReadOnlyList<GridColumnDefinition> PreferredColumns =
    [
        new("Status", ["Status"], true),
        new("Status Code", ["StatusCode", "Status Code"]),
        new("Created", ["DateCreated", "Created"]),
        new("Started", ["DateStarted", "Started"]),
        new("Updated", ["DateUpdated", "Updated"]),
        new("Correlation Id", ["CorrelationId", "Correlation Id"])
    ];

    private readonly HashSet<DateOnly> availableDates = [];

    [Inject]
    private IColumnStoreCheckRepository Repository { get; set; } = default!;

    [Inject]
    private IJvCalculationRepository PnlDateRepository { get; set; } = default!;

    [Inject]
    private PnlDateState PnlDateState { get; set; } = default!;

    [Inject]
    private IOptions<ColumnStoreCheckOptions> ColumnStoreCheckOptions { get; set; } = default!;

    [Inject]
    private ILogger<ColumnStoreCheck> Logger { get; set; } = default!;

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

    private string ProcedureName => ColumnStoreCheckOptions.Value.ColumnStoreCheckStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(ColumnStoreCheckOptions.Value.ConnectionStringName, ProcedureName);
    private string QueryDisplayText => string.IsNullOrWhiteSpace(parsedQuery) ? string.Empty : parsedQuery;
    private string SelectedPnlDateText => selectedPnlDate.HasValue
        ? selectedPnlDate.Value.ToString(DisplayDateFormat, CultureInfo.InvariantCulture)
        : "-";
    private string LastRunText => lastRunAt.HasValue
        ? lastRunAt.Value.ToString(DisplayDateTimeFormat, CultureInfo.InvariantCulture)
        : "-";

    protected override async Task OnInitializedAsync()
    {
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
            var response = await Repository.GetColumnStoreCheckAsync(selectedPnlDate.Value, CancellationToken.None);
            parsedQuery = response.ParsedQuery;
            result = response.Table;
            lastRunAt = DateTime.Now;
        }
        catch (Exception ex)
        {
            Logger.LogError(
                AppLogEvents.MonitoringLoadFailed,
                ex,
                "Failed to load Column Store Check for PnlDate {PnlDate}.",
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
            Logger.LogWarning(ex, "Unable to copy Column Store Check SQL statement to clipboard.");
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
            if (definition.IsDerived)
            {
                columns.Add(new GridColumn(definition.Header, definition.Header, -1, true));
                continue;
            }

            foreach (var alias in definition.Aliases)
            {
                if (!sourceIndexes.TryGetValue(alias, out var index))
                {
                    continue;
                }

                columns.Add(new GridColumn(alias, definition.Header, index, false));
                break;
            }
        }

        return columns;
    }

    private string? GetCellValue(GridColumn column, IReadOnlyList<string?> row)
    {
        if (result is null)
        {
            return null;
        }

        if (!column.IsDerived)
        {
            return column.Index >= 0 && column.Index < row.Count ? row[column.Index] : null;
        }

        var statusCode = GetSourceValue(row, "StatusCode", "Status Code");
        var dateUpdated = GetSourceValue(row, "DateUpdated", "Updated");
        return string.Equals(statusCode, "Executed", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(dateUpdated)
            ? "OK"
            : "KO";
    }

    private string? GetSourceValue(IReadOnlyList<string?> row, params string[] aliases)
    {
        if (result is null)
        {
            return null;
        }

        for (var i = 0; i < result.Columns.Count; i++)
        {
            var columnName = result.Columns[i];
            if (!aliases.Any(alias => string.Equals(alias, columnName, StringComparison.OrdinalIgnoreCase)))
            {
                continue;
            }

            return i < row.Count ? row[i] : null;
        }

        return null;
    }

    private static string GetColumnAlignmentClass(string columnName) => JvCalculationHelper.GetColumnAlignmentClass(columnName);

    private static string FormatCellValue(string columnName, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "-";
        }

        if ((string.Equals(columnName, "DateCreated", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(columnName, "DateStarted", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(columnName, "DateUpdated", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(columnName, "Created", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(columnName, "Started", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(columnName, "Updated", StringComparison.OrdinalIgnoreCase)) &&
            DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsedDate))
        {
            return parsedDate.ToString(GridDateTimeFormat, CultureInfo.InvariantCulture);
        }

        return value;
    }

    private sealed record GridColumnDefinition(string Header, IReadOnlyList<string> Aliases, bool IsDerived = false);

    private sealed record GridColumn(string Name, string Header, int Index, bool IsDerived);
}