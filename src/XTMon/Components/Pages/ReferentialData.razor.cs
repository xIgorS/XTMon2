using System.Globalization;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.Options;
using Microsoft.JSInterop;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;

namespace XTMon.Components.Pages;

public partial class ReferentialData : ComponentBase
{
    private const string DisplayDateFormat = "dd-MM-yyyy";
    private const string DisplayDateTimeFormat = "dd-MM-yyyy HH:mm:ss";
    private const string LoadErrorMessage = "Unable to load Referential Data right now. Please try again.";

    [Inject]
    private IReferentialDataRepository Repository { get; set; } = default!;

    [Inject]
    private IOptions<ReferentialDataOptions> ReferentialOptions { get; set; } = default!;

    [Inject]
    private ILogger<ReferentialData> Logger { get; set; } = default!;

    [Inject]
    private IJSRuntime JsRuntime { get; set; } = default!;

    private DateOnly? selectedPnlDate;
    private bool isLoading;
    private bool hasRun;
    private string? validationError;
    private string? loadError;
    private string parsedQuery = string.Empty;
    private string? copyMessage;
    private bool copySucceeded;
    private DateTime? lastRunAt;
    private MonitoringTableResult? result;
    private bool showQuery;

    private string ProcedureName => ReferentialOptions.Value.CheckReferentialDataStoredProcedure;
    private string QueryDisplayText => string.IsNullOrWhiteSpace(parsedQuery) ? string.Empty : parsedQuery;
    private string SelectedPnlDateText => selectedPnlDate.HasValue
        ? selectedPnlDate.Value.ToString(DisplayDateFormat, CultureInfo.InvariantCulture)
        : "-";
    private string LastRunText => lastRunAt.HasValue
        ? lastRunAt.Value.ToString(DisplayDateTimeFormat, CultureInfo.InvariantCulture)
        : "-";

    private Task OnPnlDateSelected(DateOnly date)
    {
        selectedPnlDate = date;
        validationError = null;
        loadError = null;
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
        loadError = null;
        copyMessage = null;
        showQuery = false;

        try
        {
            var response = await Repository.GetReferentialDataAsync(selectedPnlDate.Value, CancellationToken.None);
            parsedQuery = response.ParsedQuery;
            result = response.Table;
            lastRunAt = DateTime.Now;
        }
        catch (Exception ex)
        {
            Logger.LogError(
                AppLogEvents.MonitoringLoadFailed,
                ex,
                "Failed to load Referential Data for PnlDate {PnlDate}.",
                selectedPnlDate.Value);
            loadError = LoadErrorMessage;
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
            Logger.LogWarning(ex, "Unable to copy Referential Data SQL statement to clipboard.");
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

    private static string ToHeaderLabel(string columnName) => JvCalculationHelper.ToHeaderLabel(columnName);

    private static string GetColumnAlignmentClass(string columnName) => JvCalculationHelper.GetColumnAlignmentClass(columnName);

    private static string FormatCellValue(string? value) => string.IsNullOrWhiteSpace(value) ? "-" : value;

    private sealed record GridColumn(string Name, int Index);
}