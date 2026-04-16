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

public partial class Mirrorization : ComponentBase
{
    private const string DisplayDateFormat = "dd-MM-yyyy";
    private const string DisplayDateTimeFormat = "dd-MM-yyyy HH:mm:ss";
    private const string GridDateFormat = "dd-MM-yyyy";
    private const string LoadErrorMessage = "Unable to load Mirrorization right now. Please try again.";

    private readonly HashSet<DateOnly> availableDates = [];

    [Inject]
    private IMirrorizationRepository Repository { get; set; } = default!;

    [Inject]
    private IJvCalculationRepository PnlDateRepository { get; set; } = default!;

    [Inject]
    private IOptions<MirrorizationOptions> MirrorizationOptions { get; set; } = default!;

    [Inject]
    private ILogger<Mirrorization> Logger { get; set; } = default!;

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

    private string ProcedureName => MirrorizationOptions.Value.MirrorizationStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(MirrorizationOptions.Value.ConnectionStringName, ProcedureName);
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
    }

    private async Task LoadPnlDatesAsync()
    {
        try
        {
            var response = await PnlDateRepository.GetJvPnlDatesAsync(CancellationToken.None);

            availableDates.Clear();
            foreach (var date in response.AvailableDates)
            {
                availableDates.Add(date);
            }

            var selectedDate = response.DefaultDate;
            if (!selectedDate.HasValue && response.AvailableDates.Count > 0)
            {
                selectedDate = response.AvailableDates[0];
            }

            if (selectedDate.HasValue)
            {
                selectedPnlDate = selectedDate.Value;
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Unable to load default PNL dates for Mirrorization.");
        }
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
            var response = await Repository.GetMirrorizationAsync(selectedPnlDate.Value, CancellationToken.None);
            parsedQuery = response.ParsedQuery;
            result = response.Table;
            lastRunAt = DateTime.Now;
        }
        catch (Exception ex)
        {
            Logger.LogError(
                AppLogEvents.MonitoringLoadFailed,
                ex,
                "Failed to load Mirrorization for PnlDate {PnlDate}.",
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
            Logger.LogWarning(ex, "Unable to copy Mirrorization SQL statement to clipboard.");
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

    private sealed record GridColumn(string Name, int Index);
}