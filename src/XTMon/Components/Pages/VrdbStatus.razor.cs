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

public partial class VrdbStatus : MonitoringTableJobPageBase<VrdbStatus>
{
    private const string GridDateFormat = "dd-MM-yyyy";
    private const string GridDateTimeFormat = "dd-MM-yyyy HH:mm:ss";
    private const string LoadErrorMessage = "Unable to load VRDB Status right now. Please try again.";
    protected override string MonitoringSubmenuKey => "vrdb-status";
    protected override string MonitoringJobName => "VRDB Status";
    protected override string DefaultLoadErrorMessage => LoadErrorMessage;
    private static readonly IReadOnlyList<GridColumnDefinition> PreferredColumns =
    [
        new("Pnl Date", ["PnlDate", "PNLDate", "Pnl Date"]),
        new("File Name", ["FileName", "File Name"]),
        new("Global Business Line", ["GlobalBusinessLine", "Global Business Line"]),
        new("Adjustment Type", ["AdjustmentType", "Adjustment Type"]),
        new("Recurrence Type", ["RecurrenceType", "Recurrence Type"]),
        new("Region", ["Region"]),
        new("Integration Status", ["IntegrationStatus", "Integration Status"]),
        new("Date Time Start", ["DateTimeStart", "StartDate", "Date Time Start"]),
        new("Date Time End", ["DateTimeEnd", "EndDate", "Date Time End"]),
        new("Is Reload", ["IsReload", "Is Reload"]),
        new("Is Failed", ["IsFailed", "Is Failed"]),
        new("Status", ["Status"])
    ];

    [Inject]
    private IVrdbStatusRepository Repository { get; set; } = default!;

    [Inject]
    private IOptions<VrdbStatusOptions> VrdbStatusOptions { get; set; } = default!;

    private string ProcedureName => VrdbStatusOptions.Value.VrdbStatusStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(VrdbStatusOptions.Value.ConnectionStringName, ProcedureName);
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

        if ((string.Equals(columnName, "PnlDate", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(columnName, "PNLDate", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(columnName, "Pnl Date", StringComparison.OrdinalIgnoreCase)) &&
            DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsedDate))
        {
            return parsedDate.ToString(GridDateFormat, CultureInfo.InvariantCulture);
        }

        if ((string.Equals(columnName, "StartDate", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(columnName, "EndDate", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(columnName, "DateTimeStart", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(columnName, "DateTimeEnd", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(columnName, "Date Time Start", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(columnName, "Date Time End", StringComparison.OrdinalIgnoreCase)) &&
            DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsedDateTime))
        {
            return parsedDateTime.ToString(GridDateTimeFormat, CultureInfo.InvariantCulture);
        }

        if (string.Equals(columnName, "IsReload", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(columnName, "IsFailed", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(columnName, "Is Reload", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(columnName, "Is Failed", StringComparison.OrdinalIgnoreCase))
        {
            return value switch
            {
                "1" => "Yes",
                "0" => "No",
                _ => value
            };
        }

        return value;
    }

    private sealed record GridColumnDefinition(string Header, IReadOnlyList<string> Aliases);

    private sealed record GridColumn(string Name, string Header, int Index);
}