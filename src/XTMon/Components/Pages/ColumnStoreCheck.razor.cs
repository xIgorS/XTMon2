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

public partial class ColumnStoreCheck : MonitoringTableJobPageBase<ColumnStoreCheck>
{
    private const string GridDateTimeFormat = "dd-MM-yyyy HH:mm:ss";
    private const string LoadErrorMessage = "Unable to load Column Store Check right now. Please try again.";
    protected override string MonitoringSubmenuKey => "column-store-check";
    protected override string MonitoringJobName => "Column Store Check";
    protected override string DefaultLoadErrorMessage => LoadErrorMessage;
    private static readonly IReadOnlyList<GridColumnDefinition> PreferredColumns =
    [
        new("Status", ["Status"], true),
        new("Status Code", ["StatusCode", "Status Code"]),
        new("Created", ["DateCreated", "Created"]),
        new("Started", ["DateStarted", "Started"]),
        new("Updated", ["DateUpdated", "Updated"]),
        new("Correlation Id", ["CorrelationId", "Correlation Id"])
    ];

    [Inject]
    private IOptions<ColumnStoreCheckOptions> ColumnStoreCheckOptions { get; set; } = default!;

    private string ProcedureName => ColumnStoreCheckOptions.Value.ColumnStoreCheckStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(ColumnStoreCheckOptions.Value.ConnectionStringName, ProcedureName);

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