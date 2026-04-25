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

public partial class PrecalcMonitoring : MonitoringTableJobPageBase<PrecalcMonitoring>
{
    private const string GridDateFormat = "dd-MM-yyyy";
    private const string GridDateTimeFormat = "dd-MM-yyyy HH:mm:ss";
    private const string LoadErrorMessage = "Unable to load Precalc Monitoring right now. Please try again.";
    protected override string MonitoringSubmenuKey => "precalc-monitoring";
    protected override string MonitoringJobName => "Precalc Monitoring";
    protected override string DefaultLoadErrorMessage => LoadErrorMessage;
    private static readonly IReadOnlyList<GridColumnDefinition> PreferredColumns =
    [
        new("Pnl Date", ["PnlDate", "Pnl Date"]),
        new("Parent Correlation Id", ["ParentCorrelationId", "Parent Correlation Id"]),
        new("Type of Calculation", ["TypeOfCalculation", "Type of Calculation"]),
        new("Porfolio Id", ["PortfolioId", "Portfolio Id"]),
        new("FeedSource Id", ["FeedSourceId", "FeedSource Id"]),
        new("Business Data Type", ["BusinessDataType", "Business Data Type"]),
        new("Flow Id", ["FlowId", "Flow Id"]),
        new("Kind of Process Description", ["KindOfProcessDescription", "Kind of Process Description"]),
        new("Status Process", ["StatusProcess", "Status Process"])
    ];

    [Inject]
    private IOptions<PrecalcMonitoringOptions> PrecalcMonitoringOptions { get; set; } = default!;

    private string ProcedureName => PrecalcMonitoringOptions.Value.PrecalcMonitoringStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(PrecalcMonitoringOptions.Value.ConnectionStringName, ProcedureName);

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

        var normalizedColumnName = MonitoringDisplayHelper.NormalizeColumnName(columnName);

        if (normalizedColumnName is "pnldate" &&
            DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsedDate))
        {
            return parsedDate.ToString(GridDateFormat, CultureInfo.InvariantCulture);
        }

        if (normalizedColumnName is "datetimestart" or "datetimeend" &&
            DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsedDateTime))
        {
            return parsedDateTime.ToString(GridDateTimeFormat, CultureInfo.InvariantCulture);
        }

        return value;
    }

    private sealed record GridColumnDefinition(string Header, IReadOnlyList<string> Aliases);

    private sealed record GridColumn(string Name, string Header, int Index);
}