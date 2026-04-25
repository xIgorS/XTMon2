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

public partial class MultipleFeedVersion : MonitoringTableJobPageBase<MultipleFeedVersion>
{
    private const string GridDateFormat = "dd-MM-yyyy";
    private const string LoadErrorMessage = "Unable to load Multiple Feed Version right now. Please try again.";
    protected override string MonitoringSubmenuKey => "multiple-feed-version";
    protected override string MonitoringJobName => "Multiple Feed Version";
    protected override string DefaultLoadErrorMessage => LoadErrorMessage;
    private static readonly IReadOnlyList<(string Name, string Header)> PreferredColumns =
    [
        ("Portfolioid", "Porfolio Id"),
        ("BusinessDataTypeId", "Business Data Type"),
        ("FeedSourceName", "Feed Source Name"),
        ("PnlDate", "Pnl Date"),
        ("PortfolioFlowIdPriorBalance", "Porfolio Flow Id Prior Balance"),
        ("FeedVersion", "Feed Version"),
        ("RecordCount", "Record Count")
    ];

    [Inject]
    private IOptions<MultipleFeedVersionOptions> MultipleFeedVersionOptions { get; set; } = default!;

    private string ProcedureName => MultipleFeedVersionOptions.Value.MultipleFeedVersionStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(MultipleFeedVersionOptions.Value.ConnectionStringName, ProcedureName);

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
        foreach (var (name, header) in PreferredColumns)
        {
            if (sourceIndexes.TryGetValue(name, out var index))
            {
                columns.Add(new GridColumn(name, header, index));
            }
        }

        return columns;
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

    private sealed record GridColumn(string Name, string Header, int Index);
}