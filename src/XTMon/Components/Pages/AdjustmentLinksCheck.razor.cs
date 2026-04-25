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

public partial class AdjustmentLinksCheck : MonitoringTableJobPageBase<AdjustmentLinksCheck>
{
    private const string GridDateFormat = "dd-MM-yyyy";
    private const string LoadErrorMessage = "Unable to load Adjustment Links Check right now. Please try again.";
    protected override string MonitoringSubmenuKey => "adjustment-links-check";
    protected override string MonitoringJobName => "Adjustment Links Check";
    protected override string DefaultLoadErrorMessage => LoadErrorMessage;
    private static readonly IReadOnlyList<GridColumnDefinition> PreferredColumns =
    [
        new("Flow Id", ["FlowId", "Flow Id"]),
        new("Porfolio Flow Id Prior Balance", ["PortfolioFlowIdPriorBalance", "Portfolio Flow Id Prior Balance"]),
        new("Flow Id Derived From", ["FlowIdDerivedfrom", "FlowIdDerivedFrom", "Flow Id Derived From"]),
        new("Porfolio Id", ["Portfolioid", "PortfolioId", "Portfolio Id"]),
        new("Feed Source Name", ["FeedSourceName", "Feed Source Name"]),
        new("Business Data Type", ["BusinessDataTypeId", "BusinessDataType", "Business Data Type"]),
        new("Pnl Date", ["PnlDate", "Pnl Date"])
    ];

    [Inject]
    private IAdjustmentLinksCheckRepository Repository { get; set; } = default!;

    [Inject]
    private IOptions<AdjustmentLinksCheckOptions> AdjustmentLinksCheckOptions { get; set; } = default!;

    private string ProcedureName => AdjustmentLinksCheckOptions.Value.AdjustmentLinksCheckStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(AdjustmentLinksCheckOptions.Value.ConnectionStringName, ProcedureName);
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