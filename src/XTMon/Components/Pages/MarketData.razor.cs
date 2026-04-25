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

public partial class MarketData : MonitoringTableJobPageBase<MarketData>
{
    private const string LoadErrorMessage = "Unable to load Market Data right now. Please try again.";
    protected override string MonitoringSubmenuKey => "market-data";
    protected override string MonitoringJobName => "Market Data";
    protected override string DefaultLoadErrorMessage => LoadErrorMessage;

    [Inject]
    private IOptions<MarketDataOptions> MarketDataOptions { get; set; } = default!;

    private string ProcedureName => MarketDataOptions.Value.MarketDataStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(MarketDataOptions.Value.ConnectionStringName, ProcedureName);

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