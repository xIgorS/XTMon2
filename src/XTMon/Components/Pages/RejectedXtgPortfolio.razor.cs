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

public partial class RejectedXtgPortfolio : MonitoringTableJobPageBase<RejectedXtgPortfolio>
{
    private const string GridDateFormat = "dd-MM-yyyy";
    private const string LoadErrorMessage = "Unable to load Rejected XTG Portfolio right now. Please try again.";
    protected override string MonitoringSubmenuKey => "rejected-xtg-portfolio";
    protected override string MonitoringJobName => "Rejected XTG Portfolio";
    protected override string DefaultLoadErrorMessage => LoadErrorMessage;
    private static readonly IReadOnlyList<(string Name, string Header)> PreferredColumns =
    [
        ("PnlGroup", "Pnl Group"),
        ("SignOff", "Sign Off"),
        ("Portfolio_Name", "Porfolio Name"),
        ("Reject_Description", "Rejection Description")
    ];

    [Inject]
    private IOptions<RejectedXtgPortfolioOptions> RejectedXtgPortfolioOptions { get; set; } = default!;

    private string ProcedureName => RejectedXtgPortfolioOptions.Value.RejectedXtgPortfolioStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(RejectedXtgPortfolioOptions.Value.ConnectionStringName, ProcedureName);

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

        var columns = new List<GridColumn>(result.Columns.Count);
        foreach (var (name, header) in PreferredColumns)
        {
            if (sourceIndexes.TryGetValue(name, out var index))
            {
                columns.Add(new GridColumn(name, header, index));
            }
        }

        for (var i = 0; i < result.Columns.Count; i++)
        {
            var name = result.Columns[i];
            if (PreferredColumns.Any(column => string.Equals(column.Name, name, StringComparison.OrdinalIgnoreCase)))
            {
                continue;
            }

            columns.Add(new GridColumn(name, JvCalculationHelper.ToHeaderLabel(name), i));
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