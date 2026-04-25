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

public partial class TradingVsFivrCheck : MonitoringTableJobPageBase<TradingVsFivrCheck>
{
    private const string GridDateFormat = "dd-MM-yyyy";
    private const string LoadErrorMessage = "Unable to load Trading vs Fivr Check right now. Please try again.";
    protected override string MonitoringSubmenuKey => "trading-vs-fivr-check";
    protected override string MonitoringJobName => "Trading vs Fivr Check";
    protected override string DefaultLoadErrorMessage => LoadErrorMessage;
    private static readonly IReadOnlyList<GridColumnDefinition> PreferredColumns =
    [
        new("Pnl Group", ["PnlGroup", "Pnl Group"]),
        new("Sign Off", ["SignOff", "Sign Off"]),
        new("Porfolio Id", ["Portfolioid", "PortfolioId", "Portfolio Id"]),
        new("FIVR Porfolio Id", ["FIVR_Portfolioid", "FIVRPortfolioId", "FivrPortfolioId", "FIVR Portfolio Id", "Fivr Portfolio Id"]),
        new("Book Pnl Reporting System", ["bookPnLReportingSystem", "BookPnlReportingSystem", "BookPnl_ReportingSystem", "Book Pnl Reporting System", "Book PnL Reporting System"]),
        new("Legal Entity Code", ["LegalEntityCode", "Legal Entity Code"]),
        new("FIVR Legal Entity Code", ["FIVR_LegalEntityCode", "FIVRLegalEntityCode", "FivrLegalEntityCode", "FIVR Legal Entity Code", "Fivr Legal Entity Code"]),
        new("Legal Entity Location", ["LegalEntityLocation", "Legal Entity Location"]),
        new("FIVR Legal Entity Location", ["FIVR_LegalEntityLocation", "FIVRLegalEntityLocation", "FivrLegalEntityLocation", "FIVR Legal Entity Location", "Fivr Legal Entity Location"]),
        new("Freezing Currency", ["FreezingCurrency", "Freezing Currency"]),
        new("FIVR Freezing Currency", ["FIVR_Freezingcurrency", "FIVRFreezingCurrency", "FivrFreezingCurrency", "FIVR Freezing Currency", "Fivr Freezing Currency"]),
        new("Trading Vs Fivr Check", ["TradingVsFivr_Check", "TradingVsFivrCheck", "TradingVsFIVRCheck", "Trading Vs Fivr Check"])
    ];

    [Inject]
    private ITradingVsFivrCheckRepository Repository { get; set; } = default!;

    [Inject]
    private IOptions<TradingVsFivrCheckOptions> TradingVsFivrCheckOptions { get; set; } = default!;

    private string ProcedureName => TradingVsFivrCheckOptions.Value.TradingVsFivrCheckStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(TradingVsFivrCheckOptions.Value.ConnectionStringName, ProcedureName);
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