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

public partial class FactPvCaConsistency : MonitoringTableJobPageBase<FactPvCaConsistency>
{
    private const string LoadErrorMessage = "Unable to load Fact PV/CA Consistency right now. Please try again.";
    protected override string MonitoringSubmenuKey => "fact-pv-ca-consistency";
    protected override string MonitoringJobName => "Fact PV/CA Consistency";
    protected override string DefaultLoadErrorMessage => LoadErrorMessage;
    private static readonly IReadOnlyList<(string Name, string Header)> PreferredColumns =
    [
        ("FlowId", "Flow Id"),
        ("FileName", "File Name"),
        ("DiffAmount", "Diff Amount"),
        ("Indicator", "Indicator")
    ];

    [Inject]
    private IFactPvCaConsistencyRepository Repository { get; set; } = default!;

    [Inject]
    private IOptions<FactPvCaConsistencyOptions> FactPvCaConsistencyOptions { get; set; } = default!;

    private string ProcedureName => FactPvCaConsistencyOptions.Value.FactPvCaConsistencyStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(FactPvCaConsistencyOptions.Value.ConnectionStringName, ProcedureName);
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

        if (MonitoringDisplayHelper.NormalizeColumnName(columnName) == "diffamount" &&
            decimal.TryParse(value, NumberStyles.Number, CultureInfo.InvariantCulture, out var parsedAmount))
        {
            return MonitoringDisplayHelper.FormatCurrencyWithSpaces(parsedAmount);
        }

        return value;
    }

    private sealed record GridColumn(string Name, string Header, int Index);
}