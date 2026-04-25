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

public partial class ReferentialData : MonitoringTableJobPageBase<ReferentialData>
{
    private const string LoadErrorMessage = "Unable to load Referential Data right now. Please try again.";
    protected override string MonitoringSubmenuKey => "referential-data";
    protected override string MonitoringJobName => "Referential Data";
    protected override string DefaultLoadErrorMessage => LoadErrorMessage;

    [Inject]
    private IOptions<ReferentialDataOptions> ReferentialOptions { get; set; } = default!;

    private string ProcedureName => ReferentialOptions.Value.CheckReferentialDataStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(ReferentialOptions.Value.ConnectionStringName, ProcedureName);

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