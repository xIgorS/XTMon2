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

public partial class ResultTransfer : MonitoringTableJobPageBase<ResultTransfer>
{
    private const string GridDateFormat = "dd-MM-yyyy";
    private const string LoadErrorMessage = "Unable to load Result Transfer right now. Please try again.";
    protected override string MonitoringSubmenuKey => "result-transfer";
    protected override string MonitoringJobName => "Result Transfer";
    protected override string DefaultLoadErrorMessage => LoadErrorMessage;

    [Inject]
    private IOptions<ResultTransferOptions> ResultTransferOptions { get; set; } = default!;

    private string ProcedureName => ResultTransferOptions.Value.ResultTransferStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(ResultTransferOptions.Value.ConnectionStringName, ProcedureName);

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

    private sealed record GridColumn(string Name, int Index);
}