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

public partial class DailyBalance : SourceSystemMonitoringTableJobPageBase<DailyBalance>
{
    private const string GridDateFormat = "dd-MM-yyyy";
    private const string SourceSystemsLoadErrorText = "Unable to load Daily Balance source systems right now. Please try again.";
    private const string DailyBalanceLoadErrorMessage = "Unable to load Daily Balance right now. Please try again.";
    protected override string MonitoringSubmenuKey => "daily-balance";
    protected override string MonitoringJobName => "Daily Balance";
    protected override string DefaultLoadErrorMessage => DailyBalanceLoadErrorMessage;
    protected override string SourceSystemsLoadErrorMessage => SourceSystemsLoadErrorText;

    [Inject]
    private IDailyBalanceRepository Repository { get; set; } = default!;

    [Inject]
    private IOptions<DailyBalanceOptions> DailyBalanceOptions { get; set; } = default!;

    private string ProcedureName => DailyBalanceOptions.Value.DailyBalanceStoredProcedure;
    protected override string SourceSystemsProcedureName => DailyBalanceOptions.Value.GetAllSourceSystemsStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(DailyBalanceOptions.Value.ConnectionStringName, ProcedureName);
    private string FullyQualifiedSourceSystemsProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(DailyBalanceOptions.Value.ConnectionStringName, SourceSystemsProcedureName);

    protected override async Task<IReadOnlyList<string>> LoadAvailableSourceSystemCodesAsync(CancellationToken cancellationToken)
    {
        var availableSourceSystems = await Repository.GetSourceSystemsAsync(cancellationToken);
        return availableSourceSystems.Select(static sourceSystem => sourceSystem.Code).ToArray();
    }

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

        var normalizedColumnName = MonitoringDisplayHelper.NormalizeColumnName(columnName);

        if ((normalizedColumnName is "pnldate" or "lastupdate" or "lastupdated" or "lastupdateddate") &&
            DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsedDate))
        {
            return parsedDate.ToString(GridDateFormat, CultureInfo.InvariantCulture);
        }

        return value;
    }

    private sealed record GridColumn(string Name, int Index);
}
