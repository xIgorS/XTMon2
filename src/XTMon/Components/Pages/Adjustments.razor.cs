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

public partial class Adjustments : SourceSystemMonitoringTableJobPageBase<Adjustments>
{
    private const string GridDateFormat = "dd-MM-yyyy";
    private const string SourceSystemsLoadErrorText = "Unable to load Adjustments source systems right now. Please try again.";
    private const string AdjustmentsLoadErrorMessage = "Unable to load Adjustments right now. Please try again.";
    protected override string MonitoringSubmenuKey => "adjustments";
    protected override string MonitoringJobName => "Adjustments";
    protected override string DefaultLoadErrorMessage => AdjustmentsLoadErrorMessage;
    protected override string SourceSystemsLoadErrorMessage => SourceSystemsLoadErrorText;

    [Inject]
    private IAdjustmentsRepository Repository { get; set; } = default!;

    [Inject]
    private IOptions<AdjustmentsOptions> AdjustmentsOptions { get; set; } = default!;

    private string ProcedureName => AdjustmentsOptions.Value.AdjustmentsStoredProcedure;
    protected override string SourceSystemsProcedureName => AdjustmentsOptions.Value.GetAllSourceSystemsStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(AdjustmentsOptions.Value.ConnectionStringName, ProcedureName);
    private string FullyQualifiedSourceSystemsProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(AdjustmentsOptions.Value.ConnectionStringName, SourceSystemsProcedureName);

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
