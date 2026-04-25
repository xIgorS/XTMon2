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

public partial class PricingFileReception : MonitoringTableJobPageBase<PricingFileReception>
{
    private const string GridDateFormat = "dd-MM-yyyy";
    private const string PricingFileReceptionLoadErrorMessage = "Unable to load Pricing File Reception right now. Please try again.";
    protected override string MonitoringSubmenuKey => "pricing-file-reception";
    protected override string MonitoringJobName => "Pricing File Reception";
    protected override string DefaultLoadErrorMessage => PricingFileReceptionLoadErrorMessage;

    [Inject]
    private IPricingFileReceptionRepository Repository { get; set; } = default!;

    [Inject]
    private IOptions<PricingFileReceptionOptions> PricingFileReceptionOptions { get; set; } = default!;

    private bool traceAllVersions;

    private string ProcedureName => PricingFileReceptionOptions.Value.PricingFileReceptionStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(PricingFileReceptionOptions.Value.ConnectionStringName, ProcedureName);
    private string TraceAllVersionsText => traceAllVersions ? "Yes" : "No";
    private string SavedParameterSummaryText => string.IsNullOrWhiteSpace(savedParameterSummary) ? "Current option selection" : savedParameterSummary;

    protected override bool TryPrepareRun(out string? parametersJson, out string? parameterSummary)
    {
        parametersJson = MonitoringJobHelper.SerializeParameters(new DataValidationJobParameters(null, traceAllVersions));
        parameterSummary = $"Trace all versions: {TraceAllVersionsText}";
        return true;
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
