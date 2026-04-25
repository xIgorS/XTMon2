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

public partial class FeedOutExtraction : MonitoringTableJobPageBase<FeedOutExtraction>
{
    private const string GridDateFormat = "dd-MM-yyyy";
    private const string LoadErrorMessage = "Unable to load FeedOut Extraction right now. Please try again.";
    protected override string MonitoringSubmenuKey => "feedout-extraction";
    protected override string MonitoringJobName => "FeedOut Extraction";
    protected override string DefaultLoadErrorMessage => LoadErrorMessage;
    private static readonly IReadOnlyList<(string Name, string Header)> PreferredColumns =
    [
        ("Status", "Status"),
        ("ExtractStatus", "Extract Status"),
        ("ExtractName", "Extract Name"),
        ("SourceSystemName", "Source System Name"),
        ("BookName", "Book Name"),
        ("ScheduleTime", "Schedule Time"),
        ("ExtractionDate", "Extraction Date"),
        ("StartDate", "Start Date"),
        ("EndDate", "End Date"),
        ("FirstDateExtract", "First Date Extract"),
        ("CorrelationId", "Correlation Id"),
        ("NbRows", "Nb Rows"),
        ("ExtractionFileName", "Extraction File Name"),
        ("FileSize", "File Size"),
        ("PricingDailyStatus", "Pricing Daily Status"),
        ("AdjustmentsStatus", "Adjustments Status"),
        ("BcpStatus", "Bcp Status"),
        ("ExtractionType", "Extraction Type"),
        ("JobId", "Job Id"),
        ("IsLast", "Is Last"),
        ("PricingDailyCheckResult", "Pricing Daily Check"),
        ("AdjustmentCheckResult", "Adjustment Check")
    ];

    [Inject]
    private IOptions<FeedOutExtractionOptions> FeedOutExtractionOptions { get; set; } = default!;

    private string ProcedureName => FeedOutExtractionOptions.Value.FeedOutExtractionStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(FeedOutExtractionOptions.Value.ConnectionStringName, ProcedureName);

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

        if (MonitoringDisplayHelper.IsDateLikeColumn(columnName) &&
            DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsedDate))
        {
            return parsedDate.ToString(GridDateFormat, CultureInfo.InvariantCulture);
        }

        return value;
    }

    private sealed record GridColumn(string Name, string Header, int Index);
}