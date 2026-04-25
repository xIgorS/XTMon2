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

public partial class ReverseConsoFile : SourceSystemMonitoringTableJobPageBase<ReverseConsoFile>
{
    private const string GridDateFormat = "dd-MM-yyyy";
    private const string GridDateTimeFormat = "dd-MM-yyyy HH:mm:ss";
    private const string SourceSystemsLoadErrorText = "Unable to load Reverse Conso File source systems right now. Please try again.";
    private const string ReverseConsoFileLoadErrorMessage = "Unable to load Reverse Conso File right now. Please try again.";
    protected override string MonitoringSubmenuKey => "reverse-conso-file";
    protected override string MonitoringJobName => "Reverse Conso File";
    protected override string DefaultLoadErrorMessage => ReverseConsoFileLoadErrorMessage;
    protected override string SourceSystemsLoadErrorMessage => SourceSystemsLoadErrorText;
    private static readonly IReadOnlyList<GridColumnDefinition> PreferredColumns =
    [
        new("Status", ["Status"]),
        new("Feed Source Name", ["FeedSourceName", "Feed Source Name"]),
        new("Business Data Type Name", ["BusinessDataTypeName", "Business Data Type Name"]),
        new("Current Step", ["CurrentStep", "Current Step"]),
        new("Flow Id", ["FlowId", "flowId", "Flow Id"]),
        new("Flow Id Derived From", ["FlowIdDerivedFrom", "Flow Id Derived From"]),
        new("Pnl Date", ["PnlDate", "pnlDate", "Pnl Date"]),
        new("Arrival Date Time", ["ArrivalDateTime", "ArrivalDate", "Arrival Date Time"]),
        new("Is Failed", ["IsFailed", "Is Failed"])
    ];

    [Inject]
    private IReverseConsoFileRepository Repository { get; set; } = default!;

    [Inject]
    private IOptions<ReverseConsoFileOptions> ReverseConsoFileOptions { get; set; } = default!;

    private string ProcedureName => ReverseConsoFileOptions.Value.ReverseConsoFileStoredProcedure;
    protected override string SourceSystemsProcedureName => ReverseConsoFileOptions.Value.GetAllSourceSystemsStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(ReverseConsoFileOptions.Value.ConnectionStringName, ProcedureName);
    private string FullyQualifiedSourceSystemsProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(ReverseConsoFileOptions.Value.ConnectionStringName, SourceSystemsProcedureName);

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

    private static string? GetCellValue(GridColumn column, IReadOnlyList<string?> row)
    {
        return column.Index >= 0 && column.Index < row.Count ? row[column.Index] : null;
    }

    private static string GetColumnAlignmentClass(string columnName) => JvCalculationHelper.GetColumnAlignmentClass(columnName);

    private static string FormatCellValue(string columnName, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "-";
        }

        var normalizedColumnName = MonitoringDisplayHelper.NormalizeColumnName(columnName);

        if (normalizedColumnName == "pnldate")
        {
            if (DateOnly.TryParseExact(value, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsedDateOnly))
            {
                return parsedDateOnly.ToString(GridDateFormat, CultureInfo.InvariantCulture);
            }

            if (DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsedDate))
            {
                return parsedDate.ToString(GridDateFormat, CultureInfo.InvariantCulture);
            }
        }

        if (normalizedColumnName is "arrivaldatetime" or "arrivaldate" &&
            DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsedDateTime))
        {
            return parsedDateTime.ToString(GridDateTimeFormat, CultureInfo.InvariantCulture);
        }

        return value;
    }

    private sealed record GridColumnDefinition(string Header, IReadOnlyList<string> Aliases);

    private sealed record GridColumn(string Name, string Header, int Index);
}