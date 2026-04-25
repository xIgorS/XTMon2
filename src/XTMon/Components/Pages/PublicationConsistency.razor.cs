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

public partial class PublicationConsistency : MonitoringTableJobPageBase<PublicationConsistency>
{
    private const string GridDateFormat = "dd-MM-yyyy";
    private const string LoadErrorMessage = "Unable to load Publication Consistency right now. Please try again.";
    protected override string MonitoringSubmenuKey => "publication-consistency";
    protected override string MonitoringJobName => "Publication Consistency";
    protected override string DefaultLoadErrorMessage => LoadErrorMessage;
    private static readonly IReadOnlyList<GridColumnDefinition> PreferredColumns =
    [
        new("Field", ["Field"]),
        new("Flow Id", ["FlowId", "flowid", "Flow Id"]),
        new("Message", ["Message"]),
        new("Arrival Date", ["ArrivalDate", "Arrivaldate", "Arrival Date"]),
        new("Source System Code", ["SourceSystemCode", "Source System Code"])
    ];

    [Inject]
    private IOptions<PublicationConsistencyOptions> PublicationConsistencyOptions { get; set; } = default!;

    private string ProcedureName => PublicationConsistencyOptions.Value.PublicationConsistencyStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(PublicationConsistencyOptions.Value.ConnectionStringName, ProcedureName);

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