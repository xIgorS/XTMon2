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

public partial class MissingWorkflowCheck : MonitoringTableJobPageBase<MissingWorkflowCheck>
{
    private const string LoadErrorMessage = "Unable to load Missing Workflow Check right now. Please try again.";
    protected override string MonitoringSubmenuKey => "missing-workflow-check";
    protected override string MonitoringJobName => "Missing Workflow Check";
    protected override string DefaultLoadErrorMessage => LoadErrorMessage;
    private static readonly IReadOnlyList<GridColumnDefinition> PreferredColumns =
    [
        new("Porfolio Id", ["PortfolioId", "Portfolio Id"]),
        new("Sk Porfolio", ["SkPortfolio", "Sk Portfolio"]),
        new("Book Id", ["BookId", "Book Id"]),
        new("Book Name", ["BookName", "Book Name"]),
        new("Is Daily Asset", ["DailyValidatedAsset", "Is Daily Asset"])
    ];

    [Inject]
    private IOptions<MissingWorkflowCheckOptions> MissingWorkflowCheckOptions { get; set; } = default!;

    private string ProcedureName => MissingWorkflowCheckOptions.Value.MissingWorkflowCheckStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(MissingWorkflowCheckOptions.Value.ConnectionStringName, ProcedureName);

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

        if (string.Equals(columnName, "DailyValidatedAsset", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(columnName, "Is Daily Asset", StringComparison.OrdinalIgnoreCase))
        {
            return value switch
            {
                "1" => "Yes",
                "0" => "No",
                _ => value
            };
        }

        return value;
    }

    private sealed record GridColumnDefinition(string Header, IReadOnlyList<string> Aliases);

    private sealed record GridColumn(string Name, string Header, int Index);
}