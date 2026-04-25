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

public partial class JvBalanceConsistency : MonitoringTableJobPageBase<JvBalanceConsistency>
{
    private const string GridDateFormat = "dd-MM-yyyy";
    private const string LoadErrorMessage = "Unable to load JV Balance Consistency right now. Please try again.";
    protected override string MonitoringSubmenuKey => "jv-balance-consistency";
    protected override string MonitoringJobName => "JV Balance Consistency";
    protected override string DefaultLoadErrorMessage => LoadErrorMessage;
    private static readonly IReadOnlyList<GridColumnDefinition> PreferredColumns =
    [
        new("Pnl Date", ["PnlDate", "Pnl Date"]),
        new("Sk Porfolio", ["SkPortfolio", "portfolioid", "PortfolioId"]),
        new("Mtd Amount HO", ["MtdAmountHO", "Mtd Amount HO"]),
        new("Ytd Amount HO", ["YtdAmountHO", "Ytd Amount HO"]),
        new("Mtd Amount Paradigm", ["MtdAmountParadigm", "Mtd Amount Paradigm"]),
        new("Qtd Amount Paradigm", ["QtdAmountParadigm", "Qtd Amount Paradigm"]),
        new("Ydt Amount Paradigm", ["YtdAmountParadigm", "Ydt Amount Paradigm", "Ytd Amount Paradigm"]),
        new("Jv Check Balance", ["JvCheckBalance", "JvCheck", "Jv Check Balance"])
    ];

    [Inject]
    private IOptions<JvBalanceConsistencyOptions> JvBalanceConsistencyOptions { get; set; } = default!;

    private string precisionText = string.Empty;

    private string ProcedureName => JvBalanceConsistencyOptions.Value.JvBalanceConsistencyStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(JvBalanceConsistencyOptions.Value.ConnectionStringName, ProcedureName);
    private string SavedParameterSummaryText => string.IsNullOrWhiteSpace(savedParameterSummary) ? $"Precision: {precisionText}" : savedParameterSummary;
    private string JobStatusText => string.IsNullOrWhiteSpace(activeJobStatus) ? "-" : activeJobStatus;

    protected override Task OnInitializedCoreAsync()
    {
        precisionText = JvBalanceConsistencyOptions.Value.Precision.ToString("0.00", CultureInfo.InvariantCulture);
        return Task.CompletedTask;
    }

    private void OnPrecisionInput(ChangeEventArgs args)
    {
        precisionText = Convert.ToString(args.Value, CultureInfo.InvariantCulture) ?? string.Empty;
        validationError = null;
        runError = null;
    }

    protected override bool TryPrepareRun(out string? parametersJson, out string? parameterSummary)
    {
        if (!TryParsePrecision(out var precision, out var precisionError))
        {
            validationError = precisionError;
            parametersJson = null;
            parameterSummary = null;
            return false;
        }

        validationError = null;
        parametersJson = MonitoringJobHelper.SerializeParameters(new DataValidationJobParameters(
            SourceSystemCodes: null,
            TraceAllVersions: null,
            Precision: precision));
        parameterSummary = $"Precision: {FormatPrecision(precision)}";
        return true;
    }

    protected override void OnAfterApplyTableJob(MonitoringJobRecord job)
    {
        var savedParameters = MonitoringJobHelper.DeserializeParameters<DataValidationJobParameters>(job.ParametersJson);
        if (savedParameters?.Precision is decimal precision)
        {
            precisionText = FormatPrecision(precision);
        }
    }

    private bool TryParsePrecision(out decimal precision, out string error)
    {
        var normalized = precisionText.Trim();
        if (string.IsNullOrWhiteSpace(normalized))
        {
            precision = default;
            error = "Precision is required.";
            return false;
        }

        if (!decimal.TryParse(normalized, NumberStyles.Number, CultureInfo.InvariantCulture, out precision) &&
            !decimal.TryParse(normalized, NumberStyles.Number, CultureInfo.CurrentCulture, out precision))
        {
            error = "Precision must be a valid decimal number.";
            return false;
        }

        if (precision is < 0m or > 99.99m)
        {
            error = "Precision must be between 0 and 99.99.";
            return false;
        }

        error = string.Empty;
        return true;
    }

    private static string FormatPrecision(decimal precision)
    {
        return Math.Round(precision, 2, MidpointRounding.AwayFromZero).ToString("0.00", CultureInfo.InvariantCulture);
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