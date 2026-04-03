using System.Globalization;
using XTMon.Models;

namespace XTMon.Helpers;

internal static class BatchStatusHelper
{
    private const string DisplayDateFormat = "dd-MM-yyyy";
    private const string DisplayTimeFormat = "HH:mm:ss";

    public static IReadOnlyList<BatchStatusGridRow> BuildGridRows(MonitoringTableResult? table)
    {
        if (table is null)
        {
            return Array.Empty<BatchStatusGridRow>();
        }

        var columnMap = BuildColumnMap(table.Columns);

        EnsureRequiredColumn(columnMap, "pnldate");
        EnsureRequiredColumn(columnMap, "ConsoIsDone");
        EnsureRequiredColumn(columnMap, "CalculationIsDone");
        EnsureRequiredColumn(columnMap, "DatetimeEndCalculation");
        EnsureRequiredColumn(columnMap, "DatetimeEndExtraction");

        if (table.Rows.Count == 0)
        {
            return Array.Empty<BatchStatusGridRow>();
        }

        var rows = new List<BatchStatusGridRow>(table.Rows.Count);
        foreach (var row in table.Rows)
        {
            var consoIsDone = ReadIntegerValue(row, columnMap["consoisdone"]);
            var calculationIsDone = ReadIntegerValue(row, columnMap["calculationisdone"]);
            var pnlDate = ReadDateValue(row, columnMap["pnldate"]);
            var endCalculation = ReadDateTimeValue(row, columnMap["datetimeendcalculation"]);
            var endExtraction = ReadDateTimeValue(row, columnMap["datetimeendextraction"]);

            rows.Add(new BatchStatusGridRow(
                Status: consoIsDone == 1 && calculationIsDone == 1 ? "OK" : "KO",
                PnlDate: FormatDate(pnlDate),
                CalculationDate: FormatDate(endCalculation),
                CalculationEndTime: FormatTime(endCalculation),
                ExtractionEndTime: FormatTime(endExtraction)));
        }

        return rows;
    }

    private static Dictionary<string, int> BuildColumnMap(IReadOnlyList<string> columns)
    {
        var map = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < columns.Count; i++)
        {
            var name = columns[i]?.Trim();
            if (string.IsNullOrWhiteSpace(name))
            {
                continue;
            }

            map[name] = i;
        }

        return map;
    }

    private static void EnsureRequiredColumn(IReadOnlyDictionary<string, int> columnMap, string columnName)
    {
        if (!columnMap.ContainsKey(columnName))
        {
            throw new InvalidOperationException($"Batch Status result is missing required column '{columnName}'.");
        }
    }

    private static int? ReadIntegerValue(IReadOnlyList<string?> row, int index)
    {
        var rawValue = ReadCell(row, index);
        if (string.IsNullOrWhiteSpace(rawValue))
        {
            return null;
        }

        if (int.TryParse(rawValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedInteger))
        {
            return parsedInteger;
        }

        if (decimal.TryParse(rawValue, NumberStyles.Number, CultureInfo.InvariantCulture, out var parsedDecimal))
        {
            return (int)parsedDecimal;
        }

        if (bool.TryParse(rawValue, out var parsedBoolean))
        {
            return parsedBoolean ? 1 : 0;
        }

        return null;
    }

    private static DateOnly? ReadDateValue(IReadOnlyList<string?> row, int index)
    {
        var rawValue = ReadCell(row, index);
        if (string.IsNullOrWhiteSpace(rawValue))
        {
            return null;
        }

        if (DateOnly.TryParse(rawValue, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateOnly))
        {
            return dateOnly;
        }

        if (DateTime.TryParse(rawValue, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateTime))
        {
            return DateOnly.FromDateTime(dateTime);
        }

        return null;
    }

    private static DateTime? ReadDateTimeValue(IReadOnlyList<string?> row, int index)
    {
        var rawValue = ReadCell(row, index);
        if (string.IsNullOrWhiteSpace(rawValue))
        {
            return null;
        }

        if (DateTime.TryParse(rawValue, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateTime))
        {
            return dateTime;
        }

        return null;
    }

    private static string? ReadCell(IReadOnlyList<string?> row, int index)
    {
        return index >= 0 && index < row.Count ? row[index] : null;
    }

    private static string FormatDate(DateOnly? value)
    {
        return value.HasValue
            ? value.Value.ToString(DisplayDateFormat, CultureInfo.InvariantCulture)
            : "-";
    }

    private static string FormatDate(DateTime? value)
    {
        return value.HasValue
            ? DateOnly.FromDateTime(value.Value).ToString(DisplayDateFormat, CultureInfo.InvariantCulture)
            : "-";
    }

    private static string FormatTime(DateTime? value)
    {
        return value.HasValue
            ? TimeOnly.FromDateTime(value.Value).ToString(DisplayTimeFormat, CultureInfo.InvariantCulture)
            : "-";
    }
}