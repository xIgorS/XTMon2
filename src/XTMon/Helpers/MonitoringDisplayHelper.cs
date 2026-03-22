using System.Globalization;
using XTMon.Models;

namespace XTMon.Helpers;

internal static class MonitoringDisplayHelper
{
    private const string DisplayDateTimeFormat = "dd-MM-yyyy HH:mm:ss";

    public static string NormalizeColumnName(string? columnName)
    {
        if (string.IsNullOrWhiteSpace(columnName))
        {
            return string.Empty;
        }

        return columnName
            .Replace("_", string.Empty, StringComparison.Ordinal)
            .Replace(" ", string.Empty, StringComparison.Ordinal)
            .Trim()
            .ToLowerInvariant();
    }

    public static string FormatDateTimeForDisplay(string value)
    {
        if (DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsed))
        {
            return parsed.ToString(DisplayDateTimeFormat, CultureInfo.InvariantCulture);
        }

        return value;
    }

    public static bool IsDateLikeColumn(string columnName)
    {
        var normalized = NormalizeColumnName(columnName);
        return normalized.Contains("date", StringComparison.Ordinal) ||
               normalized.Contains("time", StringComparison.Ordinal) ||
               normalized.Contains("updated", StringComparison.Ordinal);
    }

    public static string FormatWithSpaces(long value)
    {
        return value.ToString("N0", CultureInfo.InvariantCulture).Replace(",", " ");
    }

    public static string FormatWithSpaces(decimal value)
    {
        var formatted = value.ToString("N2", CultureInfo.InvariantCulture).Replace(",", " ");
        if (formatted.EndsWith(".00", StringComparison.Ordinal))
        {
            return formatted[..^3];
        }

        if (formatted.EndsWith("0", StringComparison.Ordinal))
        {
            return formatted[..^1];
        }

        return formatted;
    }

    public static int FindLastUpdatedColumnIndex(IReadOnlyList<string> columns)
    {
        for (var i = 0; i < columns.Count; i++)
        {
            var normalized = NormalizeColumnName(columns[i]);
            if (normalized is "lastupdated" or "lastupdateddate")
            {
                return i;
            }
        }

        return -1;
    }

    public static string? GetLastUpdatedDisplayValue(MonitoringTableResult? result)
    {
        if (result is null || result.Rows.Count == 0)
        {
            return null;
        }

        var lastUpdatedIndex = FindLastUpdatedColumnIndex(result.Columns);
        if (lastUpdatedIndex < 0)
        {
            return null;
        }

        foreach (var row in result.Rows)
        {
            if (row.Count <= lastUpdatedIndex)
            {
                continue;
            }

            var value = row[lastUpdatedIndex];
            if (!string.IsNullOrWhiteSpace(value))
            {
                return FormatDateTimeForDisplay(value);
            }
        }

        return null;
    }
}
