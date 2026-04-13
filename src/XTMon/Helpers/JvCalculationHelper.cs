using System.Text;
using System.Text.Json;
using XTMon.Models;

namespace XTMon.Helpers;

internal static class JvCalculationHelper
{
    /// <summary>
    /// Returns true when the elapsed time since <paramref name="lastActivityUtc"/> exceeds <paramref name="timeout"/>.
    /// The caller is responsible for resolving the relevant timestamp and converting it to UTC.
    /// </summary>
    public static bool IsStaleRunningJob(DateTime lastActivityUtc, TimeSpan timeout)
    {
        var elapsed = DateTime.UtcNow - lastActivityUtc;
        return elapsed >= timeout;
    }

    /// <summary>
    /// Normalises a <see cref="DateTime"/> to UTC regardless of its <see cref="DateTimeKind"/>.
    /// Unspecified kind is treated as UTC (SQL Server convention in this project).
    /// </summary>
    public static DateTime ToUtc(DateTime value)
    {
        if (value.Kind == DateTimeKind.Utc)
        {
            return value;
        }

        if (value.Kind == DateTimeKind.Local)
        {
            return value.ToUniversalTime();
        }

        return DateTime.SpecifyKind(value, DateTimeKind.Utc);
    }

    /// <summary>
    /// Converts a camelCase column name into a space-separated label, e.g. "GridRowsJson" → "Grid Rows Json".
    /// </summary>
    public static string ToHeaderLabel(string? columnName)
    {
        if (string.IsNullOrWhiteSpace(columnName))
        {
            return string.Empty;
        }

        var builder = new StringBuilder(columnName.Length + 4);
        for (var i = 0; i < columnName.Length; i++)
        {
            var current = columnName[i];
            if (i > 0)
            {
                var previous = columnName[i - 1];
                var next = i + 1 < columnName.Length ? columnName[i + 1] : '\0';
                var boundary = char.IsUpper(current) &&
                               (char.IsLower(previous) || (char.IsUpper(previous) && char.IsLower(next)));

                if (boundary)
                {
                    builder.Append(' ');
                }
            }

            builder.Append(current);
        }

        return builder.ToString();
    }

    /// <summary>
    /// Formats a configured connection-string name and stored procedure name into a fully-qualified display label.
    /// Example: "StagingFiAlmt" + "monitoring.UspFoo" → "STAGING_FI_ALMT.monitoring.UspFoo".
    /// </summary>
    public static string BuildFullyQualifiedProcedureName(string connectionStringName, string procedureName)
    {
        if (string.IsNullOrWhiteSpace(procedureName))
        {
            return string.Empty;
        }

        var databaseName = ToHeaderLabel(connectionStringName)
            .Replace(' ', '_')
            .ToUpperInvariant();

        return string.IsNullOrWhiteSpace(databaseName)
            ? procedureName
            : $"{databaseName}.{procedureName}";
    }

    /// <summary>
    /// Returns the CSS alignment class for a column based on keyword matching in the column name.
    /// </summary>
    public static string GetColumnAlignmentClass(string columnName)
    {
        if (columnName.Contains("Date", StringComparison.OrdinalIgnoreCase) ||
            columnName.Contains("Time", StringComparison.OrdinalIgnoreCase) ||
            columnName.Contains("Status", StringComparison.OrdinalIgnoreCase))
        {
            return "db-grid__cell--text";
        }

        if (columnName.Contains("Id", StringComparison.OrdinalIgnoreCase) ||
            columnName.Contains("Amount", StringComparison.OrdinalIgnoreCase) ||
            columnName.Contains("Code", StringComparison.OrdinalIgnoreCase))
        {
            return "db-grid__cell--num";
        }

        return "db-grid__cell--text";
    }

    /// <summary>
    /// Deserializes JSON column/row data into a <see cref="MonitoringTableResult"/>.
    /// Returns null when either argument is blank or the JSON is malformed.
    /// </summary>
    public static MonitoringTableResult? DeserializeMonitoringTable(string? columnsJson, string? rowsJson)
    {
        if (string.IsNullOrWhiteSpace(columnsJson) || string.IsNullOrWhiteSpace(rowsJson))
        {
            return null;
        }

        try
        {
            var columns = JsonSerializer.Deserialize<List<string>>(columnsJson) ?? new List<string>();
            var rows = JsonSerializer.Deserialize<List<List<string?>>>(rowsJson) ?? new List<List<string?>>();
            return new MonitoringTableResult(columns, rows.Cast<IReadOnlyList<string?>>().ToList());
        }
        catch
        {
            return null;
        }
    }
}
