using Microsoft.Data.SqlClient;
using System.Data;
using System.Globalization;

namespace XTMon.Helpers;

internal static class SqlDataHelper
{
    public static bool TryReadDateOnly(IDataRecord reader, int ordinal, out DateOnly date)
    {
        date = default;
        if (reader.IsDBNull(ordinal))
        {
            return false;
        }

        var value = reader.GetValue(ordinal);
        switch (value)
        {
            case DateOnly dateOnly:
                date = dateOnly;
                return true;
            case DateTime dateTime:
                date = DateOnly.FromDateTime(dateTime);
                return true;
            case string rawDate when DateOnly.TryParse(rawDate, CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsedDate):
                date = parsedDate;
                return true;
            default:
                return false;
        }
    }

    public static string ParseQuery(object? rawQuery)
    {
        if (rawQuery is null || rawQuery == DBNull.Value)
        {
            return string.Empty;
        }

        var query = Convert.ToString(rawQuery) ?? string.Empty;
        if (string.IsNullOrWhiteSpace(query))
        {
            return string.Empty;
        }

        var normalized = query
            .Replace("\r\n", "\n", StringComparison.Ordinal)
            .Replace("\r", "\n", StringComparison.Ordinal)
            .Trim();

        if (!normalized.Contains('\n', StringComparison.Ordinal))
        {
            normalized = normalized.Replace("; ", ";\n", StringComparison.Ordinal);
        }

        return normalized;
    }

    public static bool ReadBoolean(IDataRecord reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
        {
            return false;
        }

        var value = reader.GetValue(ordinal);
        if (value is bool boolValue)
        {
            return boolValue;
        }

        return Convert.ToInt32(value) != 0;
    }

    public static string? ReadNullableString(IDataRecord reader, int? ordinal)
    {
        if (!ordinal.HasValue || reader.IsDBNull(ordinal.Value))
        {
            return null;
        }

        return Convert.ToString(reader.GetValue(ordinal.Value));
    }

    public static DateTime? ReadNullableDateTime(IDataRecord reader, int? ordinal)
    {
        if (!ordinal.HasValue || reader.IsDBNull(ordinal.Value))
        {
            return null;
        }

        return Convert.ToDateTime(reader.GetValue(ordinal.Value), CultureInfo.InvariantCulture);
    }

    public static int? FindOrdinal(IDataRecord reader, string columnName)
    {
        for (var i = 0; i < reader.FieldCount; i++)
        {
            if (string.Equals(reader.GetName(i), columnName, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        return null;
    }

    public static bool IsSqlTimeout(SqlException ex)
    {
        return ex.Number == -2;
    }

    public static bool IsSqlConnectionFailure(SqlException ex)
    {
        return ex.Number is -1 or 2 or 20 or 53 or 64 or 233 or 4060 or 10054 or 10060;
    }

    public static bool IsMissingStoredProcedure(SqlException ex)
    {
        return ex.Number == 2812;
    }
}
