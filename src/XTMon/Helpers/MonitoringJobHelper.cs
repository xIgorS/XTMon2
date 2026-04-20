using System.Text.Json;
using XTMon.Models;

namespace XTMon.Helpers;

internal static class MonitoringJobHelper
{
    public const string DataValidationCategory = "DataValidation";
    public const string FunctionalRejectionCategory = "FunctionalRejection";
    public const string BatchStatusSubmenuKey = "batch-status";

    public static string BuildDataValidationSubmenuKey(string route)
    {
        return string.IsNullOrWhiteSpace(route)
            ? string.Empty
            : route.Trim().TrimStart('/').ToLowerInvariant();
    }

    public static string BuildFunctionalRejectionSubmenuKey(
        int businessDataTypeId,
        string sourceSystemName,
        string dbConnection,
        string? sourceSystemBusinessDataTypeCode)
    {
        return string.Create(
            System.Globalization.CultureInfo.InvariantCulture,
            $"fr|{businessDataTypeId}|{Uri.EscapeDataString(sourceSystemName.Trim())}|{Uri.EscapeDataString(dbConnection.Trim())}|{Uri.EscapeDataString((sourceSystemBusinessDataTypeCode ?? string.Empty).Trim())}");
    }

    public static string BuildFunctionalRejectionParameterSummary(FunctionalRejectionJobParameters parameters)
    {
        return string.Create(
            System.Globalization.CultureInfo.InvariantCulture,
            $"Code: {parameters.SourceSystemBusinessDataTypeCode ?? "-"} | BusinessDataTypeId: {parameters.BusinessDataTypeId} | Source system: {parameters.SourceSystemName} | Db: {parameters.DbConnection}");
    }

    public static string SerializeParameters<T>(T value)
    {
        return JsonSerializer.Serialize(value);
    }

    public static T? DeserializeParameters<T>(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return default;
        }

        try
        {
            return JsonSerializer.Deserialize<T>(json);
        }
        catch
        {
            return default;
        }
    }

    public static string? SerializeTechnicalRejectColumns(IReadOnlyList<TechnicalRejectColumn> columns)
    {
        return columns.Count == 0 ? null : JsonSerializer.Serialize(columns);
    }

    public static IReadOnlyList<TechnicalRejectColumn> DeserializeTechnicalRejectColumns(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return Array.Empty<TechnicalRejectColumn>();
        }

        try
        {
            return JsonSerializer.Deserialize<List<TechnicalRejectColumn>>(json)?.ToArray() ?? Array.Empty<TechnicalRejectColumn>();
        }
        catch
        {
            return Array.Empty<TechnicalRejectColumn>();
        }
    }

    public static bool IsTerminalStatus(string? status)
    {
        return string.Equals(status, "Completed", StringComparison.OrdinalIgnoreCase)
            || string.Equals(status, "Failed", StringComparison.OrdinalIgnoreCase);
    }

    public static bool IsActiveStatus(string? status)
    {
        return string.Equals(status, "Queued", StringComparison.OrdinalIgnoreCase)
            || string.Equals(status, "Running", StringComparison.OrdinalIgnoreCase);
    }
}