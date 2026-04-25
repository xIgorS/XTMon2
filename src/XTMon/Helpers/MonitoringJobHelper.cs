using System.Text.Json;
using XTMon.Models;

namespace XTMon.Helpers;

internal static class MonitoringJobHelper
{
    public const string DataValidationCategory = "DataValidation";
    public const string FunctionalRejectionCategory = "FunctionalRejection";
    public const string BatchStatusSubmenuKey = "batch-status";
    public const string DailyBalanceSubmenuKey = "daily-balance";
    public const string PricingSubmenuKey = "pricing";
    public static IReadOnlyList<string> AllCategories { get; } = [DataValidationCategory, FunctionalRejectionCategory];
    public const string QueuedStatus = "Queued";
    public const string RunningStatus = "Running";
    public const string CompletedStatus = "Completed";
    public const string FailedStatus = "Failed";
    public const string CancelledStatus = "Cancelled";

    public static string GetCategoryDisplayName(string category)
    {
        if (string.Equals(category, DataValidationCategory, StringComparison.OrdinalIgnoreCase))
        {
            return "Data Validation";
        }

        if (string.Equals(category, FunctionalRejectionCategory, StringComparison.OrdinalIgnoreCase))
        {
            return "Functional Rejection";
        }

        return category;
    }

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

    public static string? SerializeTechnicalRejectColumns(IReadOnlyList<TechnicalRejectColumn> columns, bool hasAlerts)
    {
        if (columns.Count == 0 && !hasAlerts)
        {
            return null;
        }
        return JsonSerializer.Serialize(new { hasAlerts, columns });
    }

    public static IReadOnlyList<TechnicalRejectColumn> DeserializeTechnicalRejectColumns(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return Array.Empty<TechnicalRejectColumn>();
        }

        try
        {
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind == JsonValueKind.Object &&
                doc.RootElement.TryGetProperty("columns", out var columnsProp))
            {
                return columnsProp.Deserialize<List<TechnicalRejectColumn>>()?.ToArray()
                    ?? Array.Empty<TechnicalRejectColumn>();
            }
            if (doc.RootElement.ValueKind == JsonValueKind.Array)
            {
                return doc.RootElement.Deserialize<List<TechnicalRejectColumn>>()?.ToArray()
                    ?? Array.Empty<TechnicalRejectColumn>();
            }
        }
        catch (JsonException)
        {
        }
        return Array.Empty<TechnicalRejectColumn>();
    }

    public static bool TryGetHasAlertsFromMetadata(string? metadataJson, out bool hasAlerts)
    {
        hasAlerts = false;
        if (string.IsNullOrWhiteSpace(metadataJson))
        {
            return false;
        }

        try
        {
            using var doc = JsonDocument.Parse(metadataJson);
            if (doc.RootElement.ValueKind == JsonValueKind.Object &&
                doc.RootElement.TryGetProperty("hasAlerts", out var prop) &&
                (prop.ValueKind == JsonValueKind.True || prop.ValueKind == JsonValueKind.False))
            {
                hasAlerts = prop.GetBoolean();
                return true;
            }
        }
        catch (JsonException)
        {
        }
        return false;
    }

    public static bool IsTerminalStatus(string? status)
    {
        return IsCompletedStatus(status)
            || IsFailedStatus(status)
            || IsCancelledStatus(status);
    }

    public static bool IsActiveStatus(string? status)
    {
        return IsQueuedStatus(status)
            || IsRunningStatus(status);
    }

    public static bool IsQueuedStatus(string? status)
    {
        return string.Equals(status, QueuedStatus, StringComparison.OrdinalIgnoreCase);
    }

    public static bool IsRunningStatus(string? status)
    {
        return string.Equals(status, RunningStatus, StringComparison.OrdinalIgnoreCase);
    }

    public static bool IsCompletedStatus(string? status)
    {
        return string.Equals(status, CompletedStatus, StringComparison.OrdinalIgnoreCase);
    }

    public static bool IsFailedStatus(string? status)
    {
        return string.Equals(status, FailedStatus, StringComparison.OrdinalIgnoreCase);
    }

    public static bool IsCancelledStatus(string? status)
    {
        return string.Equals(status, CancelledStatus, StringComparison.OrdinalIgnoreCase);
    }

    public static bool ShouldTreatAsNotRun(string? status, DateTime? startedAt)
    {
        return IsCancelledStatus(status) && !startedAt.HasValue;
    }
}