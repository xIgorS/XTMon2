using System.IO.Compression;
using System.Text;
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
    public static StringComparer SubmenuKeyComparer { get; } = StringComparer.OrdinalIgnoreCase;
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

    public static MonitoringTableResult TruncateRows(MonitoringTableResult source, int maxRows, out int totalRowCount, out bool truncated)
    {
        ArgumentNullException.ThrowIfNull(source);

        totalRowCount = source.Rows.Count;
        truncated = maxRows > 0 && totalRowCount > maxRows;
        if (!truncated)
        {
            return source;
        }

        return new MonitoringTableResult(
            source.Columns,
            source.Rows.Take(maxRows).ToArray());
    }

    public static byte[] BuildFullResultCsvGzip(MonitoringTableResult fullResult, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(fullResult);

        using var memoryStream = new MemoryStream();
        using (var gzipStream = new GZipStream(memoryStream, CompressionLevel.Optimal, leaveOpen: true))
        using (var writer = new StreamWriter(gzipStream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), leaveOpen: true))
        {
            WriteCsvRow(writer, fullResult.Columns);

            foreach (var row in fullResult.Rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                WriteCsvRow(writer, row);
            }

            writer.Flush();
        }

        return memoryStream.ToArray();
    }

    public static string BuildPersistMetadataJson(int totalRowCount, int persistedRowCount, string? extra = null)
    {
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);

        writer.WriteStartObject();
        writer.WriteNumber("totalRowCount", totalRowCount);
        writer.WriteNumber("persistedRowCount", persistedRowCount);
        writer.WriteBoolean("truncated", persistedRowCount < totalRowCount);

        if (!string.IsNullOrWhiteSpace(extra))
        {
            try
            {
                using var extraDocument = JsonDocument.Parse(extra);
                writer.WritePropertyName("extra");
                extraDocument.RootElement.WriteTo(writer);
            }
            catch (JsonException)
            {
            }
        }

        writer.WriteEndObject();
        writer.Flush();

        return Encoding.UTF8.GetString(stream.ToArray());
    }

    public static bool TryReadPersistMetadata(string? metadataJson, out int totalRowCount, out int persistedRowCount, out bool truncated)
    {
        totalRowCount = 0;
        persistedRowCount = 0;
        truncated = false;

        if (string.IsNullOrWhiteSpace(metadataJson))
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(metadataJson);
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                return false;
            }

            if (!TryReadInt32(document.RootElement, "totalRowCount", out totalRowCount)
                || !TryReadInt32(document.RootElement, "persistedRowCount", out persistedRowCount))
            {
                totalRowCount = 0;
                persistedRowCount = 0;
                return false;
            }

            if (document.RootElement.TryGetProperty("truncated", out var truncatedProperty)
                && (truncatedProperty.ValueKind == JsonValueKind.True || truncatedProperty.ValueKind == JsonValueKind.False))
            {
                truncated = truncatedProperty.GetBoolean();
            }
            else
            {
                truncated = persistedRowCount < totalRowCount;
            }

            return true;
        }
        catch (JsonException)
        {
            return false;
        }
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
            var metadataRoot = ResolveMetadataPayloadRoot(doc.RootElement);
            if (metadataRoot.ValueKind == JsonValueKind.Object &&
                metadataRoot.TryGetProperty("columns", out var columnsProp))
            {
                return columnsProp.Deserialize<List<TechnicalRejectColumn>>()?.ToArray()
                    ?? Array.Empty<TechnicalRejectColumn>();
            }
            if (metadataRoot.ValueKind == JsonValueKind.Array)
            {
                return metadataRoot.Deserialize<List<TechnicalRejectColumn>>()?.ToArray()
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
            var metadataRoot = ResolveMetadataPayloadRoot(doc.RootElement);
            if (metadataRoot.ValueKind == JsonValueKind.Object &&
                metadataRoot.TryGetProperty("hasAlerts", out var prop) &&
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

    private static void WriteCsvRow(TextWriter writer, IReadOnlyList<string?> values)
    {
        for (var i = 0; i < values.Count; i++)
        {
            if (i > 0)
            {
                writer.Write(',');
            }

            WriteCsvField(writer, values[i]);
        }

        writer.Write("\r\n");
    }

    private static void WriteCsvField(TextWriter writer, string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return;
        }

        var requiresQuoting = value.IndexOfAny([',', '"', '\r', '\n']) >= 0;
        if (!requiresQuoting)
        {
            writer.Write(value);
            return;
        }

        writer.Write('"');
        foreach (var character in value)
        {
            if (character == '"')
            {
                writer.Write("\"\"");
            }
            else
            {
                writer.Write(character);
            }
        }

        writer.Write('"');
    }

    private static JsonElement ResolveMetadataPayloadRoot(JsonElement root)
    {
        if (root.ValueKind == JsonValueKind.Object
            && root.TryGetProperty("extra", out var extra)
            && (extra.ValueKind == JsonValueKind.Object || extra.ValueKind == JsonValueKind.Array))
        {
            return extra;
        }

        return root;
    }

    private static bool TryReadInt32(JsonElement root, string propertyName, out int value)
    {
        value = 0;
        if (!root.TryGetProperty(propertyName, out var property))
        {
            return false;
        }

        if (property.ValueKind == JsonValueKind.Number)
        {
            return property.TryGetInt32(out value);
        }

        return property.ValueKind == JsonValueKind.String
            && int.TryParse(property.GetString(), out value);
    }
}