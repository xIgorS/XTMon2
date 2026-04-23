using XTMon.Models;

namespace XTMon.Helpers;

public enum DataValidationNavRunState
{
    NotRun,
    Running,
    Succeeded,
    Alert,
    Cancelled,
    Failed
}

internal static class DataValidationNavAlertHelper
{
    private static readonly HashSet<string> StatusKoRoutes = new(StringComparer.OrdinalIgnoreCase)
    {
        MonitoringJobHelper.BatchStatusSubmenuKey,
        "referential-data",
        "pricing",
        "pricing-file-reception",
        "feedout-extraction"
    };

    private static readonly HashSet<string> ContainsRowsRoutes = new(StringComparer.OrdinalIgnoreCase)
    {
        "out-of-scope-portfolio",
        "adjustments",
        "mirrorization",
        "result-transfer",
        "rollovered-portfolios",
        "sas-tables",
        "non-xtg-portfolio",
        "rejected-xtg-portfolio",
        "future-cash",
        "fact-pv-ca-consistency",
        "multiple-feed-version"
    };

    public static IReadOnlyList<string> SupportedRoutes { get; } = DataValidationCheckCatalog.Routes;

    internal static string BuildMetadataJson(string submenuKey, MonitoringTableResult? table)
    {
        var hasAlerts = ComputeHasAlerts(submenuKey, table);
        return $"{{\"hasAlerts\":{(hasAlerts ? "true" : "false")}}}";
    }

    internal static bool ComputeHasAlerts(string submenuKey, MonitoringTableResult? table)
    {
        var route = MonitoringJobHelper.BuildDataValidationSubmenuKey(submenuKey);

        if (StatusKoRoutes.Contains(route))
        {
            return HasColumnValue(table, "status", value => string.Equals(value, "KO", StringComparison.OrdinalIgnoreCase));
        }

        if (string.Equals(route, "daily-balance", StringComparison.OrdinalIgnoreCase))
        {
            return HasColumnValue(table, "status", value => value.StartsWith("KO", StringComparison.OrdinalIgnoreCase));
        }

        if (string.Equals(route, "market-data", StringComparison.OrdinalIgnoreCase))
        {
            return HasColumnValue(table, "result", value => string.Equals(value, "MISSING", StringComparison.OrdinalIgnoreCase));
        }

        if (ContainsRowsRoutes.Contains(route))
        {
            return table is { Rows.Count: > 0 };
        }

        return false;
    }

    public static DataValidationNavRunState GetRunState(MonitoringJobRecord? job)
    {
        if (job is null)
        {
            return DataValidationNavRunState.NotRun;
        }

        if (MonitoringJobHelper.ShouldTreatAsNotRun(job.Status, job.StartedAt))
        {
            return DataValidationNavRunState.NotRun;
        }

        if (MonitoringJobHelper.IsFailedStatus(job.Status)
            || (job.FailedAt.HasValue && !MonitoringJobHelper.IsCancelledStatus(job.Status)))
        {
            return DataValidationNavRunState.Failed;
        }

        if (MonitoringJobHelper.IsCancelledStatus(job.Status))
        {
            return DataValidationNavRunState.Cancelled;
        }

        if (MonitoringJobHelper.IsCompletedStatus(job.Status) || job.CompletedAt is not null)
        {
            if (HasAlertCondition(job))
            {
                return DataValidationNavRunState.Alert;
            }

            return DataValidationNavRunState.Succeeded;
        }

        if (MonitoringJobHelper.IsActiveStatus(job.Status))
        {
            return DataValidationNavRunState.Running;
        }

        return DataValidationNavRunState.NotRun;
    }

    private static bool HasAlertCondition(MonitoringJobRecord job)
    {
        if (MonitoringJobHelper.TryGetHasAlertsFromMetadata(job.MetadataJson, out var preComputed))
        {
            return preComputed;
        }

        var table = JvCalculationHelper.DeserializeMonitoringTable(job.GridColumnsJson, job.GridRowsJson);
        return ComputeHasAlerts(job.SubmenuKey, table);
    }

    private static bool HasColumnValue(MonitoringTableResult? table, string columnName, Func<string, bool> predicate)
    {
        if (table is null || table.Rows.Count == 0)
        {
            return false;
        }

        var columnIndex = FindColumnIndex(table.Columns, columnName);
        if (columnIndex < 0)
        {
            return false;
        }

        foreach (var row in table.Rows)
        {
            if (row.Count <= columnIndex)
            {
                continue;
            }

            var value = row[columnIndex]?.Trim();
            if (!string.IsNullOrWhiteSpace(value) && predicate(value))
            {
                return true;
            }
        }

        return false;
    }

    private static int FindColumnIndex(IReadOnlyList<string> columns, string columnName)
    {
        var normalizedTarget = MonitoringDisplayHelper.NormalizeColumnName(columnName);
        for (var i = 0; i < columns.Count; i++)
        {
            if (string.Equals(MonitoringDisplayHelper.NormalizeColumnName(columns[i]), normalizedTarget, StringComparison.Ordinal))
            {
                return i;
            }
        }

        return -1;
    }
}