using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Globalization;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Repositories;
using XTMon.Models;
using XTMon.Options;

namespace XTMon.Components.Pages;

public partial class DbBackupInfo : ComponentBase
{
    private const string MonitoringLoadErrorMessage = "Unable to load DB backup data right now. Please try again.";
    private static readonly HashSet<string> TextColumns = new(StringComparer.OrdinalIgnoreCase)
    {
        "FileGroup",
        "Autogrow"
    };

    [Inject]
    private IMonitoringRepository Repository { get; set; } = default!;

    [Inject]
    private IOptions<MonitoringOptions> MonitoringOptions { get; set; } = default!;

    [Inject]
    private ILogger<DbBackupInfo> Logger { get; set; } = default!;

    private MonitoringTableResult? result;
    private bool isLoading;
    private string? loadError;
    private DateTimeOffset? lastRefresh;

    private string ProcedureName => MonitoringOptions.Value.DbBackupsStoredProcedure;

    private sealed record GridColumn(int Index, string Name);

    protected override async Task OnInitializedAsync()
    {
        await ReloadAsync();
    }

    private async Task ReloadAsync()
    {
        isLoading = true;
        loadError = null;

        try
        {
            result = await Repository.GetDbBackupsAsync(CancellationToken.None);
            lastRefresh = DateTimeOffset.Now;
        }
        catch (Exception ex)
        {
            Logger.LogError(AppLogEvents.MonitoringLoadFailed, ex, "DB backup info load failed for procedure {ProcedureName}.", ProcedureName);
            loadError = MonitoringLoadErrorMessage;
        }
        finally
        {
            isLoading = false;
        }
    }

    private static string ToHeaderLabel(string? columnName) =>
        JvCalculationHelper.ToHeaderLabel(columnName);

    private static string FormatCellValue(string? value, string columnName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "-";
        }

        if (IsNoSecondsBackupColumn(columnName) &&
            DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var backupTime))
        {
            return backupTime.ToString("dd-MM-yyyy HH:mm", CultureInfo.InvariantCulture);
        }

        if (IsDateLikeColumn(columnName) &&
            DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var dateTimeValue))
        {
            return dateTimeValue.ToString("dd-MM-yyyy HH:mm:ss", CultureInfo.InvariantCulture);
        }

        if (long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var number))
        {
            var formatted = FormatWithSpaces(number);
            if (columnName.EndsWith("MB", StringComparison.OrdinalIgnoreCase))
            {
                return formatted + " MB";
            }

            return formatted;
        }

        if (decimal.TryParse(value, NumberStyles.Number, CultureInfo.InvariantCulture, out var decimalNumber))
        {
            var formatted = FormatWithSpaces(decimalNumber);
            if (columnName.EndsWith("MB", StringComparison.OrdinalIgnoreCase))
            {
                return formatted + " MB";
            }

            return formatted;
        }

        return value;
    }

    private static bool IsNoSecondsBackupColumn(string columnName)
    {
        var normalized = NormalizeColumnName(columnName);
        return normalized is "lastdifferentialbackup" or "lastdifferentialbackupdate" or "lastfullbackup" or "lastfullbackupdate";
    }

    private static bool IsDateLikeColumn(string columnName)
    {
        var normalized = NormalizeColumnName(columnName);
        return normalized.Contains("date", StringComparison.Ordinal) ||
               normalized.Contains("time", StringComparison.Ordinal) ||
               normalized.Contains("updated", StringComparison.Ordinal);
    }

    private static string FormatWithSpaces(long value)
    {
        return value.ToString("N0", CultureInfo.InvariantCulture).Replace(",", " ");
    }

    private static string FormatWithSpaces(decimal value)
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

    private static string GetColumnAlignmentClass(string columnName)
    {
        if (columnName.Equals("AlertLevel", StringComparison.OrdinalIgnoreCase))
        {
            return "db-grid__cell--status";
        }

        if (TextColumns.Contains(columnName))
        {
            return "db-grid__cell--text";
        }

        return "db-grid__cell--num";
    }

    private IReadOnlyList<GridColumn> GetGridColumns()
    {
        if (result is null)
        {
            return Array.Empty<GridColumn>();
        }

        var lastUpdatedIndex = FindLastUpdatedColumnIndex();
        var columns = new List<GridColumn>(result.Columns.Count);
        for (var i = 0; i < result.Columns.Count; i++)
        {
            if (i == lastUpdatedIndex)
            {
                continue;
            }

            columns.Add(new GridColumn(i, result.Columns[i]));
        }

        return columns;
    }

    private string? GetLastUpdatedDisplayValue()
    {
        if (result is null || result.Rows.Count == 0)
        {
            return null;
        }

        var lastUpdatedIndex = FindLastUpdatedColumnIndex();
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

    private int FindLastUpdatedColumnIndex()
    {
        if (result is null)
        {
            return -1;
        }

        for (var i = 0; i < result.Columns.Count; i++)
        {
            var normalized = NormalizeColumnName(result.Columns[i]);
            if (normalized is "lastupdated" or "lastupdateddate")
            {
                return i;
            }
        }

        return -1;
    }

    private static string NormalizeColumnName(string? columnName)
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

    private static string FormatDateTimeForDisplay(string value)
    {
        if (DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsed))
        {
            return parsed.ToString("dd-MM-yyyy HH:mm:ss", CultureInfo.InvariantCulture);
        }

        return value;
    }
}
