using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Globalization;
using System.Linq;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Repositories;
using XTMon.Models;
using XTMon.Options;

namespace XTMon.Components.Pages;

public partial class Monitoring : ComponentBase, IAsyncDisposable
{
    private const string DatabaseNameColumn = "DatabaseName";
    private const string MonitoringLoadErrorMessage = "Unable to load monitoring data right now. Please try again.";
    private const string BackupLoadErrorMessage = "Unable to load DB backup overview data right now.";
    private static readonly HashSet<string> TextColumns = new(StringComparer.OrdinalIgnoreCase)
    {
        "FileGroup",
        "Autogrow"
    };

    private static readonly string[] CardColumnOrder =
    [
        "FileGroup",
        "AlertLevel",
        "AllocatedSpaceMB",
        "UsedSpaceMB",
        "FreeSpaceMB",
        "Autogrow",
        "FreeDriveMB",
        "PartSizeMB",
        "TotalFreeSpaceMB"
    ];

	private static readonly string[] SegmentColors =
	[
		"#06B6D4",
		"#0EA5E9",
		"#84CC16",
		"#8B5CF6",
		"#10B981",
		"#22C55E",
		"#14B8A6",
		"#3B82F6",
		"#6366F1",
		"#A855F7",
		"#2DD4BF",
		"#38BDF8"
	];

    [Inject]
    private IMonitoringRepository Repository { get; set; } = default!;

    [Inject]
    private IOptions<MonitoringOptions> MonitoringOptions { get; set; } = default!;

    [Inject]
    private ILogger<Monitoring> Logger { get; set; } = default!;

    private readonly CancellationTokenSource disposeCts = new();
    private MonitoringTableResult? result;
    private IReadOnlyList<DbCard> dbCards = Array.Empty<DbCard>();
    private bool isLoading;
    private string? loadError;
    private string? overviewLoadError;
    private string? lastUpdated;
    private DateTimeOffset? lastRefresh;
    private DateTimeOffset? overviewLastRefresh;
    
	private PieChartData fiChart = PieChartData.Empty("FI");
	private PieChartData gecdChart = PieChartData.Empty("GECD");

    private string ProcedureName => MonitoringOptions.Value.DbSizePlusDiskStoredProcedure;

    protected override async Task OnInitializedAsync()
    {
        await ReloadAsync();
    }

    private async Task ReloadAsync()
    {
        isLoading = true;
        
        await Task.WhenAll(ReloadOverviewAsync(), ReloadGridAsync());
        
        isLoading = false;
    }

    private async Task ReloadGridAsync()
    {
        loadError = null;
        try
        {
            result = await Repository.GetDbSizePlusDiskAsync(disposeCts.Token);
            lastUpdated = GetLastUpdatedDisplayValue();
            BuildDbCards();
            lastRefresh = DateTimeOffset.Now;
        }
        catch (Exception ex)
        {
            Logger.LogError(AppLogEvents.MonitoringLoadFailed, ex, "Monitoring data load failed for procedure {ProcedureName}.", ProcedureName);
            loadError = MonitoringLoadErrorMessage;
            lastUpdated = null;
        }
    }

	private async Task ReloadOverviewAsync()
	{
		overviewLoadError = null;
		try
		{
			var overviewResult = await Repository.GetDbBackupsAsync(disposeCts.Token);
			fiChart = BuildPieData(overviewResult, "FI");
			gecdChart = BuildPieData(overviewResult, "GECD");
			overviewLastRefresh = DateTimeOffset.Now;
		}
		catch (Exception ex)
		{
			Logger.LogError(AppLogEvents.MonitoringLoadFailed, ex, "Overview backup pie load failed.");
			overviewLoadError = BackupLoadErrorMessage;
			fiChart = PieChartData.Empty("FI");
			gecdChart = PieChartData.Empty("GECD");
		}
	}

    private void BuildDbCards()
    {
        if (result is null || result.Rows.Count == 0)
        {
            dbCards = Array.Empty<DbCard>();
            return;
        }

        var dbNameIndex = FindColumnIndex(DatabaseNameColumn);
        if (dbNameIndex < 0)
        {
            dbCards = Array.Empty<DbCard>();
            return;
        }

        var tableColumns = CardColumnOrder
            .Select(column => new { column, index = FindColumnIndex(column) })
            .Where(item => item.index >= 0)
            .ToList();

        dbCards = result.Rows
            .Where(row => row.Count > dbNameIndex)
            .GroupBy(row => row[dbNameIndex] ?? "Unknown")
            .Select(group =>
            {
                var rows = group
                    .Select(row =>
                        (IReadOnlyList<string?>)tableColumns
                            .Select(item => row.Count > item.index ? row[item.index] : null)
                            .ToList())
                    .ToList();

                return new DbCard(group.Key, tableColumns.Select(item => item.column).ToList(), rows);
            })
            .OrderBy(card => card.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private int FindColumnIndex(string columnName)
    {
        if (result is null)
        {
            return -1;
        }

        for (var i = 0; i < result.Columns.Count; i++)
        {
            if (string.Equals(result.Columns[i], columnName, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        return -1;
    }

    private static string ToHeaderLabel(string? columnName) =>
        JvCalculationHelper.ToHeaderLabel(columnName);

    private static string FormatCellValue(string? value, string columnName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "-";
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

        return value;
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

    private static string GetAlertLevelClass(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        return value.Trim().ToUpperInvariant() switch
        {
            "OK" => "alert-ok",
            "WARNING" => "alert-warning",
            "CRITICAL" => "alert-critical",
            _ => string.Empty
        };
    }

    private static int GetAlertColumnIndex(IReadOnlyList<string> columns)
    {
        for (var i = 0; i < columns.Count; i++)
        {
            if (columns[i].Equals("AlertLevel", StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        return -1;
    }

    private static bool IsAlertLevelColumn(string columnName)
    {
        return columnName.Equals("AlertLevel", StringComparison.OrdinalIgnoreCase);
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

    private sealed record DbCard(
        string Name,
        IReadOnlyList<string> Columns,
        IReadOnlyList<IReadOnlyList<string?>> Rows);

	private static PieChartData BuildPieData(MonitoringTableResult result, string metier)
	{
		var metierIndex = FindColumnIndex(result.Columns, "Metier");
		var spaceMbIndex = FindColumnIndex(result.Columns, "SpaceAllocatedMB", "SpaceAllocatedMb", "SpaceAllocated_MB");
		var databaseIndex = FindColumnIndex(result.Columns, "DatabaseName", "Database", "DbName", "DBName", "Name");

		if (metierIndex < 0 || spaceMbIndex < 0)
		{
			return PieChartData.Empty(metier, GetMetierDisplayName(metier));
		}

		var totalsByLabel = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);
		foreach (var row in result.Rows)
		{
			if (row.Count <= Math.Max(metierIndex, spaceMbIndex))
			{
				continue;
			}

			var metierValue = row[metierIndex];
			if (!string.Equals(metierValue?.Trim(), metier, StringComparison.OrdinalIgnoreCase))
			{
				continue;
			}

			var spaceRaw = row[spaceMbIndex];
			if (!TryParseDecimal(spaceRaw, out var spaceMb) || spaceMb <= 0)
			{
				continue;
			}

			var label = databaseIndex >= 0 && row.Count > databaseIndex && !string.IsNullOrWhiteSpace(row[databaseIndex])
				? row[databaseIndex]!.Trim()
				: "Unknown";

			totalsByLabel[label] = totalsByLabel.TryGetValue(label, out var existing)
				? existing + spaceMb
				: spaceMb;
		}

		if (totalsByLabel.Count == 0)
		{
			return PieChartData.Empty(metier, GetMetierDisplayName(metier));
		}

		var ordered = totalsByLabel
			.OrderByDescending(pair => pair.Value)
			.ToList();

		var total = ordered.Sum(item => item.Value);
		var slices = new List<PieSlice>(ordered.Count);
		for (var i = 0; i < ordered.Count; i++)
		{
			var (label, value) = ordered[i];
			var color = GetSegmentColor(i);
			slices.Add(new PieSlice(label, value, color));
		}

		var normalizedMetier = metier.ToUpperInvariant();
		return new PieChartData(normalizedMetier, GetMetierDisplayName(normalizedMetier), slices, total);
	}

	private static string GetSegmentColor(int rank)
	{
		if (SegmentColors.Length == 0)
		{
			return "#06B6D4";
		}

		var spacedIndex = (rank * 5) % SegmentColors.Length;
		return SegmentColors[spacedIndex];
	}

	private static string GetMetierDisplayName(string metier)
	{
		return metier.ToUpperInvariant() switch
		{
			"FI" => "FI",
			"GECD" => "GECD",
			_ => metier.ToUpperInvariant()
		};
	}

	private static int FindColumnIndex(IReadOnlyList<string> columns, params string[] candidates)
	{
		for (var i = 0; i < columns.Count; i++)
		{
			var normalized = NormalizeColumnName(columns[i]);
			foreach (var candidate in candidates)
			{
				if (normalized == NormalizeColumnName(candidate))
				{
					return i;
				}
			}
		}

		return -1;
	}

	private static bool TryParseDecimal(string? rawValue, out decimal parsed)
	{
		if (decimal.TryParse(rawValue, NumberStyles.Number, CultureInfo.InvariantCulture, out parsed))
		{
			return true;
		}

		return decimal.TryParse(rawValue, NumberStyles.Number, CultureInfo.CurrentCulture, out parsed);
	}

	private static string FormatSpaceMb(decimal value)
	{
		return value.ToString("N0", CultureInfo.InvariantCulture).Replace(",", " ", StringComparison.Ordinal) + " MB";
	}

	private static string BuildChartStyle(IReadOnlyList<PieSlice> slices, decimal total)
	{
		if (slices.Count == 0 || total <= 0)
		{
			return "background: var(--bg-surface-hover);";
		}

		var cumulativePercent = 0m;
		var segments = new List<string>(slices.Count);
		for (var i = 0; i < slices.Count; i++)
		{
			var slice = slices[i];
			var start = cumulativePercent;
			var end = i == slices.Count - 1
				? 100m
				: cumulativePercent + (slice.Value / total * 100m);

			segments.Add($"{slice.Color} {start.ToString("0.##", CultureInfo.InvariantCulture)}% {end.ToString("0.##", CultureInfo.InvariantCulture)}%");
			cumulativePercent = end;
		}

		return $"background: conic-gradient({string.Join(", ", segments)});";
	}

	private static string ToPercent(decimal value, decimal total)
	{
		if (total <= 0)
		{
			return "0%";
		}

		var percent = value / total * 100m;
		return percent.ToString("0.#", CultureInfo.InvariantCulture) + "%";
	}

	private sealed record PieSlice(string Label, decimal Value, string Color);

	private sealed record PieChartData(string Metier, string DisplayName, IReadOnlyList<PieSlice> Slices, decimal Total)
	{
		public static PieChartData Empty(string metier, string? displayName = null) => new(metier, displayName ?? metier, Array.Empty<PieSlice>(), 0);
		public bool HasData => Slices.Count > 0 && Total > 0;
		public string ChartStyle => BuildChartStyle(Slices, Total);
	}

    public ValueTask DisposeAsync()
    {
        disposeCts.Cancel();
        disposeCts.Dispose();
        return ValueTask.CompletedTask;
    }
}
