using System.Globalization;
using System.Text.Json;
using Microsoft.AspNetCore.Components;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;

namespace XTMon.Components.Pages;

public partial class ApplicationLogs : ComponentBase, IAsyncDisposable
{
    private const string LoadErrorMessage = "Unable to load application logs right now. Please try again.";
    private const string TimeInputFormat = "HH:mm";
    private static readonly string[] AcceptedTimeInputFormats = [TimeInputFormat, "HH:mm:ss"];
    private static readonly IReadOnlyList<string> AvailableLevels = ["Verbose", "Debug", "Information", "Warning", "Error", "Fatal"];
    private static readonly IReadOnlySet<string> DefaultLevels = new HashSet<string>(AvailableLevels, StringComparer.OrdinalIgnoreCase);

    [Inject]
    private IApplicationLogsRepository Repository { get; set; } = default!;

    [Inject]
    private IOptions<ApplicationLogsOptions> ApplicationLogsOptions { get; set; } = default!;

    [Inject]
    private ILogger<ApplicationLogs> Logger { get; set; } = default!;

    private readonly CancellationTokenSource disposeCts = new();
    private CancellationTokenSource? refreshCts;
    private IReadOnlyList<ApplicationLogRecord> rows = Array.Empty<ApplicationLogRecord>();
    private HashSet<int> expandedRowIds = new();
    private HashSet<string> selectedLevels = new(DefaultLevels, StringComparer.OrdinalIgnoreCase);
    private bool isLoading;
    private string? errorMessage;
    private DateTimeOffset? lastRefreshed;
    private DateOnly? fromDateInput;
    private TimeOnly fromTimeInput;
    private DateOnly? toDateInput;
    private TimeOnly toTimeInput;
    private string? messageContains;
    private int topN;

    private bool ViewerEnabled => ApplicationLogsOptions.Value.Enabled;
    private int MaxTopN => ApplicationLogsOptions.Value.MaxTopN;
    private string ProcedureName => ApplicationLogsOptions.Value.GetApplicationLogsStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(ApplicationLogsOptions.Value.ConnectionStringName, ProcedureName);
    private string FromTimeInputValue => FormatTimeInput(fromTimeInput);
    private string LastRefreshedText => lastRefreshed?.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) ?? "Not loaded";
    private string ResultSummaryText => rows.Count == 1 ? "1 row returned" : $"{rows.Count} rows returned";
    private string ToTimeInputValue => FormatTimeInput(toTimeInput);

    protected override async Task OnInitializedAsync()
    {
        ResetFilters();

        if (!ViewerEnabled)
        {
            return;
        }

        await RefreshAsync();
    }

    private async Task RefreshAsync()
    {
        if (!ViewerEnabled)
        {
            rows = Array.Empty<ApplicationLogRecord>();
            errorMessage = "Application log viewer is disabled by configuration.";
            return;
        }

        errorMessage = null;
        if (!TryBuildQuery(out var query))
        {
            return;
        }

        isLoading = true;
        CancelActiveRefresh();

        var localRefreshCts = CancellationTokenSource.CreateLinkedTokenSource(disposeCts.Token);
        refreshCts = localRefreshCts;

        try
        {
            rows = await Repository.GetApplicationLogsAsync(query, localRefreshCts.Token);
            expandedRowIds.Clear();
            lastRefreshed = DateTimeOffset.Now;
        }
        catch (OperationCanceledException) when (disposeCts.IsCancellationRequested || localRefreshCts.IsCancellationRequested)
        {
            return;
        }
        catch (TimeoutException)
        {
            errorMessage = "Application log query timed out. Narrow the filters and try again.";
        }
        catch (SqlException ex) when (SqlDataHelper.IsSqlTimeout(ex))
        {
            errorMessage = "Application log query timed out. Narrow the filters and try again.";
        }
        catch (SqlException)
        {
            errorMessage = "Application log query failed. Please try again.";
        }
        catch (Exception ex)
        {
            Logger.LogError(AppLogEvents.MonitoringLoadFailed, ex, "Application log page load failed for procedure {ProcedureName}.", ProcedureName);
            errorMessage = LoadErrorMessage;
        }
        finally
        {
            if (ReferenceEquals(refreshCts, localRefreshCts))
            {
                refreshCts = null;
            }

            localRefreshCts.Dispose();
            isLoading = false;
        }
    }

    private async Task ResetFiltersAsync()
    {
        ResetFilters();

        if (ViewerEnabled)
        {
            await RefreshAsync();
        }
    }

    private void ToggleRow(int id)
    {
        if (!expandedRowIds.Add(id))
        {
            expandedRowIds.Remove(id);
        }
    }

    private void OnFromDateChanged(DateOnly date)
    {
        fromDateInput = date;
    }

    private void OnToDateChanged(DateOnly date)
    {
        toDateInput = date;
    }

    private void OnFromTimeChanged(ChangeEventArgs args)
    {
        fromTimeInput = ParseTimeInputValue(args.Value, fromTimeInput);
    }

    private void OnToTimeChanged(ChangeEventArgs args)
    {
        toTimeInput = ParseTimeInputValue(args.Value, toTimeInput);
    }

    private bool IsRowExpanded(int id) => expandedRowIds.Contains(id);

    private bool IsLevelSelected(string level) => selectedLevels.Contains(level);

    private void OnLevelSelectionChanged(string level, ChangeEventArgs args)
    {
        var isSelected = args.Value as bool? == true;
        if (isSelected)
        {
            selectedLevels.Add(level);
            return;
        }

        selectedLevels.Remove(level);
    }

    private string GetLevelDisplay(string? level)
    {
        return string.IsNullOrWhiteSpace(level) ? "Unknown" : level.Trim();
    }

    private string GetLevelBadgeClass(string? level)
    {
        var normalized = GetLevelDisplay(level).ToUpperInvariant();
        return normalized switch
        {
            "ERROR" or "FATAL" => "pill bg-rose-50 text-rose-700 border border-rose-200",
            "WARNING" => "pill bg-amber-50 text-amber-700 border border-amber-200",
            _ => "pill bg-slate-100 text-slate-700 border border-slate-200"
        };
    }

    private string GetMessagePreview(string? message)
    {
        return string.IsNullOrWhiteSpace(message) ? "—" : message;
    }

    private string GetExpandedText(string? text)
    {
        return string.IsNullOrWhiteSpace(text) ? "—" : text;
    }

    private string FormatProperties(string? properties)
    {
        if (string.IsNullOrWhiteSpace(properties))
        {
            return "—";
        }

        try
        {
            var json = JsonSerializer.Deserialize<JsonElement>(properties);
            return JsonSerializer.Serialize(json, new JsonSerializerOptions
            {
                WriteIndented = true
            });
        }
        catch (JsonException)
        {
            return properties;
        }
    }

    private string FormatTimestamp(DateTime timeStamp)
    {
        return timeStamp.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture);
    }

    private void ResetFilters()
    {
        var nowUtc = DateTime.UtcNow;
        var defaults = ApplicationLogFilterHelper.ResolveTimeRange(
            null,
            null,
            ApplicationLogsOptions.Value.DefaultLookbackMinutes,
            nowUtc);

        selectedLevels = new HashSet<string>(DefaultLevels, StringComparer.OrdinalIgnoreCase);
        topN = ApplicationLogsOptions.Value.DefaultTopN;
        messageContains = null;
        ApplyTimeRangeInputs(defaults.From, defaults.To);
        errorMessage = null;
        expandedRowIds.Clear();
    }

    private bool TryBuildQuery(out ApplicationLogQuery query)
    {
        query = default!;

        topN = ApplicationLogFilterHelper.ClampTopN(topN, ApplicationLogsOptions.Value.DefaultTopN, ApplicationLogsOptions.Value.MaxTopN);
        var fromInput = CombineDateAndTime(fromDateInput, fromTimeInput);
        var toInput = CombineDateAndTime(toDateInput, toTimeInput);
        var timeRange = ApplicationLogFilterHelper.ResolveTimeRange(fromInput, toInput, ApplicationLogsOptions.Value.DefaultLookbackMinutes, DateTime.UtcNow);

        ApplyTimeRangeInputs(timeRange.From, timeRange.To);

        query = new ApplicationLogQuery(
            topN,
            timeRange.From,
            timeRange.To,
            ApplicationLogFilterHelper.NormalizeLevels(selectedLevels),
            string.IsNullOrWhiteSpace(messageContains) ? null : messageContains.Trim());

        return true;
    }

    private void ApplyTimeRangeInputs(DateTime from, DateTime to)
    {
        fromDateInput = DateOnly.FromDateTime(from);
        fromTimeInput = TimeOnly.FromDateTime(from);
        toDateInput = DateOnly.FromDateTime(to);
        toTimeInput = TimeOnly.FromDateTime(to);
    }

    private static DateTime? CombineDateAndTime(DateOnly? date, TimeOnly timeInput)
    {
        if (!date.HasValue)
        {
            return null;
        }

        return date.Value.ToDateTime(timeInput);
    }

    private static TimeOnly ParseTimeInputValue(object? value, TimeOnly fallback)
    {
        switch (value)
        {
            case TimeOnly timeOnly:
                return timeOnly;
            case DateTime dateTime:
                return TimeOnly.FromDateTime(dateTime);
            case DateTimeOffset dateTimeOffset:
                return TimeOnly.FromDateTime(dateTimeOffset.LocalDateTime);
            case string timeText when TimeOnly.TryParseExact(timeText, AcceptedTimeInputFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsedExact):
                return parsedExact;
            case string timeText when TimeOnly.TryParse(timeText, CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed):
                return parsed;
            default:
                return fallback;
        }
    }

    private static string FormatTimeInput(TimeOnly value)
    {
        return value.ToString(TimeInputFormat, CultureInfo.InvariantCulture);
    }

    private void CancelActiveRefresh()
    {
        refreshCts?.Cancel();
        refreshCts?.Dispose();
        refreshCts = null;
    }

    public ValueTask DisposeAsync()
    {
        CancelActiveRefresh();
        disposeCts.Cancel();
        disposeCts.Dispose();
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }
}