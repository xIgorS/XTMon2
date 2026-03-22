using System.Globalization;
using XTMon.Models;

namespace XTMon.Helpers;

internal enum ReplayStatusKind
{
    Pending,
    InProgress,
    Completed
}

internal static class ReplayFlowsHelper
{
    private const string DisplayDateFormat = "dd-MM-yyyy";
    private const string DisplayDateTimeFormat = "dd-MM-yyyy HH:mm:ss";

    private const string NormalizedCompleted = "completed";
    private const string NormalizedSubmissionCompleted = "submissioncompleted";
    private const string NormalizedDone = "done";
    private const string NormalizedSuccess = "success";
    private const string NormalizedInProgress = "inprogress";
    private const string NormalizedSubmissionStarted = "submissionstarted";
    private const string NormalizedSubmissionStartedTypo = "submissonstarted"; // DB typo — intentional
    private const string NormalizedProcessing = "processing";
    private const string NormalizedRunning = "running";
    private const string NormalizedPending = "pending";
    private const string NormalizedInserted = "inserted";
    private const string NormalizedQueued = "queued";
    private const string NormalizedNew = "new";

    public static bool TryNormalizeReplayFlowSet(string? replayFlowSet, out string? normalizedReplayFlowSet)
    {
        normalizedReplayFlowSet = null;

        if (string.IsNullOrWhiteSpace(replayFlowSet))
        {
            return true;
        }

        var parts = replayFlowSet.Split(',', StringSplitOptions.TrimEntries);
        if (parts.Length == 0)
        {
            return false;
        }

        foreach (var part in parts)
        {
            if (string.IsNullOrWhiteSpace(part))
            {
                return false;
            }

            if (!long.TryParse(part, NumberStyles.None, CultureInfo.InvariantCulture, out _))
            {
                return false;
            }
        }

        normalizedReplayFlowSet = string.Join(',', parts);
        return true;
    }

    public static ReplayStatusKind GetStatusKind(ReplayFlowStatusRow row)
    {
        if (!string.IsNullOrWhiteSpace(row.Status))
        {
            var normalized = row.Status.Trim().Replace("_", string.Empty).Replace("-", string.Empty).Replace(" ", string.Empty);

            if (normalized.Equals(NormalizedCompleted, StringComparison.OrdinalIgnoreCase) ||
                normalized.Equals(NormalizedSubmissionCompleted, StringComparison.OrdinalIgnoreCase) ||
                normalized.Equals(NormalizedDone, StringComparison.OrdinalIgnoreCase) ||
                normalized.Equals(NormalizedSuccess, StringComparison.OrdinalIgnoreCase))
            {
                return ReplayStatusKind.Completed;
            }

            if (normalized.Equals(NormalizedInProgress, StringComparison.OrdinalIgnoreCase) ||
                normalized.Equals(NormalizedSubmissionStarted, StringComparison.OrdinalIgnoreCase) ||
                normalized.Equals(NormalizedSubmissionStartedTypo, StringComparison.OrdinalIgnoreCase) ||
                normalized.Equals(NormalizedProcessing, StringComparison.OrdinalIgnoreCase) ||
                normalized.Equals(NormalizedRunning, StringComparison.OrdinalIgnoreCase))
            {
                return ReplayStatusKind.InProgress;
            }

            if (normalized.Equals(NormalizedPending, StringComparison.OrdinalIgnoreCase) ||
                normalized.Equals(NormalizedInserted, StringComparison.OrdinalIgnoreCase) ||
                normalized.Equals(NormalizedQueued, StringComparison.OrdinalIgnoreCase) ||
                normalized.Equals(NormalizedNew, StringComparison.OrdinalIgnoreCase))
            {
                return ReplayStatusKind.Pending;
            }
        }

        if (row.DateCompleted is not null)
        {
            return ReplayStatusKind.Completed;
        }

        if (row.DateStarted is not null)
        {
            return ReplayStatusKind.InProgress;
        }

        return ReplayStatusKind.Pending;
    }

    public static string FormatDate(DateOnly? value)
    {
        return value is null ? "-" : value.Value.ToString(DisplayDateFormat, CultureInfo.InvariantCulture);
    }

    public static string FormatDateTime(DateTime? value)
    {
        return value is null ? "-" : value.Value.ToString(DisplayDateTimeFormat, CultureInfo.InvariantCulture);
    }

    public static string FormatNumber(long? value)
    {
        return value.HasValue ? value.Value.ToString("N0", CultureInfo.InvariantCulture).Replace(",", " ") : "-";
    }

    /// <param name="rawDuration">Pre-formatted duration string from DB; returned as-is when non-empty.</param>
    /// <param name="dateStarted">When the flow started.</param>
    /// <param name="dateCompleted">When the flow completed; uses UtcNow when null (still running).</param>
    public static string FormatDuration(string? rawDuration, DateTime? dateStarted, DateTime? dateCompleted)
    {
        if (!string.IsNullOrWhiteSpace(rawDuration))
        {
            return rawDuration;
        }

        if (dateStarted is null)
        {
            return "-";
        }

        var end = dateCompleted ?? DateTime.UtcNow;
        var duration = end - dateStarted.Value;

        // Guard against clock skew between app server and DB server
        if (duration < TimeSpan.Zero)
        {
            return "0s";
        }

        if (duration.TotalHours >= 1)
        {
            return $"{(int)duration.TotalHours}h {duration.Minutes:D2}m {duration.Seconds:D2}s";
        }

        if (duration.TotalMinutes >= 1)
        {
            return $"{duration.Minutes}m {duration.Seconds:D2}s";
        }

        return $"{duration.Seconds}s";
    }
}
