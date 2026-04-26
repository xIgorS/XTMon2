namespace XTMon.Helpers;

internal static class ApplicationLogFilterHelper
{
    private static readonly HashSet<string> ValidLevels = new(StringComparer.OrdinalIgnoreCase)
    {
        "Verbose",
        "Debug",
        "Information",
        "Warning",
        "Error",
        "Fatal"
    };

    public static IReadOnlyList<string> NormalizeLevels(IEnumerable<string?>? levels)
    {
        if (levels is null)
        {
            return Array.Empty<string>();
        }

        var normalized = new List<string>();
        foreach (var level in levels)
        {
            if (string.IsNullOrWhiteSpace(level))
            {
                continue;
            }

            var trimmed = level.Trim();
            if (!ValidLevels.Contains(trimmed))
            {
                continue;
            }

            var canonical = GetCanonicalLevel(trimmed);
            if (!normalized.Contains(canonical, StringComparer.OrdinalIgnoreCase))
            {
                normalized.Add(canonical);
            }
        }

        return normalized;
    }

    public static string? ToCsv(IReadOnlyCollection<string> levels)
    {
        return levels.Count == 0 ? null : string.Join(",", levels);
    }

    public static int ClampTopN(int requested, int defaultN, int maxN)
    {
        if (requested < 1)
        {
            return defaultN;
        }

        return Math.Min(requested, maxN);
    }

    public static (DateTime From, DateTime To) ResolveTimeRange(DateTime? from, DateTime? to, int defaultLookbackMinutes, DateTime nowUtc)
    {
        var resolvedTo = to ?? nowUtc;
        var resolvedFrom = from ?? resolvedTo.AddMinutes(-defaultLookbackMinutes);

        if (resolvedFrom <= resolvedTo)
        {
            return (resolvedFrom, resolvedTo);
        }

        return (resolvedTo, resolvedFrom);
    }

    private static string GetCanonicalLevel(string level)
    {
        return level.Trim().ToUpperInvariant() switch
        {
            "VERBOSE" => "Verbose",
            "DEBUG" => "Debug",
            "INFORMATION" => "Information",
            "WARNING" => "Warning",
            "ERROR" => "Error",
            "FATAL" => "Fatal",
            _ => level
        };
    }
}