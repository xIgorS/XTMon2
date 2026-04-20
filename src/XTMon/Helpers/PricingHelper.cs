namespace XTMon.Helpers;

internal static class PricingHelper
{
    public static string? BuildSourceSystemCodes(IEnumerable<string?> sourceSystemCodes, bool quoteEachValue = false)
    {
        var normalizedCodes = new List<string>();
        var seenCodes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var sourceSystemCode in sourceSystemCodes)
        {
            var trimmedCode = sourceSystemCode?.Trim();
            if (string.IsNullOrWhiteSpace(trimmedCode) || !seenCodes.Add(trimmedCode))
            {
                continue;
            }

            normalizedCodes.Add(quoteEachValue ? $"'{trimmedCode.Replace("'", "''")}'" : trimmedCode);
        }

        return normalizedCodes.Count == 0
            ? null
            : string.Join(",", normalizedCodes);
    }
}