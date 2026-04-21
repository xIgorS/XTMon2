namespace XTMon.Helpers;

internal sealed record DataValidationCheckDefinition(
    string Route,
    string DisplayName,
    bool RequiresRestrictedAccess = false);

internal static class DataValidationCheckCatalog
{
    public const string BatchRunRoute = "data-validation-runner";

    public static IReadOnlyList<DataValidationCheckDefinition> Checks { get; } =
    [
        new(MonitoringJobHelper.BatchStatusSubmenuKey, "Batch Status", RequiresRestrictedAccess: true),
        new("referential-data", "Referential Data"),
        new("market-data", "Market Data"),
        new("pricing-file-reception", "Pricing File Reception"),
        new("out-of-scope-portfolio", "Out of Scope Portfolio"),
        new("missing-sog-check", "Missing SOG Check"),
        new("adjustment-links-check", "Adjustment Links Check"),
        new("column-store-check", "Column Store Check"),
        new("trading-vs-fivr-check", "Trading vs Fivr Check"),
        new("mirrorization", "Mirrorization"),
        new("result-transfer", "Result Transfer"),
        new("rollovered-portfolios", "Rollovered Portfolios"),
        new("sas-tables", "SAS Tables"),
        new("non-xtg-portfolio", "Non XTG Portfolio"),
        new("rejected-xtg-portfolio", "Rejected XTG Portfolio"),
        new("feedout-extraction", "FeedOut Extraction"),
        new("future-cash", "Future Cash"),
        new("fact-pv-ca-consistency", "Fact PV/CA Consistency"),
        new("multiple-feed-version", "Multiple Feed Version"),
        new("daily-balance", "Daily Balance"),
        new("adjustments", "Adjustments"),
        new("pricing", "Pricing"),
        new("reverse-conso-file", "Reverse Conso File"),
        new("publication-consistency", "Publication Consistency"),
        new("jv-balance-consistency", "JV Balance Consistency"),
        new("missing-workflow-check", "Missing Workflow Check"),
        new("precalc-monitoring", "Precalc Monitoring"),
        new("vrdb-status", "VRDB Status")
    ];

    public static IReadOnlyList<string> Routes { get; } = Checks.Select(static check => check.Route).ToArray();

    public static bool IsKnownRoute(string? route)
    {
        var normalizedRoute = MonitoringJobHelper.BuildDataValidationSubmenuKey(route ?? string.Empty);
        return Routes.Contains(normalizedRoute, StringComparer.OrdinalIgnoreCase);
    }

    public static DataValidationCheckDefinition? Find(string? route)
    {
        var normalizedRoute = MonitoringJobHelper.BuildDataValidationSubmenuKey(route ?? string.Empty);
        return Checks.FirstOrDefault(check => string.Equals(check.Route, normalizedRoute, StringComparison.OrdinalIgnoreCase));
    }
}