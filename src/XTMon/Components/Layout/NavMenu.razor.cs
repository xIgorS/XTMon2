using Microsoft.AspNetCore.Components;

namespace XTMon.Components.Layout;

public partial class NavMenu : ComponentBase
{
	[Inject]
	private NavigationManager NavigationManager { get; set; } = default!;

	private bool IsDataValidationRoute =>
		IsCurrentRoute("batch-status") ||
		IsCurrentRoute("referential-data") ||
		IsCurrentRoute("market-data") ||
		IsCurrentRoute("pricing-file-reception") ||
		IsCurrentRoute("out-of-scope-portfolio") ||
		IsCurrentRoute("missing-sog-check") ||
		IsCurrentRoute("adjustment-links-check") ||
		IsCurrentRoute("column-store-check") ||
		IsCurrentRoute("trading-vs-fivr-check") ||
		IsCurrentRoute("mirrorization") ||
		IsCurrentRoute("result-transfer") ||
		IsCurrentRoute("rollovered-portfolios") ||
		IsCurrentRoute("sas-tables") ||
		IsCurrentRoute("non-xtg-portfolio") ||
		IsCurrentRoute("rejected-xtg-portfolio") ||
		IsCurrentRoute("feedout-extraction") ||
		IsCurrentRoute("future-cash") ||
		IsCurrentRoute("fact-pv-ca-consistency") ||
		IsCurrentRoute("multiple-feed-version") ||
		IsCurrentRoute("daily-balance") ||
		IsCurrentRoute("adjustments") ||
		IsCurrentRoute("pricing") ||
		IsCurrentRoute("reverse-conso-file") ||
		IsCurrentRoute("publication-consistency") ||
		IsCurrentRoute("jv-balance-consistency");

	private bool IsCurrentRoute(string route)
	{
		var relativePath = NavigationManager.ToBaseRelativePath(NavigationManager.Uri).TrimEnd('/');
		return string.Equals(relativePath, route, StringComparison.OrdinalIgnoreCase);
	}
}
