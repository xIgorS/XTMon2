using Microsoft.AspNetCore.Components;

namespace XTMon.Components.Layout;

public partial class NavMenu : ComponentBase, IDisposable
{
	[Inject]
	private NavigationManager NavigationManager { get; set; } = default!;

	protected override void OnInitialized()
	{
		NavigationManager.LocationChanged += OnLocationChanged;
	}

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
		IsCurrentRoute("jv-balance-consistency") ||
		IsCurrentRoute("missing-workflow-check") ||
		IsCurrentRoute("precalc-monitoring") ||
		IsCurrentRoute("vrdb-status");

	private bool IsFunctionalRejectionRoute => IsCurrentRoute("functional-rejection");

	public void Dispose()
	{
		NavigationManager.LocationChanged -= OnLocationChanged;
	}

	private void OnLocationChanged(object? sender, Microsoft.AspNetCore.Components.Routing.LocationChangedEventArgs e)
	{
		_ = InvokeAsync(StateHasChanged);
	}

	private bool IsCurrentRoute(string route)
	{
		var relativePath = NavigationManager.ToBaseRelativePath(NavigationManager.Uri);
		var pathOnly = relativePath.Split('?', 2)[0].TrimEnd('/');
		return string.Equals(pathOnly, route, StringComparison.OrdinalIgnoreCase);
	}
}
