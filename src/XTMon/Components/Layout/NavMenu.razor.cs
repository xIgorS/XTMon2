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
		IsCurrentRoute("mirrorization") ||
		IsCurrentRoute("result-transfer") ||
		IsCurrentRoute("daily-balance") ||
		IsCurrentRoute("adjustments") ||
		IsCurrentRoute("pricing");

	private bool IsCurrentRoute(string route)
	{
		var relativePath = NavigationManager.ToBaseRelativePath(NavigationManager.Uri).TrimEnd('/');
		return string.Equals(relativePath, route, StringComparison.OrdinalIgnoreCase);
	}
}
