using Microsoft.AspNetCore.Components;
using XTMon.Helpers;
using XTMon.Services;

namespace XTMon.Components.Layout;

public partial class NavMenu : ComponentBase, IDisposable
{
	private readonly CancellationTokenSource _disposeCts = new();

	[Inject]
	private NavigationManager NavigationManager { get; set; } = default!;

	[Inject]
	private PnlDateState PnlDateState { get; set; } = default!;

	[Inject]
	private DataValidationNavAlertState DataValidationNavAlertState { get; set; } = default!;

	protected override void OnInitialized()
	{
		NavigationManager.LocationChanged += OnLocationChanged;
		PnlDateState.OnDateChanged += OnPnlDateChanged;
		DataValidationNavAlertState.StatusesChanged += OnDataValidationStatusesChanged;
	}

	protected override async Task OnInitializedAsync()
	{
		await RefreshDataValidationAlertsAsync();
	}

	private bool IsDataValidationRoute =>
		IsCurrentRoute(DataValidationCheckCatalog.BatchRunRoute) ||
		DataValidationCheckCatalog.Routes.Any(IsCurrentRoute);

	private bool IsFunctionalRejectionRoute =>
		IsCurrentRoute("functional-rejection") ||
		IsCurrentRoute("functional-rejection-runner");

	public void Dispose()
	{
		NavigationManager.LocationChanged -= OnLocationChanged;
		PnlDateState.OnDateChanged -= OnPnlDateChanged;
		DataValidationNavAlertState.StatusesChanged -= OnDataValidationStatusesChanged;
		_disposeCts.Cancel();
		_disposeCts.Dispose();
	}

	private void OnLocationChanged(object? sender, Microsoft.AspNetCore.Components.Routing.LocationChangedEventArgs e)
	{
		_ = InvokeAsync(StateHasChanged);
	}

	private void OnPnlDateChanged()
	{
		_ = InvokeAsync(async () =>
		{
			await RefreshDataValidationAlertsAsync();
			StateHasChanged();
		});
	}

	private void OnDataValidationStatusesChanged()
	{
		_ = InvokeAsync(StateHasChanged);
	}

	private async Task RefreshDataValidationAlertsAsync()
	{
		try
		{
			await DataValidationNavAlertState.RefreshAsync(_disposeCts.Token);
		}
		catch (OperationCanceledException)
		{
		}
	}

	private DataValidationNavRunState GetDataValidationRunState(string route)
	{
		return DataValidationNavAlertState.GetStatus(route);
	}

	private DataValidationNavRunState GetDataValidationAggregateState()
	{
		return DataValidationNavAlertState.GetAggregateStatus();
	}

	private static string GetMainMenuIndicatorClass(DataValidationNavRunState status)
	{
		return status switch
		{
			DataValidationNavRunState.Failed => "submenu-status-indicator submenu-status-badge--failed",
			DataValidationNavRunState.Alert => "submenu-status-indicator submenu-status-badge--failed",
			DataValidationNavRunState.Succeeded => "submenu-status-indicator submenu-status-badge--succeeded",
			DataValidationNavRunState.Running => "submenu-status-indicator submenu-status-badge--running",
			_ => "submenu-status-indicator submenu-status-badge--not-run"
		};
	}

	private static string GetMainMenuIndicatorDescription(DataValidationNavRunState status)
	{
		return status switch
		{
			DataValidationNavRunState.Failed => "One or more checks failed or raised alerts",
			DataValidationNavRunState.Alert => "One or more checks raised alerts",
			DataValidationNavRunState.Succeeded => "All checks completed successfully",
			DataValidationNavRunState.Running => "A check is currently running",
			_ => "No checks have been run for the selected PNL date"
		};
	}

	private bool IsCurrentRoute(string route)
	{
		var relativePath = NavigationManager.ToBaseRelativePath(NavigationManager.Uri);
		var pathOnly = relativePath.Split('?', 2)[0].TrimEnd('/');
		return string.Equals(pathOnly, route, StringComparison.OrdinalIgnoreCase);
	}
}
