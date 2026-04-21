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

	private bool IsFunctionalRejectionRoute => IsCurrentRoute("functional-rejection");

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

	private bool IsCurrentRoute(string route)
	{
		var relativePath = NavigationManager.ToBaseRelativePath(NavigationManager.Uri);
		var pathOnly = relativePath.Split('?', 2)[0].TrimEnd('/');
		return string.Equals(pathOnly, route, StringComparison.OrdinalIgnoreCase);
	}
}
