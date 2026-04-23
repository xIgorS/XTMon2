using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Options;
using XTMon.Services;

namespace XTMon.Components.Layout;

public partial class NavMenu : ComponentBase, IDisposable
{
	private readonly CancellationTokenSource _disposeCts = new();
	private PeriodicTimer? _alertsPollTimer;
	private CancellationTokenSource? _alertsPollCts;

	[Inject]
	private NavigationManager NavigationManager { get; set; } = default!;

	[Inject]
	private PnlDateState PnlDateState { get; set; } = default!;

	[Inject]
	private DataValidationNavAlertState DataValidationNavAlertState { get; set; } = default!;

	[Inject]
	private JvCalculationNavAlertState JvCalculationNavAlertState { get; set; } = default!;

	[Inject]
	private ReplayFlowsNavAlertState ReplayFlowsNavAlertState { get; set; } = default!;

	[Inject]
	private IOptions<MonitoringJobsOptions> MonitoringJobsOptions { get; set; } = default!;

	protected override void OnInitialized()
	{
		NavigationManager.LocationChanged += OnLocationChanged;
		PnlDateState.OnDateChanged += OnPnlDateChanged;
		DataValidationNavAlertState.StatusesChanged += OnDataValidationStatusesChanged;
		JvCalculationNavAlertState.StatusChanged += OnJvCalculationStatusChanged;
		ReplayFlowsNavAlertState.StatusChanged += OnReplayFlowsStatusChanged;
	}

	protected override async Task OnInitializedAsync()
	{
		await RefreshDataValidationAlertsAsync();
		RestartAlertsPollingIfNeeded();
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
		JvCalculationNavAlertState.StatusChanged -= OnJvCalculationStatusChanged;
		ReplayFlowsNavAlertState.StatusChanged -= OnReplayFlowsStatusChanged;
		StopAlertsPolling();
		_disposeCts.Cancel();
		_disposeCts.Dispose();
	}

	private void OnLocationChanged(object? sender, Microsoft.AspNetCore.Components.Routing.LocationChangedEventArgs e)
	{
		_ = InvokeAsync(async () =>
		{
			await RefreshDataValidationAlertsAsync();
			RestartAlertsPollingIfNeeded();
			StateHasChanged();
		});
	}

	private void OnPnlDateChanged()
	{
		_ = InvokeAsync(async () =>
		{
			await RefreshDataValidationAlertsAsync();
			RestartAlertsPollingIfNeeded();
			StateHasChanged();
		});
	}

	private void OnDataValidationStatusesChanged()
	{
		_ = InvokeAsync(StateHasChanged);
	}

	private void OnJvCalculationStatusChanged()
	{
		_ = InvokeAsync(StateHasChanged);
	}

	private void OnReplayFlowsStatusChanged()
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

	private void RestartAlertsPollingIfNeeded()
	{
		StopAlertsPolling();

		if (!IsDataValidationRoute)
		{
			return;
		}

		var pollIntervalSeconds = Math.Max(1, MonitoringJobsOptions.Value.JobPollIntervalSeconds);
		_alertsPollCts = CancellationTokenSource.CreateLinkedTokenSource(_disposeCts.Token);
		_alertsPollTimer = new PeriodicTimer(TimeSpan.FromSeconds(pollIntervalSeconds));
		_ = PollAlertsAsync(_alertsPollTimer, _alertsPollCts.Token);
	}

	private async Task PollAlertsAsync(PeriodicTimer timer, CancellationToken cancellationToken)
	{
		try
		{
			while (await timer.WaitForNextTickAsync(cancellationToken))
			{
				await RefreshDataValidationAlertsAsync();
				await InvokeAsync(StateHasChanged);
			}
		}
		catch (OperationCanceledException)
		{
		}
		catch (ObjectDisposedException)
		{
		}
	}

	private void StopAlertsPolling()
	{
		_alertsPollCts?.Cancel();
		_alertsPollCts?.Dispose();
		_alertsPollCts = null;

		_alertsPollTimer?.Dispose();
		_alertsPollTimer = null;
	}

	private DataValidationNavRunState GetDataValidationRunState(string route)
	{
		return DataValidationNavAlertState.GetStatus(route);
	}

	private DataValidationNavRunState GetJvCalculationRunState()
	{
		return JvCalculationNavAlertState.GetStatus();
	}

	private DataValidationNavRunState GetReplayFlowsRunState()
	{
		return ReplayFlowsNavAlertState.GetStatus();
	}

	private string GetDataValidationRunnerLinkClass()
	{
		return IsCurrentRoute(DataValidationCheckCatalog.BatchRunRoute)
			? "submenu-link submenu-link-active"
			: "submenu-link";
	}

	private string GetDataValidationSubmenuLinkClass(string route)
	{
		return IsDataValidationSubmenuActive(route)
			? "submenu-link submenu-link-active"
			: "submenu-link";
	}

	private bool IsDataValidationSubmenuActive(string route)
	{
		var normalizedRoute = MonitoringJobHelper.BuildDataValidationSubmenuKey(route);

		if (IsCurrentRoute(normalizedRoute))
		{
			return true;
		}

		return IsCurrentRoute(DataValidationCheckCatalog.BatchRunRoute)
			&& GetDataValidationRunState(normalizedRoute) == DataValidationNavRunState.Running;
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
