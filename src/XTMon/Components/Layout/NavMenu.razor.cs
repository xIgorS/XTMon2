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
	private bool _isDisposed;

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
	private DatabaseSpaceNavAlertState DatabaseSpaceNavAlertState { get; set; } = default!;

	[Inject]
	private IOptions<MonitoringJobsOptions> MonitoringJobsOptions { get; set; } = default!;

	[Inject]
	private ILogger<NavMenu> Logger { get; set; } = default!;

	protected override void OnInitialized()
	{
		NavigationManager.LocationChanged += OnLocationChanged;
		PnlDateState.OnDateChanged += OnPnlDateChanged;
		DataValidationNavAlertState.StatusesChanged += OnDataValidationStatusesChanged;
		JvCalculationNavAlertState.StatusChanged += OnJvCalculationStatusChanged;
		ReplayFlowsNavAlertState.StatusChanged += OnReplayFlowsStatusChanged;
		DatabaseSpaceNavAlertState.StatusChanged += OnDatabaseSpaceStatusChanged;
	}

	protected override async Task OnInitializedAsync()
	{
		await RefreshAllNavAlertsAsync();

		if (_isDisposed)
		{
			return;
		}

		StartAlertsPolling();
	}

	private bool IsDataValidationRoute =>
		IsCurrentRoute(DataValidationCheckCatalog.BatchRunRoute) ||
		DataValidationCheckCatalog.Routes.Any(IsCurrentRoute);

	private bool IsFunctionalRejectionRoute =>
		IsCurrentRoute("functional-rejection") ||
		IsCurrentRoute("functional-rejection-runner");

	public void Dispose()
	{
		if (_isDisposed)
		{
			return;
		}

		_isDisposed = true;
		NavigationManager.LocationChanged -= OnLocationChanged;
		PnlDateState.OnDateChanged -= OnPnlDateChanged;
		DataValidationNavAlertState.StatusesChanged -= OnDataValidationStatusesChanged;
		JvCalculationNavAlertState.StatusChanged -= OnJvCalculationStatusChanged;
		ReplayFlowsNavAlertState.StatusChanged -= OnReplayFlowsStatusChanged;
		DatabaseSpaceNavAlertState.StatusChanged -= OnDatabaseSpaceStatusChanged;
		StopAlertsPolling();
		_disposeCts.Cancel();
		_disposeCts.Dispose();
	}

	private void OnLocationChanged(object? sender, Microsoft.AspNetCore.Components.Routing.LocationChangedEventArgs e)
	{
		if (_isDisposed)
		{
			return;
		}

		_ = InvokeAsync(async () =>
		{
			if (_isDisposed)
			{
				return;
			}

			await RefreshAllNavAlertsAsync();

			if (!_isDisposed)
			{
				StateHasChanged();
			}
		});
	}

	private void OnPnlDateChanged()
	{
		if (_isDisposed)
		{
			return;
		}

		_ = InvokeAsync(async () =>
		{
			if (_isDisposed)
			{
				return;
			}

			await RefreshAllNavAlertsAsync();

			if (!_isDisposed)
			{
				StateHasChanged();
			}
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

	private void OnDatabaseSpaceStatusChanged()
	{
		_ = InvokeAsync(StateHasChanged);
	}

	private async Task RefreshAllNavAlertsAsync()
	{
		if (!TryGetDisposeToken(out var disposeToken))
		{
			return;
		}

		var tasks = new[]
		{
			RefreshNavAlertSafelyAsync("data-validation", DataValidationNavAlertState.RefreshAsync, disposeToken),
			RefreshNavAlertSafelyAsync("jv-calculation",  JvCalculationNavAlertState.RefreshAsync, disposeToken),
			RefreshNavAlertSafelyAsync("replay-flows",    ReplayFlowsNavAlertState.RefreshAsync, disposeToken)
		};

		await Task.WhenAll(tasks);
	}

	private async Task RefreshNavAlertSafelyAsync(string label, Func<CancellationToken, Task> refresh, CancellationToken cancellationToken)
	{
		try
		{
			await refresh(cancellationToken);
		}
		catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
		{
		}
		catch (ObjectDisposedException) when (_isDisposed || cancellationToken.IsCancellationRequested)
		{
		}
		catch (Exception ex)
		{
			if (!_isDisposed)
			{
				Logger.LogWarning(ex, "Unable to refresh {NavAlert} nav alerts.", label);
			}
		}
	}

	private void StartAlertsPolling()
	{
		StopAlertsPolling();

		if (!TryGetDisposeToken(out var disposeToken))
		{
			return;
		}

		var pollIntervalSeconds = Math.Max(1, MonitoringJobsOptions.Value.NavAlertPollIntervalSeconds);

		try
		{
			_alertsPollCts = CancellationTokenSource.CreateLinkedTokenSource(disposeToken);
		}
		catch (ObjectDisposedException) when (_isDisposed)
		{
			return;
		}

		_alertsPollTimer = new PeriodicTimer(TimeSpan.FromSeconds(pollIntervalSeconds));
		_ = PollAlertsAsync(_alertsPollTimer, _alertsPollCts.Token);
	}

	private async Task PollAlertsAsync(PeriodicTimer timer, CancellationToken cancellationToken)
	{
		try
		{
			while (await timer.WaitForNextTickAsync(cancellationToken))
			{
				await RefreshAllNavAlertsAsync();
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
		try
		{
			_alertsPollCts?.Cancel();
		}
		catch (ObjectDisposedException)
		{
		}

		_alertsPollCts?.Dispose();
		_alertsPollCts = null;

		_alertsPollTimer?.Dispose();
		_alertsPollTimer = null;
	}

	private bool TryGetDisposeToken(out CancellationToken cancellationToken)
	{
		if (_isDisposed)
		{
			cancellationToken = default;
			return false;
		}

		try
		{
			cancellationToken = _disposeCts.Token;
			return true;
		}
		catch (ObjectDisposedException)
		{
			cancellationToken = default;
			return false;
		}
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

	private string GetDatabaseSpaceIndicatorClass()
	{
		return DatabaseSpaceNavAlertState.GetStatus() switch
		{
			DataValidationNavRunState.Failed => "submenu-status-indicator submenu-status-badge--failed",
			DataValidationNavRunState.Alert => "submenu-status-indicator submenu-status-badge--warning",
			DataValidationNavRunState.Succeeded => "submenu-status-indicator submenu-status-badge--succeeded",
			_ => "submenu-status-indicator submenu-status-badge--not-run"
		};
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

		return IsCurrentRoute(normalizedRoute);
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
