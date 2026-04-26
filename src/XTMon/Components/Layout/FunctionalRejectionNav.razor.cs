using System.Globalization;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Models;
using XTMon.Options;
using XTMon.Services;

namespace XTMon.Components.Layout;

public partial class FunctionalRejectionNav : ComponentBase, IDisposable
{
    private readonly List<FunctionalRejectionMenuItem> menuItems = [];
    private readonly CancellationTokenSource disposeCts = new();
    private PeriodicTimer? alertsPollTimer;
    private CancellationTokenSource? alertsPollCts;
    private bool isDisposed;

    [Inject]
    private FunctionalRejectionMenuState FunctionalRejectionMenuState { get; set; } = default!;

    [Inject]
    private NavigationManager NavigationManager { get; set; } = default!;

    [Inject]
    private FunctionalRejectionNavAlertState FunctionalRejectionNavAlertState { get; set; } = default!;

    [Inject]
    private PnlDateState PnlDateState { get; set; } = default!;

    [Inject]
    private IOptions<MonitoringJobsOptions> MonitoringJobsOptions { get; set; } = default!;

    [Parameter]
    public bool IsOpen { get; set; }

    private bool isLoading;
    private string? loadError;
    private string? loadWarning;

    protected override async Task OnInitializedAsync()
    {
        NavigationManager.LocationChanged += OnLocationChanged;
        FunctionalRejectionNavAlertState.StatusesChanged += OnStatusesChanged;
        PnlDateState.OnDateChanged += OnPnlDateChanged;

        await LoadMenuItemsAsync();
        await RefreshAlertsAsync();

        if (isDisposed)
        {
            return;
        }

        RestartAlertsPollingIfNeeded();
    }

    public void Dispose()
    {
        if (isDisposed)
        {
            return;
        }

        isDisposed = true;
        NavigationManager.LocationChanged -= OnLocationChanged;
        FunctionalRejectionNavAlertState.StatusesChanged -= OnStatusesChanged;
        PnlDateState.OnDateChanged -= OnPnlDateChanged;
        StopAlertsPolling();
        disposeCts.Cancel();
        disposeCts.Dispose();
    }

    private async Task LoadMenuItemsAsync()
    {
        if (!TryGetDisposeToken(out var cancellationToken))
        {
            return;
        }

        isLoading = true;
        loadError = null;
        loadWarning = null;

        try
        {
            await FunctionalRejectionMenuState.RefreshAsync(cancellationToken);

            if (isDisposed)
            {
                return;
            }

            menuItems.Clear();
            menuItems.AddRange(FunctionalRejectionMenuState.MenuItems);
            loadError = FunctionalRejectionMenuState.ErrorMessage;
            loadWarning = FunctionalRejectionMenuState.WarningMessage;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return;
        }
        catch (ObjectDisposedException) when (isDisposed || cancellationToken.IsCancellationRequested)
        {
            return;
        }
        finally
        {
            if (!isDisposed)
            {
                isLoading = false;
            }
        }
    }

    private async Task RefreshAlertsAsync()
    {
        if (!TryGetDisposeToken(out var cancellationToken))
        {
            return;
        }

        try
        {
            await FunctionalRejectionNavAlertState.RefreshAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (ObjectDisposedException) when (isDisposed || cancellationToken.IsCancellationRequested)
        {
        }
    }

    private void OnLocationChanged(object? sender, Microsoft.AspNetCore.Components.Routing.LocationChangedEventArgs e)
    {
        if (isDisposed)
        {
            return;
        }

        _ = InvokeAsync(async () =>
        {
            if (isDisposed)
            {
                return;
            }

            await RefreshAlertsAsync();
            RestartAlertsPollingIfNeeded();

            if (!isDisposed)
            {
                StateHasChanged();
            }
        });
    }

    private void OnStatusesChanged()
    {
        _ = InvokeAsync(StateHasChanged);
    }

    private void OnPnlDateChanged()
    {
        if (isDisposed)
        {
            return;
        }

        _ = InvokeAsync(async () =>
        {
            if (isDisposed)
            {
                return;
            }

            await RefreshAlertsAsync();
            RestartAlertsPollingIfNeeded();

            if (!isDisposed)
            {
                StateHasChanged();
            }
        });
    }

    private void RestartAlertsPollingIfNeeded()
    {
        StopAlertsPolling();

        if (!IsOpen || !TryGetDisposeToken(out var disposeToken))
        {
            return;
        }

        var pollIntervalSeconds = Math.Max(1, MonitoringJobsOptions.Value.NavAlertPollIntervalSeconds);

        try
        {
            alertsPollCts = CancellationTokenSource.CreateLinkedTokenSource(disposeToken);
        }
        catch (ObjectDisposedException) when (isDisposed)
        {
            return;
        }

        alertsPollTimer = new PeriodicTimer(TimeSpan.FromSeconds(pollIntervalSeconds));
        _ = PollAlertsAsync(alertsPollTimer, alertsPollCts.Token);
    }

    private async Task PollAlertsAsync(PeriodicTimer timer, CancellationToken cancellationToken)
    {
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                await RefreshAlertsAsync();
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
            alertsPollCts?.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }

        alertsPollCts?.Dispose();
        alertsPollCts = null;

        alertsPollTimer?.Dispose();
        alertsPollTimer = null;
    }

    private bool TryGetDisposeToken(out CancellationToken cancellationToken)
    {
        if (isDisposed)
        {
            cancellationToken = default;
            return false;
        }

        try
        {
            cancellationToken = disposeCts.Token;
            return true;
        }
        catch (ObjectDisposedException)
        {
            cancellationToken = default;
            return false;
        }
    }

    private DataValidationNavRunState GetItemRunState(FunctionalRejectionMenuItem item)
    {
        return FunctionalRejectionNavAlertState.GetStatus(item);
    }

    private DataValidationNavRunState GetAggregateStatus()
    {
        return FunctionalRejectionNavAlertState.GetAggregateStatus(menuItems);
    }

    private static string GetAggregateIndicatorClass(DataValidationNavRunState status)
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

    private static string GetAggregateIndicatorDescription(DataValidationNavRunState status)
    {
        return status switch
        {
            DataValidationNavRunState.Failed => "One or more Functional Rejection checks failed or raised alerts",
            DataValidationNavRunState.Alert => "One or more Functional Rejection checks raised alerts",
            DataValidationNavRunState.Succeeded => "All Functional Rejection checks completed successfully",
            DataValidationNavRunState.Running => "A Functional Rejection check is currently running",
            _ => "No Functional Rejection checks have been run for the selected PNL date"
        };
    }

    private string GetItemHref(FunctionalRejectionMenuItem item)
    {
        var uri = NavigationManager.ToAbsoluteUri(NavigationManager.Uri);
        var query = QueryHelpers.ParseQuery(uri.Query);
        query.TryGetValue("pnlDate", out var pnlDate);

        return FunctionalRejectionUrlHelper.BuildHref(item, pnlDate.ToString());
    }

    private string GetSubmenuLinkClass(FunctionalRejectionMenuItem item)
    {
        return IsActive(item)
            ? "submenu-link submenu-link-active"
            : "submenu-link";
    }

    private bool IsActive(FunctionalRejectionMenuItem item)
    {
        var relativePath = NavigationManager.ToBaseRelativePath(NavigationManager.Uri);
        var route = relativePath.Split('?', 2)[0].TrimEnd('/');

        if (!string.Equals(route, "functional-rejection", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var uri = NavigationManager.ToAbsoluteUri(NavigationManager.Uri);
        var query = QueryHelpers.ParseQuery(uri.Query);
        if (!query.TryGetValue("code", out var code) ||
            !query.TryGetValue("businessDatatypeId", out var businessDatatypeId) ||
            !query.TryGetValue("sourceSystemName", out var sourceSystemName) ||
            !query.TryGetValue("dbConnection", out var dbConnection))
        {
            return false;
        }

        return string.Equals(code.ToString(), item.SourceSystemBusinessDataTypeCode, StringComparison.OrdinalIgnoreCase)
            && string.Equals(sourceSystemName.ToString(), item.SourceSystemName, StringComparison.OrdinalIgnoreCase)
            && string.Equals(dbConnection.ToString(), item.DbConnection, StringComparison.OrdinalIgnoreCase)
            && string.Equals(businessDatatypeId.ToString(), item.BusinessDataTypeId.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal);
    }
}
