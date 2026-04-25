using System.Globalization;
using Microsoft.AspNetCore.Components;
using XTMon.Services;

namespace XTMon.Components.Pages;

/// <summary>
/// Shared PNL-date lifecycle for pages that do not participate in the monitoring-job restore/polling model.
/// This intentionally remains separate from <see cref="MonitoringJobPageBase{TPage}"/> so non-monitoring flows
/// do not inherit monitoring-job repositories, job state, or polling behavior they do not use.
/// </summary>
public abstract class PnlDateAwarePageBase<TPage> : ComponentBase, IDisposable
{
    protected const string DisplayDateFormat = "dd-MM-yyyy";
    protected const string DisplayDateTimeFormat = "dd-MM-yyyy HH:mm:ss";

    [Inject]
    protected PnlDateState PnlDateState { get; set; } = default!;

    [Inject]
    protected ILogger<TPage> Logger { get; set; } = default!;

    protected readonly HashSet<DateOnly> availableDates = [];
    protected readonly CancellationTokenSource disposeCts = new();
    protected DateOnly? selectedPnlDate;

    protected string SelectedPnlDateText => selectedPnlDate.HasValue
        ? selectedPnlDate.Value.ToString(DisplayDateFormat, CultureInfo.InvariantCulture)
        : "-";

    protected sealed override async Task OnInitializedAsync()
    {
        await ReloadPnlDatesAsync();
        PnlDateState.OnDateChanged += OnGlobalPnlDateChanged;
        await OnInitializedCoreAsync();
    }

    protected abstract Task EnsurePnlDatesLoadedAsync(CancellationToken cancellationToken);

    protected virtual Task OnInitializedCoreAsync() => Task.CompletedTask;

    protected virtual Task OnGlobalPnlDateChangedCoreAsync() => Task.CompletedTask;

    protected virtual void HandlePnlDateLoadException(Exception exception)
    {
        Logger.LogWarning(exception, "Unable to load default PNL dates.");
    }

    protected virtual void DisposeCore()
    {
    }

    protected Task SetGlobalPnlDateAsync(DateOnly date)
    {
        PnlDateState.SetDate(date);
        return Task.CompletedTask;
    }

    protected async Task ReloadPnlDatesAsync()
    {
        try
        {
            await EnsurePnlDatesLoadedAsync(disposeCts.Token);
            SyncPnlDateState();
        }
        catch (OperationCanceledException) when (disposeCts.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            HandlePnlDateLoadException(ex);
        }
    }

    public void Dispose()
    {
        PnlDateState.OnDateChanged -= OnGlobalPnlDateChanged;
        DisposeCore();
        disposeCts.Cancel();
        disposeCts.Dispose();
    }

    private void OnGlobalPnlDateChanged()
    {
        _ = InvokeAsync(async () =>
        {
            SyncPnlDateState();
            await OnGlobalPnlDateChangedCoreAsync();
            StateHasChanged();
        });
    }

    private void SyncPnlDateState()
    {
        selectedPnlDate = PnlDateState.SelectedDate;

        availableDates.Clear();
        foreach (var date in PnlDateState.AvailableDates)
        {
            availableDates.Add(date);
        }
    }
}