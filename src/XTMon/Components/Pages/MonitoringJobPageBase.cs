using System.Globalization;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;
using XTMon.Services;

namespace XTMon.Components.Pages;

public abstract class MonitoringJobPageBase<TPage> : ComponentBase, IAsyncDisposable
{
    protected const string DisplayDateFormat = "dd-MM-yyyy";
    protected const string DisplayDateTimeFormat = "dd-MM-yyyy HH:mm:ss";

    private sealed class PollSession
    {
        public required CancellationTokenSource CancellationSource { get; init; }

        public required PeriodicTimer Timer { get; init; }

        public Task? Task { get; set; }
    }

    private string? loadErrorMessage;
    private string? runErrorMessage;
    private PollSession? pollSession;

    [Inject]
    protected IMonitoringJobRepository MonitoringJobRepository { get; set; } = default!;

    [Inject]
    protected IJvCalculationRepository PnlDateRepository { get; set; } = default!;

    [Inject]
    protected PnlDateState PnlDateState { get; set; } = default!;

    [Inject]
    protected IOptions<MonitoringJobsOptions> MonitoringJobsOptions { get; set; } = default!;

    [Inject]
    protected ILogger<TPage> Logger { get; set; } = default!;

    protected readonly HashSet<DateOnly> availableDates = [];
    protected readonly CancellationTokenSource disposeCts = new();
    protected DateOnly? selectedPnlDate;
    protected bool isLoading;
    protected bool hasRun;
    protected string? validationError;
    protected DateTime? lastRunAt;
    protected long? activeJobId;
    protected string? activeJobStatus;

    protected string? loadError
    {
        get => loadErrorMessage;
        set => loadErrorMessage = value;
    }

    protected string? runError
    {
        get => runErrorMessage;
        set => runErrorMessage = value;
    }

    protected string SelectedPnlDateText => selectedPnlDate.HasValue
        ? selectedPnlDate.Value.ToString(DisplayDateFormat, CultureInfo.InvariantCulture)
        : "-";

    protected string LastRunText => lastRunAt.HasValue
        ? lastRunAt.Value.ToString(DisplayDateTimeFormat, CultureInfo.InvariantCulture)
        : "-";

    protected bool IsJobActive => MonitoringJobHelper.IsActiveStatus(activeJobStatus);

    protected virtual string MonitoringCategory => MonitoringJobHelper.DataValidationCategory;

    protected virtual string MissingPnlDateValidationMessage => "PNL DATE is required.";

    protected virtual bool ShouldRestoreAfterPnlDateSelected => true;

    protected virtual bool ShouldRestoreOnGlobalPnlDateChanged => true;

    protected abstract string MonitoringSubmenuKey { get; }

    protected abstract string MonitoringJobName { get; }

    protected abstract string DefaultLoadErrorMessage { get; }

    protected sealed override async Task OnInitializedAsync()
    {
        await LoadPnlDatesAsync();
        PnlDateState.OnDateChanged += OnGlobalPnlDateChanged;
        await OnInitializedCoreAsync();
        await RestoreLatestJobAsync();
    }

    protected virtual Task OnInitializedCoreAsync() => Task.CompletedTask;

    protected virtual Task OnPnlDateSelectedCoreAsync() => Task.CompletedTask;

    protected virtual DateOnly? GetCurrentPnlDateFromState() => PnlDateState.SelectedDate;

    protected virtual bool CanRestoreLatestJob() => true;

    protected virtual Task<bool> ValidateBeforeRunAsync() => Task.FromResult(true);

    protected virtual bool TryPrepareRun(out string? parametersJson, out string? parameterSummary)
    {
        parametersJson = null;
        parameterSummary = null;
        return true;
    }

    protected virtual void OnBeforeRun()
    {
    }

    protected virtual void OnRunFailed()
    {
    }

    protected virtual Task<bool> TryHandleRunExceptionAsync(Exception exception) => Task.FromResult(false);

    protected virtual void LogRunFailure(Exception exception)
    {
        Logger.LogError(
            AppLogEvents.MonitoringLoadFailed,
            exception,
            "Failed to load {MonitoringJobName} for PnlDate {PnlDate}.",
            MonitoringJobName,
            selectedPnlDate);
    }

    protected virtual void DisposeCore()
    {
    }

    protected virtual bool TryHandleRestoreException(Exception exception) => false;

    protected abstract void ApplyJobCore(MonitoringJobRecord job);

    protected abstract void ClearLoadedStateCore();

    protected abstract bool HasLoadedResult();

    public async ValueTask DisposeAsync()
    {
        PnlDateState.OnDateChanged -= OnGlobalPnlDateChanged;
        disposeCts.Cancel();
        await StopPollingAsync();
        DisposeCore();
        disposeCts.Dispose();
    }

    protected async Task OnPnlDateSelected(DateOnly date)
    {
        selectedPnlDate = date;
        validationError = null;
        ClearBackgroundJobError();
        await OnPnlDateSelectedCoreAsync();

        if (ShouldRestoreAfterPnlDateSelected)
        {
            await RestoreLatestJobAsync();
        }
    }

    protected async Task RunAsync()
    {
        if (!selectedPnlDate.HasValue)
        {
            validationError = MissingPnlDateValidationMessage;
            return;
        }

        if (!TryPrepareRun(out var parametersJson, out var parameterSummary))
        {
            return;
        }

        if (!await ValidateBeforeRunAsync())
        {
            return;
        }

        isLoading = true;
        hasRun = true;
        validationError = null;
        ClearBackgroundJobError();
        OnBeforeRun();

        try
        {
            var enqueueResult = await MonitoringJobRepository.EnqueueMonitoringJobAsync(
                MonitoringCategory,
                MonitoringSubmenuKey,
                MonitoringJobName,
                selectedPnlDate.Value,
                parametersJson,
                parameterSummary,
                disposeCts.Token);

            activeJobId = enqueueResult.JobId;
            await RefreshActiveJobAsync(disposeCts.Token);
            await StartPollingIfNeededAsync();
        }
        catch (Exception ex)
        {
            if (!await TryHandleRunExceptionAsync(ex))
            {
                LogRunFailure(ex);
                SetBackgroundJobError(DefaultLoadErrorMessage);
                OnRunFailed();
            }
        }
        finally
        {
            isLoading = false;
        }
    }

    protected async Task RestoreLatestJobAsync()
    {
        await StopPollingAsync();

        if (!selectedPnlDate.HasValue)
        {
            ClearLoadedState();
            return;
        }

        if (!CanRestoreLatestJob())
        {
            ClearLoadedState();
            return;
        }

        try
        {
            var latestJob = await MonitoringJobRepository.GetLatestMonitoringJobAsync(
                MonitoringCategory,
                MonitoringSubmenuKey,
                selectedPnlDate.Value,
                disposeCts.Token);

            if (latestJob is null)
            {
                ClearLoadedState();
                await StartPollingIfNeededAsync();
                return;
            }

            ApplyJob(latestJob);
            await StartPollingIfNeededAsync();
        }
        catch (Exception ex)
        {
            if (!TryHandleRestoreException(ex))
            {
                Logger.LogWarning(ex, "Unable to restore latest {MonitoringJobName} job for PnlDate {PnlDate}.", MonitoringJobName, selectedPnlDate.Value);
            }
        }
    }

    private async Task LoadPnlDatesAsync()
    {
        try
        {
            await PnlDateState.EnsureLoadedAsync(PnlDateRepository, disposeCts.Token);
            selectedPnlDate = GetCurrentPnlDateFromState();

            availableDates.Clear();
            foreach (var date in PnlDateState.AvailableDates)
            {
                availableDates.Add(date);
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Unable to load default PNL dates.");
        }
    }

    private void OnGlobalPnlDateChanged()
    {
        _ = InvokeAsync(async () =>
        {
            selectedPnlDate = GetCurrentPnlDateFromState();

            if (ShouldRestoreOnGlobalPnlDateChanged)
            {
                await RestoreLatestJobAsync();
            }

            StateHasChanged();
        });
    }

    private void ApplyJob(MonitoringJobRecord job)
    {
        activeJobId = job.JobId;
        activeJobStatus = job.Status;
        ApplyJobCore(job);
        hasRun = true;

        var latestExecution = job.CompletedAt ?? job.StartedAt ?? job.EnqueuedAt;
        lastRunAt = JvCalculationHelper.ToUtc(latestExecution).ToLocalTime();

        if (string.Equals(job.Status, "Failed", StringComparison.OrdinalIgnoreCase) && !HasLoadedResult())
        {
            SetBackgroundJobError(MonitoringDisplayHelper.GetSafeBackgroundJobMessage(job.ErrorMessage, DefaultLoadErrorMessage));
        }
        else
        {
            ClearBackgroundJobError();
        }
    }

    protected void ClearLoadedState()
    {
        activeJobId = null;
        activeJobStatus = null;
        hasRun = false;
        lastRunAt = null;
        ClearBackgroundJobError();
        ClearLoadedStateCore();
    }

    private void ClearBackgroundJobError()
    {
        SetBackgroundJobError(null);
    }

    private void SetBackgroundJobError(string? errorMessage)
    {
        loadError = errorMessage;
        runError = errorMessage;
    }

    protected virtual Task RequestStateHasChangedAsync() => InvokeAsync(StateHasChanged);

    protected async Task StopPollingAsync()
    {
        var session = pollSession;
        if (session is null)
        {
            return;
        }

        pollSession = null;
        session.CancellationSource.Cancel();

        if (session.Task is null)
        {
            return;
        }

        try
        {
            await session.Task;
        }
        catch (OperationCanceledException)
        {
        }
    }

    protected async Task StartPollingIfNeededAsync()
    {
        await StopPollingAsync();

        if (!CanPollForLatestJobDiscovery() && (!activeJobId.HasValue || !IsJobActive))
        {
            return;
        }

        var session = new PollSession
        {
            CancellationSource = CancellationTokenSource.CreateLinkedTokenSource(disposeCts.Token),
            Timer = new PeriodicTimer(TimeSpan.FromSeconds(MonitoringJobsOptions.Value.JobPollIntervalSeconds))
        };

        session.Task = PollJobAsync(session);
        pollSession = session;
    }

    private async Task PollJobAsync(PollSession session)
    {
        try
        {
            while (await session.Timer.WaitForNextTickAsync(session.CancellationSource.Token))
            {
                var shouldContinuePolling = await RefreshPollingStateAsync(session.CancellationSource.Token);
                await RequestStateHasChangedAsync();
                if (!shouldContinuePolling)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            if (ReferenceEquals(pollSession, session))
            {
                pollSession = null;
            }

            session.Timer.Dispose();
            session.CancellationSource.Dispose();
        }
    }

    private async Task<bool> RefreshActiveJobAsync(CancellationToken cancellationToken)
    {
        if (!activeJobId.HasValue)
        {
            return false;
        }

        var job = await MonitoringJobRepository.GetMonitoringJobByIdAsync(activeJobId.Value, cancellationToken);
        if (job is null)
        {
            return false;
        }

        ApplyJob(job);
        return IsJobActive;
    }

    private bool CanPollForLatestJobDiscovery()
    {
        return selectedPnlDate.HasValue && CanRestoreLatestJob();
    }

    private async Task<bool> RefreshPollingStateAsync(CancellationToken cancellationToken)
    {
        if (activeJobId.HasValue)
        {
            var shouldContinueActivePolling = await RefreshActiveJobAsync(cancellationToken);
            if (shouldContinueActivePolling)
            {
                return true;
            }
        }

        if (!CanPollForLatestJobDiscovery())
        {
            return false;
        }

        var latestJob = await MonitoringJobRepository.GetLatestMonitoringJobAsync(
            MonitoringCategory,
            MonitoringSubmenuKey,
            selectedPnlDate!.Value,
            cancellationToken);

        if (latestJob is null)
        {
            ClearLoadedState();
            return true;
        }

        ApplyJob(latestJob);
        return true;
    }
}
