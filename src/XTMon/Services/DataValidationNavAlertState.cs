using XTMon.Helpers;
using XTMon.Models;
using XTMon.Repositories;

namespace XTMon.Services;

public sealed class DataValidationNavAlertState
{
    private readonly PnlDateState _pnlDateState;
    private readonly IJvCalculationRepository _pnlDateRepository;
    private readonly IMonitoringJobRepository _monitoringJobRepository;
    private readonly ILogger<DataValidationNavAlertState> _logger;
    private readonly Dictionary<string, DataValidationNavRunState> _statuses = new(StringComparer.OrdinalIgnoreCase);

    public event Action? StatusesChanged;

    public DataValidationNavAlertState(
        PnlDateState pnlDateState,
        IJvCalculationRepository pnlDateRepository,
        IMonitoringJobRepository monitoringJobRepository,
        ILogger<DataValidationNavAlertState> logger)
    {
        _pnlDateState = pnlDateState;
        _pnlDateRepository = pnlDateRepository;
        _monitoringJobRepository = monitoringJobRepository;
        _logger = logger;
        ResetStatuses();
    }

    public DataValidationNavRunState GetStatus(string route)
    {
        var normalizedRoute = MonitoringJobHelper.BuildDataValidationSubmenuKey(route);
        return _statuses.TryGetValue(normalizedRoute, out var status)
            ? status
            : DataValidationNavRunState.NotRun;
    }

    public async Task RefreshAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _pnlDateState.EnsureLoadedAsync(_pnlDateRepository, cancellationToken);

            if (!_pnlDateState.SelectedDate.HasValue)
            {
                ApplyStatuses(null, Array.Empty<MonitoringJobRecord>());
                return;
            }

            var jobs = await _monitoringJobRepository.GetLatestMonitoringJobsByCategoryAsync(
                MonitoringJobHelper.DataValidationCategory,
                _pnlDateState.SelectedDate.Value,
                cancellationToken);

            ApplyStatuses(_pnlDateState.SelectedDate.Value, jobs);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            ResetStatuses();
            NotifyStatusesChanged();
            _logger.LogWarning(ex, "Unable to refresh data-validation nav statuses.");
        }
    }

    public void ApplyStatuses(DateOnly? pnlDate, IReadOnlyCollection<MonitoringJobRecord> jobs)
    {
        ResetStatuses();

        if (!pnlDate.HasValue)
        {
            NotifyStatusesChanged();
            return;
        }

        foreach (var job in jobs)
        {
            if (!string.Equals(job.Category, MonitoringJobHelper.DataValidationCategory, StringComparison.OrdinalIgnoreCase)
                || job.PnlDate != pnlDate.Value)
            {
                continue;
            }

            var route = MonitoringJobHelper.BuildDataValidationSubmenuKey(job.SubmenuKey);
            if (!_statuses.ContainsKey(route))
            {
                continue;
            }

            _statuses[route] = DataValidationNavAlertHelper.GetRunState(job);
        }

        NotifyStatusesChanged();
    }

    private void ResetStatuses()
    {
        _statuses.Clear();

        foreach (var route in DataValidationNavAlertHelper.SupportedRoutes)
        {
            _statuses[route] = DataValidationNavRunState.NotRun;
        }
    }

    private void NotifyStatusesChanged()
    {
        StatusesChanged?.Invoke();
    }
}