using Microsoft.Data.SqlClient;
using XTMon.Helpers;
using XTMon.Models;
using XTMon.Repositories;

namespace XTMon.Services;

public sealed class DataValidationNavAlertState
{
    private static readonly TimeSpan SqlFailureBackoff = TimeSpan.FromSeconds(10);

    private readonly PnlDateState _pnlDateState;
    private readonly IJvCalculationRepository _pnlDateRepository;
    private readonly IMonitoringJobRepository _monitoringJobRepository;
    private readonly ILogger<DataValidationNavAlertState> _logger;
    private readonly Dictionary<string, DataValidationNavRunState> _statuses = new(StringComparer.OrdinalIgnoreCase);
    private DateTimeOffset? _nextRefreshAllowedAtUtc;

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

    public DataValidationNavRunState GetAggregateStatus()
    {
        return NavRunStateAggregator.Aggregate(_statuses.Values);
    }

    public async Task RefreshAsync(CancellationToken cancellationToken)
    {
        if (_nextRefreshAllowedAtUtc.HasValue && DateTimeOffset.UtcNow < _nextRefreshAllowedAtUtc.Value)
        {
            return;
        }

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

            _nextRefreshAllowedAtUtc = null;
            ApplyStatuses(_pnlDateState.SelectedDate.Value, jobs);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (SqlException ex) when (SqlDataHelper.IsSqlTimeout(ex) || SqlDataHelper.IsSqlConnectionFailure(ex) || SqlDataHelper.IsSqlDeadlock(ex))
        {
            _nextRefreshAllowedAtUtc = DateTimeOffset.UtcNow.Add(SqlFailureBackoff);
            _logger.LogWarning(ex,
            "Unable to refresh data-validation nav statuses due to a SQL timeout/connection/deadlock problem. Preserving current statuses and backing off until {RetryAtUtc:O}.",
                _nextRefreshAllowedAtUtc.Value);
        }
        catch (Exception ex)
        {
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