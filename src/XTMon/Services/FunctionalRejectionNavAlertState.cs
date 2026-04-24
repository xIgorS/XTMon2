using Microsoft.Data.SqlClient;
using XTMon.Helpers;
using XTMon.Models;
using XTMon.Repositories;

namespace XTMon.Services;

public sealed class FunctionalRejectionNavAlertState
{
    private static readonly TimeSpan SqlFailureBackoff = TimeSpan.FromSeconds(10);

    private readonly PnlDateState _pnlDateState;
    private readonly IJvCalculationRepository _pnlDateRepository;
    private readonly IMonitoringJobRepository _monitoringJobRepository;
    private readonly ILogger<FunctionalRejectionNavAlertState> _logger;
    private readonly Dictionary<string, DataValidationNavRunState> _statuses = new(StringComparer.OrdinalIgnoreCase);
    private DateTimeOffset? _nextRefreshAllowedAtUtc;

    public event Action? StatusesChanged;

    public FunctionalRejectionNavAlertState(
        PnlDateState pnlDateState,
        IJvCalculationRepository pnlDateRepository,
        IMonitoringJobRepository monitoringJobRepository,
        ILogger<FunctionalRejectionNavAlertState> logger)
    {
        _pnlDateState = pnlDateState;
        _pnlDateRepository = pnlDateRepository;
        _monitoringJobRepository = monitoringJobRepository;
        _logger = logger;
    }

    public DataValidationNavRunState GetStatus(FunctionalRejectionMenuItem item)
    {
        var key = MonitoringJobHelper.BuildFunctionalRejectionSubmenuKey(
            item.BusinessDataTypeId,
            item.SourceSystemName,
            item.DbConnection,
            item.SourceSystemBusinessDataTypeCode);
        return GetStatus(key);
    }

    public DataValidationNavRunState GetStatus(string submenuKey)
    {
        return _statuses.TryGetValue(submenuKey, out var status)
            ? status
            : DataValidationNavRunState.NotRun;
    }

    public DataValidationNavRunState GetAggregateStatus(IEnumerable<FunctionalRejectionMenuItem> menuItems)
    {
        return NavRunStateAggregator.Aggregate(menuItems.Select(GetStatus));
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
                MonitoringJobHelper.FunctionalRejectionCategory,
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
            "Unable to refresh Functional Rejection nav statuses due to a SQL timeout/connection/deadlock problem. Preserving current statuses and backing off until {RetryAtUtc:O}.",
                _nextRefreshAllowedAtUtc.Value);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Unable to refresh Functional Rejection nav statuses.");
        }
    }

    public void ApplyStatuses(DateOnly? pnlDate, IReadOnlyCollection<MonitoringJobRecord> jobs)
    {
        _statuses.Clear();

        if (!pnlDate.HasValue)
        {
            NotifyStatusesChanged();
            return;
        }

        foreach (var job in jobs)
        {
            if (!string.Equals(job.Category, MonitoringJobHelper.FunctionalRejectionCategory, StringComparison.OrdinalIgnoreCase)
                || job.PnlDate != pnlDate.Value)
            {
                continue;
            }

            _statuses[job.SubmenuKey] = FunctionalRejectionNavAlertHelper.GetRunState(job);
        }

        NotifyStatusesChanged();
    }

    private void NotifyStatusesChanged()
    {
        StatusesChanged?.Invoke();
    }
}
