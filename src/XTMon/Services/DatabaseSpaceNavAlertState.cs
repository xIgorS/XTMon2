using XTMon.Helpers;
using XTMon.Models;

namespace XTMon.Services;

public sealed class DatabaseSpaceNavAlertState
{
    private DataValidationNavRunState _status = DataValidationNavRunState.NotRun;

    public event Action? StatusChanged;

    public DataValidationNavRunState GetStatus() => _status;

    public void ApplyResult(MonitoringTableResult? result)
    {
        _status = ComputeStatus(result);
        StatusChanged?.Invoke();
    }

    private static DataValidationNavRunState ComputeStatus(MonitoringTableResult? result)
    {
        if (result is null || result.Rows.Count == 0)
        {
            return DataValidationNavRunState.NotRun;
        }

        var alertLevelIndex = -1;
        for (var i = 0; i < result.Columns.Count; i++)
        {
            if (string.Equals(result.Columns[i], "AlertLevel", StringComparison.OrdinalIgnoreCase))
            {
                alertLevelIndex = i;
                break;
            }
        }

        if (alertLevelIndex < 0)
        {
            return DataValidationNavRunState.NotRun;
        }

        var hasCritical = false;
        var hasWarning = false;
        var hasOk = false;

        foreach (var row in result.Rows)
        {
            if (row.Count <= alertLevelIndex)
            {
                continue;
            }

            var level = row[alertLevelIndex]?.Trim().ToUpperInvariant();
            if (level == "CRITICAL")
            {
                hasCritical = true;
            }
            else if (level == "WARNING")
            {
                hasWarning = true;
            }
            else if (level == "OK")
            {
                hasOk = true;
            }
        }

        if (hasCritical)
        {
            return DataValidationNavRunState.Failed;
        }

        if (hasWarning)
        {
            return DataValidationNavRunState.Alert;
        }

        return hasOk ? DataValidationNavRunState.Succeeded : DataValidationNavRunState.NotRun;
    }
}
