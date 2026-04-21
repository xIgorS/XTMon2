namespace XTMon.Helpers;

internal static class NavRunStateAggregator
{
    public static DataValidationNavRunState Aggregate(IEnumerable<DataValidationNavRunState> statuses)
    {
        var hasAny = false;
        var hasFailed = false;
        var hasRunning = false;
        var hasSucceeded = false;
        var hasNotRun = false;

        foreach (var status in statuses)
        {
            hasAny = true;
            switch (status)
            {
                case DataValidationNavRunState.Failed:
                case DataValidationNavRunState.Alert:
                    hasFailed = true;
                    break;
                case DataValidationNavRunState.Running:
                    hasRunning = true;
                    break;
                case DataValidationNavRunState.Succeeded:
                    hasSucceeded = true;
                    break;
                default:
                    hasNotRun = true;
                    break;
            }
        }

        if (!hasAny || (hasNotRun && !hasSucceeded && !hasRunning && !hasFailed))
        {
            return DataValidationNavRunState.NotRun;
        }

        if (hasFailed)
        {
            return DataValidationNavRunState.Failed;
        }

        if (hasRunning)
        {
            return DataValidationNavRunState.Running;
        }

        if (hasSucceeded && !hasNotRun)
        {
            return DataValidationNavRunState.Succeeded;
        }

        return DataValidationNavRunState.Succeeded;
    }
}
