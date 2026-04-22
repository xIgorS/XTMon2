namespace XTMon.Components.Layout;

internal sealed class StartupDiagnosticsGateController
{
    private bool _startupDiagnosticsTriggered;
    private bool _startupGateReleased;

    public bool ShowApplicationBody => _startupGateReleased;

    public void Initialize(bool hasCompleted)
    {
        if (!hasCompleted)
        {
            return;
        }

        _startupDiagnosticsTriggered = true;
        _startupGateReleased = true;
    }

    public bool ShouldTriggerStartupDiagnostics(bool firstRender)
    {
        if (!firstRender || _startupDiagnosticsTriggered)
        {
            return false;
        }

        _startupDiagnosticsTriggered = true;
        return true;
    }

    public bool TryReleaseGate(bool hasCompleted)
    {
        if (_startupGateReleased || !hasCompleted)
        {
            return false;
        }

        _startupGateReleased = true;
        return true;
    }
}