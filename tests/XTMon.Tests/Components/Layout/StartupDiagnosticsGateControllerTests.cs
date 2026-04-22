using XTMon.Components.Layout;

namespace XTMon.Tests.Components.Layout;

public class StartupDiagnosticsGateControllerTests
{
    [Fact]
    public void Initialize_WhenDiagnosticsAlreadyCompleted_OpensGateImmediately()
    {
        var controller = new StartupDiagnosticsGateController();

        controller.Initialize(hasCompleted: true);

        Assert.True(controller.ShowApplicationBody);
        Assert.False(controller.ShouldTriggerStartupDiagnostics(firstRender: true));
    }

    [Fact]
    public void FirstRender_TriggersDiagnosticsOnlyOnce_AndReleasesAfterCompletion()
    {
        var controller = new StartupDiagnosticsGateController();

        Assert.False(controller.ShowApplicationBody);
        Assert.True(controller.ShouldTriggerStartupDiagnostics(firstRender: true));
        Assert.False(controller.ShouldTriggerStartupDiagnostics(firstRender: true));
        Assert.False(controller.ShowApplicationBody);

        Assert.True(controller.TryReleaseGate(hasCompleted: true));
        Assert.True(controller.ShowApplicationBody);
        Assert.False(controller.TryReleaseGate(hasCompleted: true));
    }
}