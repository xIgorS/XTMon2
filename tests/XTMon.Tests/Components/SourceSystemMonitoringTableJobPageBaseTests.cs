using XTMon.Components.Pages;

namespace XTMon.Tests.Components;

public class SourceSystemMonitoringTableJobPageBaseTests
{
    [Fact]
    public void BuildSourceSystemCodesPayload_DefaultBehavior_QuotesEachCode()
    {
        var page = new TestSourceSystemPage();

        var result = page.BuildPayload("ALMT", "XTG");

        Assert.Equal("'ALMT','XTG'", result);
    }

    private sealed class TestSourceSystemPage : SourceSystemMonitoringTableJobPageBase<TestSourceSystemPage>
    {
        protected override string MonitoringSubmenuKey => "test-source-system";

        protected override string MonitoringJobName => "Test Source System";

        protected override string DefaultLoadErrorMessage => "Load failed";

        protected override string SourceSystemsLoadErrorMessage => "Source systems failed";

        protected override string SourceSystemsProcedureName => "dbo.TestSourceSystems";

        public string? BuildPayload(params string[] selectedCodes)
        {
            return BuildSourceSystemCodesPayload(selectedCodes);
        }

        protected override Task<IReadOnlyList<string>> LoadAvailableSourceSystemCodesAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>());
        }
    }
}