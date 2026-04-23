using XTMon.Helpers;
using XTMon.Models;

namespace XTMon.Tests.Helpers;

public class DataValidationNavAlertHelperTests
{
    private static readonly DateOnly TestDate = new(2026, 4, 21);

    [Fact]
    public void GetRunState_ReturnsFailedForFailedJob()
    {
        var job = CreateJob("batch-status", status: "Failed", failedAt: DateTime.UtcNow);

        var result = DataValidationNavAlertHelper.GetRunState(job);

        Assert.Equal(DataValidationNavRunState.Failed, result);
    }

    [Fact]
    public void GetRunState_ReturnsNotRunForCancelledJobThatNeverStarted()
    {
        var job = CreateJob("batch-status", status: MonitoringJobHelper.CancelledStatus, startedAt: null, failedAt: null);

        var result = DataValidationNavAlertHelper.GetRunState(job);

        Assert.Equal(DataValidationNavRunState.NotRun, result);
    }

    [Fact]
    public void GetRunState_ReturnsCancelledForCancelledJobThatStarted()
    {
        var job = CreateJob("batch-status", status: MonitoringJobHelper.CancelledStatus, startedAt: DateTime.UtcNow, failedAt: DateTime.UtcNow);

        var result = DataValidationNavAlertHelper.GetRunState(job);

        Assert.Equal(DataValidationNavRunState.Cancelled, result);
    }

    [Fact]
    public void GetRunState_ReturnsRunningForActiveJob()
    {
        var job = CreateJob("daily-balance", status: "Running");

        var result = DataValidationNavAlertHelper.GetRunState(job);

        Assert.Equal(DataValidationNavRunState.Running, result);
    }

    [Fact]
    public void GetRunState_PrefersCompletedTerminalStateOverQueuedStatus()
    {
        var job = CreateJob("future-cash", status: "Queued", completedAt: DateTime.UtcNow);

        var result = DataValidationNavAlertHelper.GetRunState(job);

        Assert.Equal(DataValidationNavRunState.Succeeded, result);
    }

    [Fact]
    public void GetRunState_PrefersFailedTerminalStateOverRunningStatus()
    {
        var job = CreateJob("future-cash", status: "Running", failedAt: DateTime.UtcNow);

        var result = DataValidationNavAlertHelper.GetRunState(job);

        Assert.Equal(DataValidationNavRunState.Failed, result);
    }

    [Fact]
    public void GetRunState_ReturnsSucceededForCompletedJob()
    {
        var job = CreateJob("market-data", status: "Completed", completedAt: DateTime.UtcNow);

        var result = DataValidationNavAlertHelper.GetRunState(job);

        Assert.Equal(DataValidationNavRunState.Succeeded, result);
    }

    [Fact]
    public void GetRunState_ReturnsAlertForBatchStatusKoRow()
    {
        var job = CreateJob(
            "batch-status",
            status: "Completed",
            completedAt: DateTime.UtcNow,
            columnsJson: "[\"Status\",\"Message\"]",
            rowsJson: "[[\"KO\",\"Broken\"]]");

        var result = DataValidationNavAlertHelper.GetRunState(job);

        Assert.Equal(DataValidationNavRunState.Alert, result);
    }

    [Fact]
    public void GetRunState_ReturnsAlertForMarketDataMissingRow()
    {
        var job = CreateJob(
            "market-data",
            status: "Completed",
            completedAt: DateTime.UtcNow,
            columnsJson: "[\"Result\",\"Message\"]",
            rowsJson: "[[\"MISSING\",\"Feed missing\"]]");

        var result = DataValidationNavAlertHelper.GetRunState(job);

        Assert.Equal(DataValidationNavRunState.Alert, result);
    }

    [Fact]
    public void GetRunState_ReturnsAlertForDailyBalanceStatusStartingWithKo()
    {
        var job = CreateJob(
            "daily-balance",
            status: "Completed",
            completedAt: DateTime.UtcNow,
            columnsJson: "[\"Status\",\"Source\"]",
            rowsJson: "[[\"KO - mismatch\",\"XTG\"]]");

        var result = DataValidationNavAlertHelper.GetRunState(job);

        Assert.Equal(DataValidationNavRunState.Alert, result);
    }

    [Fact]
    public void GetRunState_ReturnsAlertForFutureCashRows()
    {
        var job = CreateJob(
            "future-cash",
            status: "Completed",
            completedAt: DateTime.UtcNow,
            columnsJson: "[\"Portfolio\"]",
            rowsJson: "[[\"ABC\"]]");

        var result = DataValidationNavAlertHelper.GetRunState(job);

        Assert.Equal(DataValidationNavRunState.Alert, result);
    }

    [Fact]
    public void GetRunState_ReturnsSucceededForCompletedRouteWithoutConfiguredAlertRule()
    {
        var job = CreateJob(
            "reverse-conso-file",
            status: "Completed",
            completedAt: DateTime.UtcNow,
            columnsJson: "[\"Anything\"]",
            rowsJson: "[[\"Value\"]]");

        var result = DataValidationNavAlertHelper.GetRunState(job);

        Assert.Equal(DataValidationNavRunState.Succeeded, result);
    }

    [Fact]
    public void GetRunState_ReturnsNotRunWhenJobIsMissing()
    {
        var result = DataValidationNavAlertHelper.GetRunState(null);

        Assert.Equal(DataValidationNavRunState.NotRun, result);
    }

    [Fact]
    public void GetRunState_ReturnsNotRunForUnknownStatus()
    {
        var job = CreateJob("batch-status", status: "QueuedForRetry", completedAt: null, failedAt: null);

        var result = DataValidationNavAlertHelper.GetRunState(job);

        Assert.Equal(DataValidationNavRunState.NotRun, result);
    }

    [Fact]
    public void GetRunState_ReturnsAlertWhenMetadataJsonHasHasAlertsTrue()
    {
        var job = CreateJob("daily-balance", status: "Completed", completedAt: DateTime.UtcNow,
            metadataJson: "{\"hasAlerts\":true}");

        var result = DataValidationNavAlertHelper.GetRunState(job);

        Assert.Equal(DataValidationNavRunState.Alert, result);
    }

    [Fact]
    public void GetRunState_ReturnsSucceededWhenMetadataJsonHasHasAlertsFalse()
    {
        var job = CreateJob("daily-balance", status: "Completed", completedAt: DateTime.UtcNow,
            metadataJson: "{\"hasAlerts\":false}");

        var result = DataValidationNavAlertHelper.GetRunState(job);

        Assert.Equal(DataValidationNavRunState.Succeeded, result);
    }

    [Fact]
    public void GetRunState_FallsBackToGridDataWhenMetadataJsonAbsent()
    {
        var job = CreateJob("daily-balance", status: "Completed", completedAt: DateTime.UtcNow,
            columnsJson: "[\"Status\",\"Source\"]",
            rowsJson: "[[\"KO - mismatch\",\"XTG\"]]");

        var result = DataValidationNavAlertHelper.GetRunState(job);

        Assert.Equal(DataValidationNavRunState.Alert, result);
    }

    [Fact]
    public void GetRunState_IgnoresMalformedMetadataJsonAndFallsBackToGrid()
    {
        var job = CreateJob("daily-balance", status: "Completed", completedAt: DateTime.UtcNow,
            metadataJson: "not-valid-json",
            columnsJson: "[\"Status\"]",
            rowsJson: "[[\"KO - mismatch\"]]");

        var result = DataValidationNavAlertHelper.GetRunState(job);

        Assert.Equal(DataValidationNavRunState.Alert, result);
    }

    [Theory]
    [InlineData("daily-balance", "[\"Status\"]", "[[\"KO - issue\"]]", true)]
    [InlineData("daily-balance", "[\"Status\"]", "[[\"OK\"]]", false)]
    [InlineData("market-data", "[\"Result\"]", "[[\"MISSING\"]]", true)]
    [InlineData("market-data", "[\"Result\"]", "[[\"OK\"]]", false)]
    [InlineData("future-cash", "[\"Portfolio\"]", "[[\"ABC\"]]", true)]
    [InlineData("future-cash", null, null, false)]
    [InlineData("reverse-conso-file", "[\"Col\"]", "[[\"val\"]]", false)]
    public void BuildMetadataJson_ReturnsCorrectHasAlertsValue(
        string submenuKey, string? columnsJson, string? rowsJson, bool expectedHasAlerts)
    {
        var table = columnsJson is null
            ? null
            : new MonitoringTableResult(
                System.Text.Json.JsonSerializer.Deserialize<List<string>>(columnsJson)!,
                System.Text.Json.JsonSerializer.Deserialize<List<List<string?>>>(rowsJson!)!
                    .Select(r => (IReadOnlyList<string?>)r).ToList());

        var json = DataValidationNavAlertHelper.BuildMetadataJson(submenuKey, table);

        Assert.Equal($"{{\"hasAlerts\":{(expectedHasAlerts ? "true" : "false")}}}", json);
    }

    private static MonitoringJobRecord CreateJob(
        string submenuKey,
        string status,
        DateTime? startedAt = null,
        DateTime? completedAt = null,
        DateTime? failedAt = null,
        string? columnsJson = null,
        string? rowsJson = null,
        string? metadataJson = null)
    {
        return new MonitoringJobRecord(
            JobId: 1,
            Category: MonitoringJobHelper.DataValidationCategory,
            SubmenuKey: submenuKey,
            DisplayName: submenuKey,
            PnlDate: TestDate,
            Status: status,
            WorkerId: null,
            ParametersJson: null,
            ParameterSummary: null,
            EnqueuedAt: DateTime.UtcNow,
            StartedAt: startedAt,
            LastHeartbeatAt: startedAt,
            CompletedAt: completedAt,
            FailedAt: failedAt,
            ErrorMessage: null,
            ParsedQuery: "SELECT 1",
            GridColumnsJson: columnsJson,
            GridRowsJson: rowsJson,
            MetadataJson: metadataJson,
            SavedAt: DateTime.UtcNow);
    }
}