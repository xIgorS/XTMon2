using System.Globalization;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;
using XTMon.Services;

namespace XTMon.Components.Pages;

public partial class BatchStatus : MonitoringJobPageBase<BatchStatus>
{
    private const string LoadErrorMessage = "Unable to load batch status right now. Please try again.";
    protected override string MonitoringSubmenuKey => MonitoringJobHelper.BatchStatusSubmenuKey;
    protected override string MonitoringJobName => "Batch Status";
    protected override string DefaultLoadErrorMessage => LoadErrorMessage;

    [Inject]
    private IOptions<BatchStatusOptions> BatchStatusOptions { get; set; } = default!;
    private IReadOnlyList<BatchStatusGridRow> gridRows = Array.Empty<BatchStatusGridRow>();
    private DateTime? activeJobEnqueuedAt;
    private DateTime? activeJobStartedAt;
    private DateTime? activeJobCompletedAt;
    private string? activeJobError;

    private string ProcedureName => BatchStatusOptions.Value.CheckBatchStatusStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(BatchStatusOptions.Value.ConnectionStringName, ProcedureName);
    private string JobStatusText => string.IsNullOrWhiteSpace(activeJobStatus) ? "-" : activeJobStatus;
    protected override void ApplyJobCore(MonitoringJobRecord job)
    {
        activeJobEnqueuedAt = job.EnqueuedAt;
        activeJobStartedAt = job.StartedAt;
        activeJobCompletedAt = job.CompletedAt;
        activeJobError = MonitoringDisplayHelper.GetSafeBackgroundJobMessage(job.ErrorMessage, LoadErrorMessage);

        var table = JvCalculationHelper.DeserializeMonitoringTable(job.GridColumnsJson, job.GridRowsJson);
        gridRows = BatchStatusHelper.BuildGridRows(table);
    }

    protected override void ClearLoadedStateCore()
    {
        activeJobEnqueuedAt = null;
        activeJobStartedAt = null;
        activeJobCompletedAt = null;
        activeJobError = null;
        gridRows = Array.Empty<BatchStatusGridRow>();
    }

    protected override bool HasLoadedResult() => gridRows.Count > 0;

}