using Microsoft.AspNetCore.Components;
using Microsoft.JSInterop;
using XTMon.Helpers;
using XTMon.Models;

namespace XTMon.Components.Pages;

public abstract class MonitoringTableJobPageBase<TPage> : MonitoringJobPageBase<TPage>
{
    [Inject]
    protected IJSRuntime JsRuntime { get; set; } = default!;

    protected string parsedQuery = string.Empty;
    protected string? copyMessage;
    protected bool copySucceeded;
    protected MonitoringTableResult? result;
    protected bool showQuery;
    protected string? savedParameterSummary;
    protected int totalRowCount;
    protected int persistedRowCount;
    protected bool truncated;
    protected bool hasPersistedJob;

    protected string QueryDisplayText => string.IsNullOrWhiteSpace(parsedQuery) ? string.Empty : parsedQuery;

    protected override void OnBeforeRun()
    {
        copyMessage = null;
        showQuery = false;
    }

    protected override void OnRunFailed()
    {
        parsedQuery = string.Empty;
        result = null;
        OnAfterRunFailed();
    }

    protected override void ApplyJobCore(MonitoringJobRecord job)
    {
        savedParameterSummary = job.ParameterSummary;
        parsedQuery = job.ParsedQuery ?? string.Empty;
        result = JvCalculationHelper.DeserializeMonitoringTable(job.GridColumnsJson, job.GridRowsJson);
        hasPersistedJob = job.SavedAt.HasValue;

        if (MonitoringJobHelper.TryReadPersistMetadata(job.MetadataJson, out var total, out var persisted, out var isTruncated))
        {
            totalRowCount = total;
            persistedRowCount = persisted;
            truncated = isTruncated;
        }
        else
        {
            persistedRowCount = result?.Rows.Count ?? 0;
            totalRowCount = persistedRowCount;
            truncated = false;
        }

        OnAfterApplyTableJob(job);
    }

    protected override void ClearLoadedStateCore()
    {
        savedParameterSummary = null;
        parsedQuery = string.Empty;
        result = null;
        totalRowCount = 0;
        persistedRowCount = 0;
        truncated = false;
        hasPersistedJob = false;
        OnAfterClearLoadedState();
    }

    protected override bool HasLoadedResult() => result is not null;

    protected virtual void OnAfterApplyTableJob(MonitoringJobRecord job)
    {
    }

    protected virtual void OnAfterClearLoadedState()
    {
    }

    protected virtual void OnAfterRunFailed()
    {
    }

    protected void ToggleQueryVisibility()
    {
        if (string.IsNullOrWhiteSpace(parsedQuery))
        {
            return;
        }

        showQuery = !showQuery;
    }

    protected async Task CopySqlToClipboardAsync()
    {
        if (string.IsNullOrWhiteSpace(parsedQuery))
        {
            copyMessage = "No SQL statement available to copy.";
            copySucceeded = false;
            return;
        }

        try
        {
            await JsRuntime.InvokeVoidAsync("navigator.clipboard.writeText", parsedQuery);
            copyMessage = "SQL copied to clipboard.";
            copySucceeded = true;
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Unable to copy {MonitoringJobName} SQL statement to clipboard.", MonitoringJobName);
            copyMessage = "Failed to copy SQL to clipboard.";
            copySucceeded = false;
        }
    }

}