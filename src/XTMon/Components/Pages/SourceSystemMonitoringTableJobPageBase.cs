using Microsoft.AspNetCore.Components;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Services;

namespace XTMon.Components.Pages;

public abstract class SourceSystemMonitoringTableJobPageBase<TPage> : MonitoringTableJobPageBase<TPage>
{
    protected sealed class SourceSystemSelection
    {
        public SourceSystemSelection(string code, bool isSelected)
        {
            Code = code;
            IsSelected = isSelected;
        }

        public string Code { get; }

        public bool IsSelected { get; set; }
    }

    protected readonly List<SourceSystemSelection> sourceSystems = [];
    protected bool isLoadingSourceSystems;
    protected string? sourceSystemsError;

    protected bool AreAllSourceSystemsSelected => sourceSystems.Count > 0 && sourceSystems.All(static sourceSystem => sourceSystem.IsSelected);

    protected int SelectedSourceSystemsCount => sourceSystems.Count(static sourceSystem => sourceSystem.IsSelected);

    protected string SelectedSourceSystemsCountText => sourceSystems.Count == 0
        ? "0 / 0"
        : $"{SelectedSourceSystemsCount} / {sourceSystems.Count}";

    protected string SelectedSourceSystemsSummary => BuildSelectedSourceSystemsSummary();

    protected string SavedParameterSummaryText => string.IsNullOrWhiteSpace(savedParameterSummary)
        ? "Current source system selection"
        : savedParameterSummary;

    protected abstract string SourceSystemsLoadErrorMessage { get; }

    protected abstract string SourceSystemsProcedureName { get; }

    protected override Task OnInitializedCoreAsync() => LoadSourceSystemsAsync();

    protected override bool TryPrepareRun(out string? parametersJson, out string? parameterSummary)
    {
        parametersJson = MonitoringJobHelper.SerializeParameters(new DataValidationJobParameters(BuildSourceSystemCodesPayload(GetSelectedSourceSystemCodeList())));
        parameterSummary = SelectedSourceSystemsSummary;
        return true;
    }

    protected abstract Task<IReadOnlyList<string>> LoadAvailableSourceSystemCodesAsync(CancellationToken cancellationToken);

    protected async Task LoadSourceSystemsAsync()
    {
        isLoadingSourceSystems = true;
        sourceSystemsError = null;

        try
        {
            var availableSourceSystems = await LoadAvailableSourceSystemCodesAsync(disposeCts.Token);
            sourceSystems.Clear();
            sourceSystems.AddRange(availableSourceSystems.Select(static code => new SourceSystemSelection(code, true)));
        }
        catch (Exception ex)
        {
            Logger.LogError(
                AppLogEvents.MonitoringLoadFailed,
                ex,
                "Failed to load {MonitoringJobName} source systems from procedure {ProcedureName}.",
                MonitoringJobName,
                SourceSystemsProcedureName);
            sourceSystemsError = SourceSystemsLoadErrorMessage;
            sourceSystems.Clear();
        }
        finally
        {
            isLoadingSourceSystems = false;
        }
    }

    protected void OnAllSourceSystemsChanged(ChangeEventArgs args)
    {
        var isSelected = (bool)(args.Value ?? false);
        foreach (var sourceSystem in sourceSystems)
        {
            sourceSystem.IsSelected = isSelected;
        }
    }

    protected void OnSourceSystemChanged(string code, bool isSelected)
    {
        var sourceSystem = sourceSystems.FirstOrDefault(item => string.Equals(item.Code, code, StringComparison.OrdinalIgnoreCase));
        if (sourceSystem is null)
        {
            return;
        }

        sourceSystem.IsSelected = isSelected;
    }

    protected virtual string? BuildSourceSystemCodesPayload(IEnumerable<string> selectedCodes)
    {
        return PricingHelper.BuildSourceSystemCodes(selectedCodes, quoteEachValue: true);
    }

    protected IReadOnlyList<string> GetSelectedSourceSystemCodeList()
    {
        return sourceSystems
            .Where(static sourceSystem => sourceSystem.IsSelected)
            .Select(static sourceSystem => sourceSystem.Code)
            .ToArray();
    }

    private string BuildSelectedSourceSystemsSummary()
    {
        if (sourceSystems.Count == 0)
        {
            return "No source systems selected";
        }

        var selectedCodes = sourceSystems
            .Where(static sourceSystem => sourceSystem.IsSelected)
            .Select(static sourceSystem => sourceSystem.Code)
            .ToArray();

        return selectedCodes.Length switch
        {
            0 => "No source systems selected",
            _ when selectedCodes.Length == sourceSystems.Count => "All source systems",
            _ => string.Join(", ", selectedCodes)
        };
    }
}