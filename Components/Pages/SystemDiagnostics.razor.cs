using Microsoft.AspNetCore.Components;
using XTMon.Data;
using XTMon.Models;

namespace XTMon.Components.Pages;

public partial class SystemDiagnostics : ComponentBase
{
    [Inject]
    private IDeploymentCheckService CheckService { get; set; } = default!;

    [Inject]
    private ILogger<SystemDiagnostics> Logger { get; set; } = default!;

    private DiagnosticsReport? report;
    private bool isRunning;
    private string? runError;

    private async Task RunCheckAsync()
    {
        isRunning = true;
        runError = null;
        report = null;
        StateHasChanged();

        try
        {
            report = await CheckService.RunCheckAsync(CancellationToken.None);
        }
        catch (Exception ex)
        {
            Logger.LogError(AppLogEvents.DiagnosticsCheckFailed, ex, "System diagnostics check failed unexpectedly.");
            runError = "An unexpected error occurred while running the diagnostics check. Please try again.";
        }
        finally
        {
            isRunning = false;
        }
    }

    private static string FormatDuration(TimeSpan duration) =>
        duration.TotalMilliseconds < 1000
            ? $"{duration.TotalMilliseconds:0} ms"
            : $"{duration.TotalSeconds:0.0} s";

    private static string FormatParameters(IReadOnlyList<StoredProcedureParameterInfo> parameters) =>
        string.Join(", ", parameters.Select(p => p.IsOutput ? $"{p.Name} ({p.TypeName} OUT)" : $"{p.Name} ({p.TypeName})"));
}
