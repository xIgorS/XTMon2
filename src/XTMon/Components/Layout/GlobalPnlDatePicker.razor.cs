using Microsoft.AspNetCore.Components;
using XTMon.Repositories;
using XTMon.Services;

namespace XTMon.Components.Layout;

public partial class GlobalPnlDatePicker : ComponentBase
{
    [Inject]
    private PnlDateState PnlDateState { get; set; } = default!;

    [Inject]
    private IJvCalculationRepository PnlDateRepository { get; set; } = default!;

    [Inject]
    private ILogger<GlobalPnlDatePicker> Logger { get; set; } = default!;

    protected override async Task OnInitializedAsync()
    {
        try
        {
            await PnlDateState.EnsureLoadedAsync(PnlDateRepository, CancellationToken.None);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Unable to load default PNL dates for global date picker.");
        }
    }

    private Task OnGlobalDateChanged(DateOnly date)
    {
        PnlDateState.SetDate(date);
        return Task.CompletedTask;
    }
}
