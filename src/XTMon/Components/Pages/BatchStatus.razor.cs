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

public partial class BatchStatus : ComponentBase, IDisposable
{
    private const string DisplayDateFormat = "dd-MM-yyyy";
    private const string DisplayDateTimeFormat = "dd-MM-yyyy HH:mm:ss";
    private const string LoadErrorMessage = "Unable to load batch status right now. Please try again.";

    private readonly HashSet<DateOnly> availableDates = [];

    [Inject]
    private IBatchStatusRepository Repository { get; set; } = default!;

    [Inject]
    private IJvCalculationRepository PnlDateRepository { get; set; } = default!;

    [Inject]
    private PnlDateState PnlDateState { get; set; } = default!;

    [Inject]
    private IOptions<BatchStatusOptions> BatchStatusOptions { get; set; } = default!;

    [Inject]
    private ILogger<BatchStatus> Logger { get; set; } = default!;

    private DateOnly? selectedPnlDate;
    private bool isLoading;
    private bool hasRun;
    private string? validationError;
    private string? loadError;
    private DateTime? lastRunAt;
    private IReadOnlyList<BatchStatusGridRow> gridRows = Array.Empty<BatchStatusGridRow>();

    private string ProcedureName => BatchStatusOptions.Value.CheckBatchStatusStoredProcedure;
    private string FullyQualifiedProcedureName => JvCalculationHelper.BuildFullyQualifiedProcedureName(BatchStatusOptions.Value.ConnectionStringName, ProcedureName);
    private string SelectedPnlDateText => selectedPnlDate.HasValue
        ? selectedPnlDate.Value.ToString(DisplayDateFormat, CultureInfo.InvariantCulture)
        : "-";
    private string LastRunText => lastRunAt.HasValue
        ? lastRunAt.Value.ToString(DisplayDateTimeFormat, CultureInfo.InvariantCulture)
        : "-";

    protected override async Task OnInitializedAsync()
    {
        PnlDateState.OnDateChanged += OnGlobalPnlDateChanged;
        await LoadPnlDatesAsync();
    }

    private async Task LoadPnlDatesAsync()
    {
        try
        {
            await PnlDateState.EnsureLoadedAsync(PnlDateRepository, CancellationToken.None);
            selectedPnlDate = PnlDateState.SelectedDate;

            availableDates.Clear();
            foreach (var date in PnlDateState.AvailableDates)
            {
                availableDates.Add(date);
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Unable to load default PNL dates.");
        }
    }

    private void OnGlobalPnlDateChanged()
    {
        InvokeAsync(() =>
        {
            selectedPnlDate = PnlDateState.SelectedDate;
            StateHasChanged();
        });
    }

    public void Dispose()
    {
        PnlDateState.OnDateChanged -= OnGlobalPnlDateChanged;
    }

    private Task OnPnlDateSelected(DateOnly date)
    {
        selectedPnlDate = date;
        validationError = null;
        loadError = null;
        return Task.CompletedTask;
    }

    private async Task RunAsync()
    {
        if (!selectedPnlDate.HasValue)
        {
            validationError = "PNL DATE is required.";
            return;
        }

        isLoading = true;
        hasRun = true;
        validationError = null;
        loadError = null;

        try
        {
            var table = await Repository.GetBatchStatusAsync(selectedPnlDate.Value, CancellationToken.None);
            gridRows = BatchStatusHelper.BuildGridRows(table);
            lastRunAt = DateTime.Now;
        }
        catch (Exception ex)
        {
            Logger.LogError(
                AppLogEvents.MonitoringLoadFailed,
                ex,
                "Failed to load batch status for PnlDate {PnlDate}.",
                selectedPnlDate.Value);
            loadError = LoadErrorMessage;
            gridRows = Array.Empty<BatchStatusGridRow>();
        }
        finally
        {
            isLoading = false;
        }
    }

}