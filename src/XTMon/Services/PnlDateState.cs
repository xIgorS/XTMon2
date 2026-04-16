using XTMon.Repositories;

namespace XTMon.Services;

/// <summary>
/// Scoped state holding the global PnL date and available dates for the current circuit.
/// First caller triggers a DB load; subsequent callers reuse the cached result.
/// </summary>
public sealed class PnlDateState
{
    private readonly SemaphoreSlim _loadLock = new(1, 1);

    public DateOnly? SelectedDate { get; private set; }

    public HashSet<DateOnly> AvailableDates { get; } = [];

    public bool IsLoaded { get; private set; }

    public event Action? OnDateChanged;

    public void SetDate(DateOnly date)
    {
        if (SelectedDate == date)
        {
            return;
        }

        SelectedDate = date;
        OnDateChanged?.Invoke();
    }

    public async Task EnsureLoadedAsync(IJvCalculationRepository repository, CancellationToken cancellationToken)
    {
        if (IsLoaded)
        {
            return;
        }

        await _loadLock.WaitAsync(cancellationToken);
        try
        {
            if (IsLoaded)
            {
                return;
            }

            var response = await repository.GetJvPnlDatesAsync(cancellationToken);

            AvailableDates.Clear();
            foreach (var date in response.AvailableDates)
            {
                AvailableDates.Add(date);
            }

            var selectedDate = response.DefaultDate;
            if (!selectedDate.HasValue && response.AvailableDates.Count > 0)
            {
                selectedDate = response.AvailableDates[0];
            }

            if (selectedDate.HasValue)
            {
                SelectedDate = selectedDate.Value;
            }

            IsLoaded = true;
        }
        finally
        {
            _loadLock.Release();
        }
    }
}
