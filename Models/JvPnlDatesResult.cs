namespace XTMon.Models;

public sealed record JvPnlDatesResult(
    DateOnly? DefaultDate,
    IReadOnlyList<DateOnly> AvailableDates);
