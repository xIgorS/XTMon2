using Microsoft.AspNetCore.Components;
using System.Globalization;

namespace XTMon.Components.Shared;

public partial class DatePickerInput : ComponentBase
{
    private static readonly string[] WeekdayLabels = ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"];
    private const string InputDateFormat = "yyyy-MM-dd";
    private const string DisplayDateFormat = "dd-MM-yyyy";

    [Parameter]
    public DateOnly? Value { get; set; }

    [Parameter]
    public EventCallback<DateOnly> ValueChanged { get; set; }

    /// <summary>
    /// When provided, only these dates are selectable and all others are shown as disabled.
    /// When null, all dates are selectable.
    /// </summary>
    [Parameter]
    public IReadOnlyCollection<DateOnly>? AvailableDates { get; set; }

    [Parameter]
    public bool IsDisabled { get; set; }

    [Parameter]
    public string? InputId { get; set; }

    [Parameter]
    public string? AriaLabel { get; set; }

    private bool isOpen;
    private DateOnly monthStart = new(DateTime.Today.Year, DateTime.Today.Month, 1);

    private string DisplayValue => Value.HasValue
        ? Value.Value.ToString(DisplayDateFormat, CultureInfo.InvariantCulture)
        : string.Empty;

    private void Toggle()
    {
        if (IsDisabled)
        {
            return;
        }

        isOpen = !isOpen;

        if (isOpen)
        {
            if (Value.HasValue)
            {
                monthStart = new DateOnly(Value.Value.Year, Value.Value.Month, 1);
            }
            else if (AvailableDates?.Count > 0)
            {
                var nearest = AvailableDates.OrderByDescending(static d => d).First();
                monthStart = new DateOnly(nearest.Year, nearest.Month, 1);
            }
            else
            {
                monthStart = new DateOnly(DateTime.Today.Year, DateTime.Today.Month, 1);
            }
        }
    }

    private void MoveMonth(int delta)
    {
        monthStart = monthStart.AddMonths(delta);
    }

    private async Task SelectDateAsync(DateOnly date)
    {
        if (AvailableDates is not null && !AvailableDates.Contains(date))
        {
            return;
        }

        isOpen = false;
        await ValueChanged.InvokeAsync(date);
    }

    private string GetCalendarTitle()
    {
        return monthStart.ToDateTime(TimeOnly.MinValue).ToString("MMMM yyyy", CultureInfo.InvariantCulture);
    }

    private IReadOnlyList<CalendarDay> GetCalendarDays()
    {
        var firstOfMonth = monthStart;
        var firstDayOfWeek = (int)firstOfMonth.DayOfWeek;
        var offsetFromMonday = (firstDayOfWeek + 6) % 7;
        var gridStart = firstOfMonth.AddDays(-offsetFromMonday);

        var days = new List<CalendarDay>(42);
        for (var i = 0; i < 42; i++)
        {
            var date = gridStart.AddDays(i);
            var isCurrentMonth = date.Month == monthStart.Month && date.Year == monthStart.Year;
            var isSelectable = AvailableDates is null || AvailableDates.Contains(date);
            var isSelected = Value.HasValue && Value.Value == date;

            days.Add(new CalendarDay(date, isCurrentMonth, isSelectable, isSelected));
        }

        return days;
    }

    private static string GetDayClass(CalendarDay day)
    {
        var classes = new List<string>(3) { "date-picker__day" };

        if (!day.IsCurrentMonth)
            classes.Add("date-picker__day--outside");

        if (!day.IsSelectable)
            classes.Add("date-picker__day--disabled");

        if (day.IsSelected)
            classes.Add("date-picker__day--selected");

        return string.Join(" ", classes);
    }

    private sealed record CalendarDay(DateOnly Date, bool IsCurrentMonth, bool IsSelectable, bool IsSelected);
}
