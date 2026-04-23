namespace XTMon.Services;

public readonly record struct BackgroundJobCancellationResult(bool WasActive, bool CancellationConfirmed)
{
    public static BackgroundJobCancellationResult AlreadyInactive => new(false, false);

    public static BackgroundJobCancellationResult Confirmed => new(true, true);

    public static BackgroundJobCancellationResult Pending => new(true, false);
}