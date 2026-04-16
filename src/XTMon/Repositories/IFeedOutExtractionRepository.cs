using XTMon.Models;

namespace XTMon.Repositories;

public interface IFeedOutExtractionRepository
{
    Task<FeedOutExtractionResult> GetFeedOutExtractionAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}