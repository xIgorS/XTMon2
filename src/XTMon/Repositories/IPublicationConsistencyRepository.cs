using XTMon.Models;

namespace XTMon.Repositories;

public interface IPublicationConsistencyRepository
{
    Task<PublicationConsistencyResult> GetPublicationConsistencyAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}