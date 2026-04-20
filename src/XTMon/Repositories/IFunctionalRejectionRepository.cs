using XTMon.Models;

namespace XTMon.Repositories;

public interface IFunctionalRejectionRepository
{
    Task<IReadOnlyList<FunctionalRejectionMenuItem>> GetMenuItemsAsync(CancellationToken cancellationToken);

    Task<TechnicalRejectResult> GetTechnicalRejectAsync(
        DateOnly pnlDate,
        int businessDataTypeId,
    string dbConnection,
        string sourceSystemName,
        CancellationToken cancellationToken);
}