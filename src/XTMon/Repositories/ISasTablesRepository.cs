using XTMon.Models;

namespace XTMon.Repositories;

public interface ISasTablesRepository
{
    Task<SasTablesResult> GetSasTablesAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}