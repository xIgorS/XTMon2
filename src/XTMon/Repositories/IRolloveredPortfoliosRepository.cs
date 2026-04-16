using XTMon.Models;

namespace XTMon.Repositories;

public interface IRolloveredPortfoliosRepository
{
    Task<RolloveredPortfoliosResult> GetRolloveredPortfoliosAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}