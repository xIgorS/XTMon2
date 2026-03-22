namespace XTMon.Repositories;

public interface IUamAuthorizationRepository
{
    Task<bool> IsUserAuthorizedAsync(string windowsUsername, CancellationToken cancellationToken = default);
}
