using System.Threading;
using Microsoft.Data.SqlClient;
using XTMon.Helpers;
using XTMon.Models;
using XTMon.Repositories;

namespace XTMon.Services;

public sealed class FunctionalRejectionMenuState : IDisposable
{
    private static readonly TimeSpan SqlFailureBackoff = TimeSpan.FromSeconds(10);
    private const string CachedItemsMessage = "Showing previously loaded Functional Rejection items while live refresh is unavailable.";
    private const string SqlUnavailableMessage = "Functional Rejection items are temporarily unavailable while the menu database connection recovers.";
    private const string GenericUnavailableMessage = "Unable to load Functional Rejection items right now.";

    private readonly IFunctionalRejectionRepository _repository;
    private readonly ILogger<FunctionalRejectionMenuState> _logger;
    private readonly SemaphoreSlim _refreshLock = new(1, 1);
    private IReadOnlyList<FunctionalRejectionMenuItem> _menuItems = Array.Empty<FunctionalRejectionMenuItem>();
    private DateTimeOffset? _nextRefreshAllowedAtUtc;

    public FunctionalRejectionMenuState(
        IFunctionalRejectionRepository repository,
        ILogger<FunctionalRejectionMenuState> logger)
    {
        _repository = repository;
        _logger = logger;
    }

    public IReadOnlyList<FunctionalRejectionMenuItem> MenuItems => _menuItems;

    public string? ErrorMessage { get; private set; }

    public string? WarningMessage { get; private set; }

    public async Task RefreshAsync(CancellationToken cancellationToken)
    {
        if (IsSqlBackoffActive())
        {
            ApplyUnavailableState(hasCachedItems: _menuItems.Count > 0, isSqlFailure: true);
            return;
        }

        await _refreshLock.WaitAsync(cancellationToken);
        try
        {
            if (IsSqlBackoffActive())
            {
                ApplyUnavailableState(hasCachedItems: _menuItems.Count > 0, isSqlFailure: true);
                return;
            }

            var items = await _repository.GetMenuItemsAsync(cancellationToken);
            _menuItems = items;
            _nextRefreshAllowedAtUtc = null;
            ErrorMessage = null;
            WarningMessage = null;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (SqlException ex) when (SqlDataHelper.IsSqlTimeout(ex) || SqlDataHelper.IsSqlConnectionFailure(ex) || SqlDataHelper.IsSqlDeadlock(ex))
        {
            _nextRefreshAllowedAtUtc = DateTimeOffset.UtcNow.Add(SqlFailureBackoff);
            _logger.LogWarning(
                ex,
                "Unable to refresh Functional Rejection menu items due to a SQL timeout/connection/deadlock problem. Preserving current items and backing off until {RetryAtUtc:O}.",
                _nextRefreshAllowedAtUtc.Value);
            ApplyUnavailableState(hasCachedItems: _menuItems.Count > 0, isSqlFailure: true);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Unable to refresh Functional Rejection menu items.");
            ApplyUnavailableState(hasCachedItems: _menuItems.Count > 0, isSqlFailure: false);
        }
        finally
        {
            _refreshLock.Release();
        }
    }

    public void Dispose()
    {
        _refreshLock.Dispose();
    }

    private bool IsSqlBackoffActive()
    {
        return _nextRefreshAllowedAtUtc.HasValue
            && DateTimeOffset.UtcNow < _nextRefreshAllowedAtUtc.Value;
    }

    private void ApplyUnavailableState(bool hasCachedItems, bool isSqlFailure)
    {
        if (hasCachedItems)
        {
            ErrorMessage = null;
            WarningMessage = CachedItemsMessage;
            return;
        }

        WarningMessage = null;
        ErrorMessage = isSqlFailure
            ? SqlUnavailableMessage
            : GenericUnavailableMessage;
    }
}