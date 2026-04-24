using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using XTMon.Infrastructure;
using XTMon.Options;

namespace XTMon.Repositories;

public sealed class UamAuthorizationRepository : IUamAuthorizationRepository
{
    private readonly SqlConnectionFactory _connectionFactory;
    private readonly UamAuthorizationOptions _options;
    private readonly ILogger<UamAuthorizationRepository> _logger;

    public UamAuthorizationRepository(
        SqlConnectionFactory connectionFactory,
        IOptions<UamAuthorizationOptions> options,
        ILogger<UamAuthorizationRepository> logger)
    {
        _connectionFactory = connectionFactory;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<bool> IsUserAuthorizedAsync(string windowsUsername, CancellationToken cancellationToken = default)
    {
        try
        {
            // Extract the user ID from DOMAIN\Username if necessary, depending on what the DB expects.
            // Assuming the whole string or just the part after the slash might be valid.
            // For safety against domain pre-fixes, we take the last part.
            var userBnpId = windowsUsername.Contains('\\') 
                ? windowsUsername.Split('\\').Last() 
                : windowsUsername;

            using var connection = _connectionFactory.CreateConnection(_options.ConnectionStringName);
            await _connectionFactory.OpenAsync(connection, cancellationToken);

            using var command = connection.CreateCommand();
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = _options.GetAdminUserStoredProcedure;
            command.CommandTimeout = _options.CommandTimeoutSeconds;

            command.Parameters.Add(new SqlParameter("@UserBnpId", SqlDbType.NVarChar, 50) { Value = userBnpId });
            command.Parameters.Add(new SqlParameter("@IsTechnical", SqlDbType.Bit) { Value = false });

            using var reader = await command.ExecuteReaderAsync(cancellationToken);

            // Replay Flow / JV Calculation requires Name = "APS"
            var nameOrdinal = reader.GetOrdinal("Name");
            while (await reader.ReadAsync(cancellationToken))
            {
                if (!reader.IsDBNull(nameOrdinal))
                {
                    var nameVal = reader.GetString(nameOrdinal);
                    if (string.Equals(nameVal, "APS", StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                }
            }
            
            return false;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Failed to check authorization for user {Username} via {StoredProcedure}",
                windowsUsername, _options.GetAdminUserStoredProcedure);
            throw;
        }
    }
}
