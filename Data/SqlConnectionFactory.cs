using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

namespace XTMon.Data;

public sealed class SqlConnectionFactory
{
    private readonly IConfiguration _configuration;

    public SqlConnectionFactory(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public SqlConnection CreateConnection()
    {
        return CreateConnection("LogFiAlmt");
    }

    public SqlConnection CreateConnection(string connectionStringName)
    {
        var connectionString = _configuration.GetConnectionString(connectionStringName);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException($"Connection string '{connectionStringName}' is not configured.");
        }

        return new SqlConnection(connectionString);
    }
}
