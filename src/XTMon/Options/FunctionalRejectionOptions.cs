using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class FunctionalRejectionOptions
{
    public const string SectionName = "FunctionalRejection";

    [Required]
    public string MenuConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string StagingConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string DtmConnectionStringName { get; set; } = "DtmFi";

    [Required]
    public string SourceSystemTechnicalRejectStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringSourceSystemTechnicalReject";

    [Required]
    public string TechnicalRejectStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringTechnicalReject";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;

    public string ResolveDetailConnectionStringName(string? dbConnection)
    {
        if (string.IsNullOrWhiteSpace(dbConnection))
        {
            throw new InvalidOperationException("Functional Rejection dbconnexion value is required.");
        }

        var normalized = dbConnection.Trim();
        if (string.Equals(normalized, "DTM", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(normalized, "DTM_FI", StringComparison.OrdinalIgnoreCase))
        {
            return DtmConnectionStringName;
        }

        if (string.Equals(normalized, "STAGING", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(normalized, "STAGING_FI_ALMT", StringComparison.OrdinalIgnoreCase))
        {
            return StagingConnectionStringName;
        }

        throw new InvalidOperationException($"Unsupported Functional Rejection dbconnexion value '{dbConnection}'.");
    }
}