using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public class UamAuthorizationOptions
{
    public const string SectionName = "UamAuthorization";

    [Required]
    public string ConnectionStringName { get; init; } = string.Empty;

    [Required]
    public string GetAdminUserStoredProcedure { get; init; } = string.Empty;

    public int CommandTimeoutSeconds { get; init; } = 30;
}
