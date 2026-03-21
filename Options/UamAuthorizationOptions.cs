using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class UamAuthorizationOptions
{
    public const string SectionName = "UamAuthorization";

    [Required]
    public string ConnectionStringName { get; set; } = string.Empty;

    [Required]
    public string GetAdminUserStoredProcedure { get; set; } = string.Empty;

    public int CommandTimeoutSeconds { get; set; } = 30;
}
