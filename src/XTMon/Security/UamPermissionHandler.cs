using Microsoft.AspNetCore.Authorization;
using XTMon.Repositories;

namespace XTMon.Security;

public sealed class UamPermissionHandler(
    IUamAuthorizationRepository repository,
    ILogger<UamPermissionHandler> logger,
    AuthorizationFeedbackState feedbackState)
    : AuthorizationHandler<RequiresUamPermissionRequirement>
{
    protected override async Task HandleRequirementAsync(
        AuthorizationHandlerContext context, 
        RequiresUamPermissionRequirement requirement)
    {
        feedbackState.Clear();

        if (context.User.Identity?.IsAuthenticated != true || string.IsNullOrWhiteSpace(context.User.Identity.Name))
        {
            logger.LogWarning("UamPermissionHandler: User identity is not authenticated or name is missing.");
            return;
        }

        var username = context.User.Identity.Name;
        logger.LogInformation("UamPermissionHandler: Checking UAM authorization for user {Username}", username);

        try
        {
            var isAuthorized = await repository.IsUserAuthorizedAsync(username);

            if (isAuthorized)
            {
                logger.LogInformation("UamPermissionHandler: User {Username} is strictly authorized.", username);
                context.Succeed(requirement);
            }
            else
            {
                logger.LogWarning("UamPermissionHandler: User {Username} failed UAM authorization check.", username);
            }
        }
        catch (Exception ex)
        {
            feedbackState.SetInfrastructureFailure("The authorization service is currently unavailable. Please try again later or contact support if the problem persists.");
            logger.LogError(ex, "UamPermissionHandler: Error checking UAM authorization for {Username}.", username);
            // Don't succeed the requirement on error
        }
    }
}
