using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using System.Security.Claims;
using XTMon.Data;
using XTMon.Security;

namespace XTMon.Tests.Security;

public class UamPermissionHandlerTests
{
    private static UamPermissionHandler CreateHandler(IUamAuthorizationRepository repository) =>
        new UamPermissionHandler(repository, NullLogger<UamPermissionHandler>.Instance);

    private static AuthorizationHandlerContext CreateContext(
        string? identityName,
        bool isAuthenticated = true)
    {
        ClaimsIdentity identity;
        if (isAuthenticated && identityName is not null)
        {
            identity = new ClaimsIdentity(
                new[] { new Claim(ClaimTypes.Name, identityName) },
                authenticationType: "Negotiate");
        }
        else
        {
            identity = new ClaimsIdentity(); // unauthenticated
        }

        var user = new ClaimsPrincipal(identity);
        var requirements = new[] { new RequiresUamPermissionRequirement() };
        return new AuthorizationHandlerContext(requirements, user, null);
    }

    [Fact]
    public async Task WhenUserIsAuthorized_SucceedsRequirement()
    {
        var repo = new Mock<IUamAuthorizationRepository>();
        repo.Setup(r => r.IsUserAuthorizedAsync("DOMAIN\\alice", It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var handler = CreateHandler(repo.Object);
        var context = CreateContext("DOMAIN\\alice");

        await handler.HandleAsync(context);

        Assert.True(context.HasSucceeded);
    }

    [Fact]
    public async Task WhenUserIsNotAuthorized_DoesNotSucceed()
    {
        var repo = new Mock<IUamAuthorizationRepository>();
        repo.Setup(r => r.IsUserAuthorizedAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        var handler = CreateHandler(repo.Object);
        var context = CreateContext("DOMAIN\\bob");

        await handler.HandleAsync(context);

        Assert.False(context.HasSucceeded);
    }

    [Fact]
    public async Task WhenUserIsNotAuthenticated_DoesNotSucceed()
    {
        var repo = new Mock<IUamAuthorizationRepository>();

        var handler = CreateHandler(repo.Object);
        var context = CreateContext(null, isAuthenticated: false);

        await handler.HandleAsync(context);

        Assert.False(context.HasSucceeded);
        repo.Verify(r => r.IsUserAuthorizedAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task WhenIdentityNameIsEmpty_DoesNotSucceed()
    {
        var repo = new Mock<IUamAuthorizationRepository>();

        var handler = CreateHandler(repo.Object);
        // Build a context with an authenticated identity but blank name
        var identity = new ClaimsIdentity(
            new[] { new Claim(ClaimTypes.Name, "   ") },
            authenticationType: "Negotiate");
        var user = new ClaimsPrincipal(identity);
        var context = new AuthorizationHandlerContext(
            new[] { new RequiresUamPermissionRequirement() }, user, null);

        await handler.HandleAsync(context);

        Assert.False(context.HasSucceeded);
        repo.Verify(r => r.IsUserAuthorizedAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task WhenRepositoryThrows_DoesNotSucceedAndDoesNotRethrow()
    {
        var repo = new Mock<IUamAuthorizationRepository>();
        repo.Setup(r => r.IsUserAuthorizedAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("DB unavailable"));

        var handler = CreateHandler(repo.Object);
        var context = CreateContext("DOMAIN\\carol");

        // Should not throw
        await handler.HandleAsync(context);

        Assert.False(context.HasSucceeded);
    }
}
