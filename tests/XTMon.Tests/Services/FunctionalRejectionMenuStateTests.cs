using System.Reflection;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using XTMon.Models;
using XTMon.Repositories;
using XTMon.Services;

namespace XTMon.Tests.Services;

public class FunctionalRejectionMenuStateTests
{
    [Fact]
    public async Task RefreshAsync_LoadsItemsAndClearsMessages()
    {
        var expectedItems = new[]
        {
            CreateItem("FOCUS", 1, "FOCUS", "STAGING"),
            CreateItem("ALG", 2, "ALG", "DTM")
        };

        var repository = new Mock<IFunctionalRejectionRepository>();
        repository
            .Setup(value => value.GetMenuItemsAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedItems);

        var state = new FunctionalRejectionMenuState(
            repository.Object,
            NullLogger<FunctionalRejectionMenuState>.Instance);

        await state.RefreshAsync(CancellationToken.None);

        Assert.Equal(expectedItems, state.MenuItems);
        Assert.Null(state.ErrorMessage);
        Assert.Null(state.WarningMessage);
    }

    [Fact]
    public async Task RefreshAsync_WhenSqlFailureAfterSuccessfulLoad_PreservesCachedItemsAndBacksOff()
    {
        var expectedItems = new[]
        {
            CreateItem("FOCUS", 1, "FOCUS", "STAGING")
        };

        var repository = new Mock<IFunctionalRejectionRepository>();
        repository
            .SetupSequence(value => value.GetMenuItemsAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedItems)
            .ThrowsAsync(MakeSqlException(53, "A network-related or instance-specific error occurred while establishing a connection to SQL Server."));

        var state = new FunctionalRejectionMenuState(
            repository.Object,
            NullLogger<FunctionalRejectionMenuState>.Instance);

        await state.RefreshAsync(CancellationToken.None);
        await state.RefreshAsync(CancellationToken.None);
        await state.RefreshAsync(CancellationToken.None);

        Assert.Equal(expectedItems, state.MenuItems);
        Assert.Null(state.ErrorMessage);
        Assert.Equal("Showing previously loaded Functional Rejection items while live refresh is unavailable.", state.WarningMessage);
        repository.Verify(value => value.GetMenuItemsAsync(It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    [Fact]
    public async Task RefreshAsync_WhenSqlFailureWithoutCachedItems_SetsErrorAndBacksOff()
    {
        var repository = new Mock<IFunctionalRejectionRepository>();
        repository
            .Setup(value => value.GetMenuItemsAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(MakeSqlException(53, "A network-related or instance-specific error occurred while establishing a connection to SQL Server."));

        var state = new FunctionalRejectionMenuState(
            repository.Object,
            NullLogger<FunctionalRejectionMenuState>.Instance);

        await state.RefreshAsync(CancellationToken.None);
        await state.RefreshAsync(CancellationToken.None);

        Assert.Empty(state.MenuItems);
        Assert.Equal("Functional Rejection items are temporarily unavailable while the menu database connection recovers.", state.ErrorMessage);
        Assert.Null(state.WarningMessage);
        repository.Verify(value => value.GetMenuItemsAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task RefreshAsync_WhenDisposedDuringInflightRefresh_DoesNotThrow()
    {
        var refreshStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowRefreshToComplete = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var repository = new Mock<IFunctionalRejectionRepository>();
        repository
            .Setup(value => value.GetMenuItemsAsync(It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                refreshStarted.SetResult();
                await allowRefreshToComplete.Task;
                return Array.Empty<FunctionalRejectionMenuItem>();
            });

        var state = new FunctionalRejectionMenuState(
            repository.Object,
            NullLogger<FunctionalRejectionMenuState>.Instance);

        var refreshTask = state.RefreshAsync(CancellationToken.None);
        await refreshStarted.Task;

        state.Dispose();
        allowRefreshToComplete.SetResult();

        await refreshTask;
    }

    private static FunctionalRejectionMenuItem CreateItem(string code, int businessDataTypeId, string sourceSystemName, string dbConnection)
    {
        return new FunctionalRejectionMenuItem(code, businessDataTypeId, sourceSystemName, dbConnection);
    }

    private static SqlException MakeSqlException(int number, string message)
    {
        const BindingFlags nonPublicInstance = BindingFlags.Instance | BindingFlags.NonPublic;

        var errorCtor = typeof(SqlError).GetConstructors(nonPublicInstance)[0];
        var errorArgs = errorCtor.GetParameters()
            .Select(parameter => parameter.ParameterType == typeof(int) ? number
                : parameter.ParameterType == typeof(byte) ? (object)(byte)0
                : parameter.ParameterType == typeof(uint) ? (object)0u
                : parameter.ParameterType == typeof(Exception) ? new Exception(message)
                : parameter.ParameterType == typeof(string) ? message
                : parameter.ParameterType.IsValueType ? Activator.CreateInstance(parameter.ParameterType)!
                : null)
            .ToArray();
        var error = (SqlError)errorCtor.Invoke(errorArgs);

        var collectionCtor = typeof(SqlErrorCollection).GetConstructors(nonPublicInstance)[0];
        var errors = (SqlErrorCollection)collectionCtor.Invoke(Array.Empty<object>());
        var addMethod = typeof(SqlErrorCollection).GetMethod("Add", nonPublicInstance)!;
        addMethod.Invoke(errors, [error]);

        var exceptionCtor = typeof(SqlException).GetConstructors(nonPublicInstance)[0];
        var exceptionArgs = exceptionCtor.GetParameters()
            .Select(parameter => parameter.ParameterType == typeof(string) ? message
                : parameter.ParameterType == typeof(SqlErrorCollection) ? errors
                : parameter.ParameterType == typeof(Exception) ? new Exception(message)
                : parameter.ParameterType.IsValueType ? Activator.CreateInstance(parameter.ParameterType)!
                : null)
            .ToArray();

        return (SqlException)exceptionCtor.Invoke(exceptionArgs);
    }
}