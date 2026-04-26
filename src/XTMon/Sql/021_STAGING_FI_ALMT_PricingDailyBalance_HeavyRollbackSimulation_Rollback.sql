USE [STAGING_FI_ALMT]
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

DECLARE @Targets TABLE
(
    [ProcedureName] sysname NOT NULL
);

INSERT INTO @Targets ([ProcedureName])
VALUES
    (N'UspXtgMonitoringBalancesCalculation'),
    (N'UspXtgMonitoringPricingDaily');

DECLARE
    @ProcedureName sysname,
    @ObjectName nvarchar(517),
    @Definition nvarchar(max),
    @Altered nvarchar(max),
    @StartIndex int,
    @EndIndex int,
    @EndLineIndex int,
    @RemovalStart int,
    @RemovalLength int,
    @ErrorMessage nvarchar(2048);

DECLARE TargetCursor CURSOR LOCAL FAST_FORWARD FOR
SELECT [ProcedureName]
FROM @Targets
ORDER BY [ProcedureName];

OPEN TargetCursor;

FETCH NEXT FROM TargetCursor INTO @ProcedureName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @ObjectName = QUOTENAME(N'monitoring') + N'.' + QUOTENAME(@ProcedureName);
    SET @Definition = OBJECT_DEFINITION(OBJECT_ID(@ObjectName));

    IF @Definition IS NULL
    BEGIN
        SET @ErrorMessage = N'Unable to load definition for ' + @ObjectName + N' from database ' + DB_NAME() + N'.';
        THROW 50011, @ErrorMessage, 1;
    END;

    IF @Definition NOT LIKE N'%XTMon heavy rollback simulation start%'
    BEGIN
        PRINT N'Skipping ' + @ObjectName + N' because heavy rollback simulation was not found.';
        FETCH NEXT FROM TargetCursor INTO @ProcedureName;
        CONTINUE;
    END;

    SET @Altered = @Definition;

    SET @Altered = REPLACE(
        @Altered,
        N'CREATE PROCEDURE [monitoring].[' + @ProcedureName + N']',
        N'ALTER PROCEDURE [monitoring].[' + @ProcedureName + N']');
    SET @Altered = REPLACE(
        @Altered,
        N'CREATE   PROCEDURE [monitoring].[' + @ProcedureName + N']',
        N'ALTER PROCEDURE [monitoring].[' + @ProcedureName + N']');
    SET @Altered = REPLACE(
        @Altered,
        N'CREATE OR ALTER PROCEDURE [monitoring].[' + @ProcedureName + N']',
        N'ALTER PROCEDURE [monitoring].[' + @ProcedureName + N']');

    SET @StartIndex = CHARINDEX(N'-- XTMon heavy rollback simulation start', @Altered);
    SET @EndIndex = CHARINDEX(N'-- XTMon heavy rollback simulation end', @Altered, @StartIndex);

    IF @StartIndex <= 0 OR @EndIndex <= 0
    BEGIN
        SET @ErrorMessage = N'Unable to locate rollback simulation markers for ' + @ObjectName + N'.';
        THROW 50012, @ErrorMessage, 1;
    END;

    SET @RemovalStart = CASE
        WHEN @StartIndex > 2 AND SUBSTRING(@Altered, @StartIndex - 2, 2) = CHAR(13) + CHAR(10) THEN @StartIndex - 2
        ELSE @StartIndex
    END;

    SET @EndLineIndex = CHARINDEX(CHAR(10), @Altered, @EndIndex);
    IF @EndLineIndex <= 0
    BEGIN
        SET @EndLineIndex = LEN(@Altered) + 1;
    END;

    SET @RemovalLength = @EndLineIndex - @RemovalStart + 1;

    SET @Altered = STUFF(@Altered, @RemovalStart, @RemovalLength, N'');

    EXEC sys.sp_executesql @Altered;

    PRINT N'Removed heavy rollback simulation from ' + @ObjectName + N'.';

    FETCH NEXT FROM TargetCursor INTO @ProcedureName;
END;

CLOSE TargetCursor;
DEALLOCATE TargetCursor;

DROP TABLE IF EXISTS [monitoring].[PricingRollbackHarness];
DROP TABLE IF EXISTS [monitoring].[DailyBalanceRollbackHarness];
DROP TABLE IF EXISTS [monitoring].[HeavyRollbackSimulationConfig];
GO