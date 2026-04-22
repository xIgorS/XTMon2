SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @Targets TABLE
(
    DatabaseName sysname NOT NULL,
    SchemaName sysname NOT NULL,
    ProcedureName sysname NOT NULL
);

INSERT INTO @Targets (DatabaseName, SchemaName, ProcedureName)
VALUES
    ('STAGING_FI_ALMT', 'administration', 'UspCheckBatchStatus'),
    ('STAGING_FI_ALMT', 'monitoring', 'UspXtgMonitoringCheckReferentiel'),
    ('DTM_FI', 'monitoring', 'UspXtgMonitoringMarketData'),
    ('STAGING_FI_ALMT', 'monitoring', 'UspXtgMonitoringPricingReception'),
    ('STAGING_FI_ALMT', 'monitoring', 'UspXtgMonitoringOutOfScope'),
    ('DTM_FI', 'monitoring', 'UspXtgMonitoringMissingSOG'),
    ('STAGING_FI_ALMT', 'monitoring', 'UspGetAdjNotlink'),
    ('LOG_FI_ALMT', 'monitoring', 'UspGetColStoreStatus'),
    ('STAGING_FI_ALMT', 'monitoring', 'UspXtgMonitoringTradingVsFivr'),
    ('STAGING_FI_ALMT', 'monitoring', 'UspXtgMonitoringCarrySpread'),
    ('STAGING_FI_ALMT', 'monitoring', 'UspXtgMonitoringResultTransfer'),
    ('DTM_FI', 'monitoring', 'UspXtgMonitoringPTFRolled'),
    ('STAGING_FI_ALMT', 'monitoring', 'UspXtgMonitoringSAS'),
    ('STAGING_FI_ALMT', 'monitoring', 'UspXtgMonitoringPortfolioFlaggedXTG'),
    ('STAGING_FI_ALMT', 'monitoring', 'UspXtgMonitoringPortfolioXTGRejected'),
    ('STAGING_FI_ALMT', 'monitoring', 'UspXtgMonitoringCheckFeedOutExtraction'),
    ('STAGING_FI_ALMT', 'monitoring', 'UspXtgMonitoringCheckFutureCash'),
    ('STAGING_FI_ALMT', 'monitoring', 'UspXtgMonitoringCheckConsistencyEvent'),
    ('STAGING_FI_ALMT', 'monitoring', 'UspXtgMonitoringMultipleFeedVersion'),
    ('STAGING_FI_ALMT', 'monitoring', 'UspXtgMonitoringBalancesCalculation'),
    ('STAGING_FI_ALMT', 'monitoring', 'UspXtgMonitoringAdjustments'),
    ('STAGING_FI_ALMT', 'monitoring', 'UspXtgMonitoringPricingDaily'),
    ('STAGING_FI_ALMT', 'monitoring', 'UspXtgMTRevConWorkflowCheck'),
    ('STAGING_FI_ALMT', 'monitoring', 'UspGetPublicationConsistency'),
    ('Publication', 'monitoring', 'UspCheckJvBalanceBetweenTwoDates'),
    ('Publication', 'monitoring', 'UspCheckIfPortfolioCreatedInWorkflow'),
    ('STAGING_FI_ALMT', 'core_process', 'UspCheckIfTableCreatedAfterPreCalc'),
    ('STAGING_FI_ALMT', 'monitoring', 'UspGetVRDBPricingReceptionStatus');

DECLARE
    @DatabaseName sysname,
    @SchemaName sysname,
    @ProcedureName sysname,
    @Command nvarchar(max);

DECLARE TargetCursor CURSOR LOCAL FAST_FORWARD FOR
SELECT DatabaseName, SchemaName, ProcedureName
FROM @Targets
ORDER BY DatabaseName, SchemaName, ProcedureName;

OPEN TargetCursor;

FETCH NEXT FROM TargetCursor INTO @DatabaseName, @SchemaName, @ProcedureName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @Command = N'
USE ' + QUOTENAME(@DatabaseName) + N';

DECLARE @ObjectName nvarchar(517) = N''[' + REPLACE(@SchemaName, '''', '''''') + N'].[' + REPLACE(@ProcedureName, '''', '''''') + N']'';
DECLARE @Definition nvarchar(max) = OBJECT_DEFINITION(OBJECT_ID(@ObjectName));
DECLARE @ErrorMessage nvarchar(2048);

IF @Definition IS NULL
BEGIN
    SET @ErrorMessage = N''Unable to load definition for '' + @ObjectName + N'' from database '' + DB_NAME() + N''.'';
    ;THROW 50011, @ErrorMessage, 1;
END;

IF @Definition NOT LIKE N''%XTMon WAITFOR delay injection%''
BEGIN
    PRINT N''Skipping '' + DB_NAME() + N''.'' + @ObjectName + N'' because delay injection was not found.'';
    RETURN;
END;

DECLARE @Altered nvarchar(max) = @Definition;
DECLARE @Marker nvarchar(200) = CHAR(13) + CHAR(10) + N''    -- XTMon WAITFOR delay injection'';
DECLARE @MarkerIndex int;
DECLARE @WaitForIndex int;
DECLARE @SemicolonIndex int;
DECLARE @RemovalLength int;

SET @Altered = REPLACE(@Altered, N''CREATE PROCEDURE'', N''ALTER PROCEDURE'');
SET @Altered = REPLACE(@Altered, N''create procedure'', N''ALTER PROCEDURE'');
SET @Altered = REPLACE(@Altered, N''CREATE PROC'', N''ALTER PROC'');
SET @Altered = REPLACE(@Altered, N''create proc'', N''ALTER PROC'');

SET @MarkerIndex = CHARINDEX(@Marker, @Altered);

IF @MarkerIndex <= 0
BEGIN
    SET @ErrorMessage = N''Unable to locate rollback marker for '' + @ObjectName + N'' in database '' + DB_NAME() + N''.'';
    ;THROW 50012, @ErrorMessage, 1;
END;

SET @WaitForIndex = CHARINDEX(N''WAITFOR DELAY'', @Altered, @MarkerIndex);

IF @WaitForIndex <= 0
BEGIN
    SET @ErrorMessage = N''Unable to locate WAITFOR DELAY after rollback marker for '' + @ObjectName + N'' in database '' + DB_NAME() + N''.'';
    ;THROW 50013, @ErrorMessage, 1;
END;

SET @SemicolonIndex = CHARINDEX(N'';'', @Altered, @WaitForIndex);

IF @SemicolonIndex <= 0
BEGIN
    SET @ErrorMessage = N''Unable to locate the end of the injected WAITFOR statement for '' + @ObjectName + N'' in database '' + DB_NAME() + N''.'';
    ;THROW 50014, @ErrorMessage, 1;
END;

SET @RemovalLength = @SemicolonIndex - @MarkerIndex + 1;

IF SUBSTRING(@Altered, @SemicolonIndex + 1, 2) = CHAR(13) + CHAR(10)
BEGIN
    SET @RemovalLength += 2;
END;

SET @Altered = STUFF(@Altered, @MarkerIndex, @RemovalLength, N'''');

EXEC sys.sp_executesql @Altered;

PRINT N''Removed delay injection from '' + DB_NAME() + N''.'' + @ObjectName + N''.'';
';

    EXEC sys.sp_executesql @Command;

    FETCH NEXT FROM TargetCursor INTO @DatabaseName, @SchemaName, @ProcedureName;
END;

CLOSE TargetCursor;
DEALLOCATE TargetCursor;