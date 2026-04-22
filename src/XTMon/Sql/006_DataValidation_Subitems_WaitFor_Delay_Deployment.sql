SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @DelayMinutes int = 1;

IF @DelayMinutes NOT BETWEEN 1 AND 10
BEGIN
    ;THROW 50000, 'DelayMinutes must be between 1 and 10.', 1;
END;

DECLARE @DelayLiteral char(8) = CONCAT('00:', RIGHT(CONCAT('0', CONVERT(varchar(2), @DelayMinutes)), 2), ':00');

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
DECLARE @HasExecuteParameter bit = CASE
    WHEN EXISTS
    (
        SELECT 1
        FROM sys.parameters
        WHERE object_id = OBJECT_ID(@ObjectName)
          AND name = N''@Execute''
    ) THEN 1 ELSE 0 END;
DECLARE @ErrorMessage nvarchar(2048);

IF @Definition IS NULL
BEGIN
    SET @ErrorMessage = N''Unable to load definition for '' + @ObjectName + N'' from database '' + DB_NAME() + N''.'';
    ;THROW 50001, @ErrorMessage, 1;
END;

IF @Definition LIKE N''%XTMon WAITFOR delay injection%''
BEGIN
    PRINT N''Skipping '' + DB_NAME() + N''.'' + @ObjectName + N'' because delay injection already exists.'';
    RETURN;
END;

DECLARE @Altered nvarchar(max) = @Definition;
DECLARE @UpperDefinition nvarchar(max);
DECLARE @AsIndex int;
DECLARE @BeginIndex int;
DECLARE @Injection nvarchar(max);

SET @Altered = REPLACE(@Altered, N''CREATE PROCEDURE'', N''ALTER PROCEDURE'');
SET @Altered = REPLACE(@Altered, N''create procedure'', N''ALTER PROCEDURE'');
SET @Altered = REPLACE(@Altered, N''CREATE PROC'', N''ALTER PROC'');
SET @Altered = REPLACE(@Altered, N''create proc'', N''ALTER PROC'');

SET @UpperDefinition = UPPER(@Altered);
SET @AsIndex = CHARINDEX(N''AS'', @UpperDefinition);
SET @BeginIndex = CASE WHEN @AsIndex > 0 THEN CHARINDEX(N''BEGIN'', @UpperDefinition, @AsIndex) ELSE 0 END;

IF @BeginIndex <= 0
BEGIN
    SET @ErrorMessage = N''Unable to locate BEGIN block for '' + @ObjectName + N'' in database '' + DB_NAME() + N''.'';
    ;THROW 50002, @ErrorMessage, 1;
END;

SET @Injection = CHAR(13) + CHAR(10)
    + N''    -- XTMon WAITFOR delay injection'' + CHAR(13) + CHAR(10)
    + CASE
        WHEN @HasExecuteParameter = 1 THEN N''    IF ISNULL(@Execute, 1) = 1'' + CHAR(13) + CHAR(10) + N''        WAITFOR DELAY '''''' + @DelayLiteral + N'''''';'' + CHAR(13) + CHAR(10)
        ELSE N''    WAITFOR DELAY '''''' + @DelayLiteral + N'''''';'' + CHAR(13) + CHAR(10)
      END;

SET @Altered = STUFF(@Altered, @BeginIndex + LEN(N''BEGIN''), 0, @Injection);

EXEC sys.sp_executesql @Altered;

PRINT N''Applied delay '' + @DelayLiteral + N'' to '' + DB_NAME() + N''.'' + @ObjectName + N''.'';
';

    EXEC sys.sp_executesql
        @Command,
        N'@DelayLiteral char(8)',
        @DelayLiteral = @DelayLiteral;

    FETCH NEXT FROM TargetCursor INTO @DatabaseName, @SchemaName, @ProcedureName;
END;

CLOSE TargetCursor;
DEALLOCATE TargetCursor;