USE [STAGING_FI_ALMT]
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

DECLARE @HarnessSeedRowCount int = 50000;
DECLARE @DefaultRowsToTouch int = 50000;
DECLARE @DefaultDelayLiteral char(8) = '00:02:00';
DECLARE @DefaultUpdatePasses int = 96;

IF @HarnessSeedRowCount < @DefaultRowsToTouch
BEGIN
    THROW 50000, 'HarnessSeedRowCount must be greater than or equal to DefaultRowsToTouch.', 1;
END;

IF OBJECT_ID(N'[monitoring].[HeavyRollbackSimulationConfig]', N'U') IS NULL
BEGIN
    CREATE TABLE [monitoring].[HeavyRollbackSimulationConfig]
    (
        [SimulationKey] sysname NOT NULL,
        [IsEnabled] bit NOT NULL CONSTRAINT [DF_HeavyRollbackSimulationConfig_IsEnabled] DEFAULT ((0)),
        [DelayLiteral] char(8) NOT NULL,
        [RowsToTouch] int NOT NULL,
        [UpdatePasses] int NOT NULL CONSTRAINT [DF_HeavyRollbackSimulationConfig_UpdatePasses] DEFAULT ((96)),
        [TriggerPnlDate] date NULL,
        [UpdatedAtUtc] datetime2(3) NOT NULL CONSTRAINT [DF_HeavyRollbackSimulationConfig_UpdatedAtUtc] DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT [PK_HeavyRollbackSimulationConfig] PRIMARY KEY CLUSTERED ([SimulationKey])
    );
END;

IF COL_LENGTH(N'[monitoring].[HeavyRollbackSimulationConfig]', N'UpdatePasses') IS NULL
BEGIN
    ALTER TABLE [monitoring].[HeavyRollbackSimulationConfig]
        ADD [UpdatePasses] int NULL;

    UPDATE [monitoring].[HeavyRollbackSimulationConfig]
    SET [UpdatePasses] = @DefaultUpdatePasses
    WHERE [UpdatePasses] IS NULL;

    ALTER TABLE [monitoring].[HeavyRollbackSimulationConfig]
        ALTER COLUMN [UpdatePasses] int NOT NULL;

    ALTER TABLE [monitoring].[HeavyRollbackSimulationConfig]
        ADD CONSTRAINT [DF_HeavyRollbackSimulationConfig_UpdatePasses] DEFAULT ((96)) FOR [UpdatePasses];
END;

IF OBJECT_ID(N'[monitoring].[DailyBalanceRollbackHarness]', N'U') IS NULL
BEGIN
    CREATE TABLE [monitoring].[DailyBalanceRollbackHarness]
    (
        [HarnessId] bigint NOT NULL IDENTITY(1, 1),
        [FlipBit] bit NOT NULL CONSTRAINT [DF_DailyBalanceRollbackHarness_FlipBit] DEFAULT ((0)),
        [TouchStamp] datetime2(3) NULL,
        [Payload] char(2000) NOT NULL CONSTRAINT [DF_DailyBalanceRollbackHarness_Payload] DEFAULT (REPLICATE('A', 2000)),
        [Payload2] char(2000) NOT NULL CONSTRAINT [DF_DailyBalanceRollbackHarness_Payload2] DEFAULT (REPLICATE('B', 2000)),
        [StressPayload] varchar(max) NULL,
        CONSTRAINT [PK_DailyBalanceRollbackHarness] PRIMARY KEY CLUSTERED ([HarnessId])
    );

    CREATE NONCLUSTERED INDEX [IX_DailyBalanceRollbackHarness_FlipBit_HarnessId]
        ON [monitoring].[DailyBalanceRollbackHarness] ([FlipBit], [HarnessId])
        INCLUDE ([TouchStamp]);
END;

IF COL_LENGTH(N'[monitoring].[DailyBalanceRollbackHarness]', N'StressPayload') IS NULL
BEGIN
    ALTER TABLE [monitoring].[DailyBalanceRollbackHarness]
        ADD [StressPayload] varchar(max) NULL;
END;

IF OBJECT_ID(N'[monitoring].[PricingRollbackHarness]', N'U') IS NULL
BEGIN
    CREATE TABLE [monitoring].[PricingRollbackHarness]
    (
        [HarnessId] bigint NOT NULL IDENTITY(1, 1),
        [FlipBit] bit NOT NULL CONSTRAINT [DF_PricingRollbackHarness_FlipBit] DEFAULT ((0)),
        [TouchStamp] datetime2(3) NULL,
        [Payload] char(2000) NOT NULL CONSTRAINT [DF_PricingRollbackHarness_Payload] DEFAULT (REPLICATE('C', 2000)),
        [Payload2] char(2000) NOT NULL CONSTRAINT [DF_PricingRollbackHarness_Payload2] DEFAULT (REPLICATE('D', 2000)),
        [StressPayload] varchar(max) NULL,
        CONSTRAINT [PK_PricingRollbackHarness] PRIMARY KEY CLUSTERED ([HarnessId])
    );

    CREATE NONCLUSTERED INDEX [IX_PricingRollbackHarness_FlipBit_HarnessId]
        ON [monitoring].[PricingRollbackHarness] ([FlipBit], [HarnessId])
        INCLUDE ([TouchStamp]);
END;

IF COL_LENGTH(N'[monitoring].[PricingRollbackHarness]', N'StressPayload') IS NULL
BEGIN
    ALTER TABLE [monitoring].[PricingRollbackHarness]
        ADD [StressPayload] varchar(max) NULL;
END;

DECLARE @DailyBalanceRowsMissing int = @HarnessSeedRowCount - (SELECT COUNT_BIG(*) FROM [monitoring].[DailyBalanceRollbackHarness]);
IF @DailyBalanceRowsMissing > 0
BEGIN
    ;WITH [SeedRows] AS
    (
        SELECT TOP (@DailyBalanceRowsMissing) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS [RowNumber]
        FROM sys.all_objects AS [a]
        CROSS JOIN sys.all_objects AS [b]
    )
    INSERT INTO [monitoring].[DailyBalanceRollbackHarness] ([FlipBit], [TouchStamp], [Payload], [Payload2])
    SELECT 0, NULL, REPLICATE('A', 2000), REPLICATE('B', 2000)
    FROM [SeedRows];
END;

DECLARE @PricingRowsMissing int = @HarnessSeedRowCount - (SELECT COUNT_BIG(*) FROM [monitoring].[PricingRollbackHarness]);
IF @PricingRowsMissing > 0
BEGIN
    ;WITH [SeedRows] AS
    (
        SELECT TOP (@PricingRowsMissing) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS [RowNumber]
        FROM sys.all_objects AS [a]
        CROSS JOIN sys.all_objects AS [b]
    )
    INSERT INTO [monitoring].[PricingRollbackHarness] ([FlipBit], [TouchStamp], [Payload], [Payload2])
    SELECT 0, NULL, REPLICATE('C', 2000), REPLICATE('D', 2000)
    FROM [SeedRows];
END;

IF NOT EXISTS
(
    SELECT 1
    FROM [monitoring].[HeavyRollbackSimulationConfig]
    WHERE [SimulationKey] = N'DailyBalance'
)
BEGIN
    INSERT INTO [monitoring].[HeavyRollbackSimulationConfig]
    (
        [SimulationKey],
        [IsEnabled],
        [DelayLiteral],
        [RowsToTouch],
        [UpdatePasses],
        [TriggerPnlDate]
    )
    VALUES
    (
        N'DailyBalance',
        1,
        @DefaultDelayLiteral,
        @DefaultRowsToTouch,
        @DefaultUpdatePasses,
        NULL
    );
END;

IF NOT EXISTS
(
    SELECT 1
    FROM [monitoring].[HeavyRollbackSimulationConfig]
    WHERE [SimulationKey] = N'Pricing'
)
BEGIN
    INSERT INTO [monitoring].[HeavyRollbackSimulationConfig]
    (
        [SimulationKey],
        [IsEnabled],
        [DelayLiteral],
        [RowsToTouch],
        [UpdatePasses],
        [TriggerPnlDate]
    )
    VALUES
    (
        N'Pricing',
        1,
        @DefaultDelayLiteral,
        @DefaultRowsToTouch,
        @DefaultUpdatePasses,
        NULL
    );
END;

UPDATE [cfg]
SET
    [cfg].[IsEnabled] = 1,
    [cfg].[DelayLiteral] = CASE
        WHEN [cfg].[DelayLiteral] < @DefaultDelayLiteral THEN @DefaultDelayLiteral
        ELSE [cfg].[DelayLiteral]
    END,
    [cfg].[RowsToTouch] = CASE
        WHEN [cfg].[RowsToTouch] < @DefaultRowsToTouch THEN @DefaultRowsToTouch
        ELSE [cfg].[RowsToTouch]
    END,
    [cfg].[UpdatePasses] = CASE
        WHEN [cfg].[UpdatePasses] < @DefaultUpdatePasses THEN @DefaultUpdatePasses
        ELSE [cfg].[UpdatePasses]
    END,
    [cfg].[UpdatedAtUtc] = SYSUTCDATETIME()
FROM [monitoring].[HeavyRollbackSimulationConfig] AS [cfg]
WHERE [cfg].[SimulationKey] IN (N'DailyBalance', N'Pricing');

DECLARE @Targets TABLE
(
    [ProcedureName] sysname NOT NULL,
    [SimulationKey] sysname NOT NULL,
    [HarnessTableName] sysname NOT NULL
);

INSERT INTO @Targets ([ProcedureName], [SimulationKey], [HarnessTableName])
VALUES
    (N'UspXtgMonitoringBalancesCalculation', N'DailyBalance', N'DailyBalanceRollbackHarness'),
    (N'UspXtgMonitoringPricingDaily', N'Pricing', N'PricingRollbackHarness');

DECLARE
    @ProcedureName sysname,
    @SimulationKey sysname,
    @HarnessTableName sysname,
    @ObjectName nvarchar(517),
    @Definition nvarchar(max),
    @Altered nvarchar(max),
    @UpperDefinition nvarchar(max),
    @Injection nvarchar(max),
    @InsertIndex int,
    @DelayMarkerIndex int,
    @WaitForIndex int,
    @SemicolonIndex int,
    @DelayRemovalLength int,
    @StartIndex int,
    @EndIndex int,
    @EndLineIndex int,
    @RemovalStart int,
    @RemovalLength int,
    @ErrorMessage nvarchar(2048);

DECLARE TargetCursor CURSOR LOCAL FAST_FORWARD FOR
SELECT [ProcedureName], [SimulationKey], [HarnessTableName]
FROM @Targets
ORDER BY [ProcedureName];

OPEN TargetCursor;

FETCH NEXT FROM TargetCursor INTO @ProcedureName, @SimulationKey, @HarnessTableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @ObjectName = QUOTENAME(N'monitoring') + N'.' + QUOTENAME(@ProcedureName);
    SET @Definition = OBJECT_DEFINITION(OBJECT_ID(@ObjectName));

    IF @Definition IS NULL
    BEGIN
        SET @ErrorMessage = N'Unable to load definition for ' + @ObjectName + N' from database ' + DB_NAME() + N'.';
        THROW 50001, @ErrorMessage, 1;
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

    IF @Altered LIKE N'%XTMon heavy rollback simulation start%'
    BEGIN
        SET @StartIndex = CHARINDEX(N'-- XTMon heavy rollback simulation start', @Altered);
        SET @EndIndex = CHARINDEX(N'-- XTMon heavy rollback simulation end', @Altered, @StartIndex);

        IF @StartIndex <= 0 OR @EndIndex <= 0
        BEGIN
            SET @ErrorMessage = N'Unable to locate the existing heavy rollback simulation markers for ' + @ObjectName + N'.';
            THROW 50004, @ErrorMessage, 1;
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
    END;

    SET @DelayMarkerIndex = CHARINDEX(N'-- XTMon WAITFOR delay injection', @Altered);
    IF @DelayMarkerIndex > 0
    BEGIN
        SET @WaitForIndex = CHARINDEX(N'WAITFOR DELAY', @Altered, @DelayMarkerIndex);
        SET @SemicolonIndex = CASE WHEN @WaitForIndex > 0 THEN CHARINDEX(N';', @Altered, @WaitForIndex) ELSE 0 END;

        IF @WaitForIndex <= 0 OR @SemicolonIndex <= 0
        BEGIN
            SET @ErrorMessage = N'Unable to locate the existing XTMon WAITFOR delay injection for ' + @ObjectName + N'.';
            THROW 50003, @ErrorMessage, 1;
        END;

        SET @DelayMarkerIndex = CASE
            WHEN @DelayMarkerIndex > 2 AND SUBSTRING(@Altered, @DelayMarkerIndex - 2, 2) = CHAR(13) + CHAR(10) THEN @DelayMarkerIndex - 2
            ELSE @DelayMarkerIndex
        END;

        SET @DelayRemovalLength = @SemicolonIndex - @DelayMarkerIndex + 1;
        IF SUBSTRING(@Altered, @SemicolonIndex + 1, 2) = CHAR(13) + CHAR(10)
        BEGIN
            SET @DelayRemovalLength += 2;
        END;

        SET @Altered = STUFF(@Altered, @DelayMarkerIndex, @DelayRemovalLength, N'');
    END;

    SET @UpperDefinition = UPPER(@Altered);
    SET @InsertIndex = CHARINDEX(N'SET NOCOUNT ON;', @UpperDefinition);

    IF @InsertIndex > 0
    BEGIN
        SET @InsertIndex += LEN(N'SET NOCOUNT ON;');
    END;
    ELSE
    BEGIN
        SET @InsertIndex = CHARINDEX(N'SET NOCOUNT ON', @UpperDefinition);

        IF @InsertIndex > 0
        BEGIN
            SET @InsertIndex += LEN(N'SET NOCOUNT ON');
        END;
        ELSE
        BEGIN
            SET @InsertIndex = CHARINDEX(N'BEGIN', @UpperDefinition);

            IF @InsertIndex <= 0
            BEGIN
                SET @ErrorMessage = N'Unable to locate SET NOCOUNT ON or BEGIN for ' + @ObjectName + N'.';
                THROW 50002, @ErrorMessage, 1;
            END;

            SET @InsertIndex += LEN(N'BEGIN');
        END;
    END;

    SET @Injection = CHAR(13) + CHAR(10)
        + N'    -- XTMon heavy rollback simulation start' + CHAR(13) + CHAR(10)
        + N'    DECLARE @XtMonSimulationEnabled bit = 0;' + CHAR(13) + CHAR(10)
        + N'    DECLARE @XtMonDelayLiteral char(8) = ''00:00:00'';' + CHAR(13) + CHAR(10)
        + N'    DECLARE @XtMonRowsToTouch int = 0;' + CHAR(13) + CHAR(10)
        + N'    DECLARE @XtMonUpdatePasses int = 0;' + CHAR(13) + CHAR(10)
        + N'    DECLARE @XtMonTriggerPnlDate date = NULL;' + CHAR(13) + CHAR(10)
        + N'' + CHAR(13) + CHAR(10)
        + N'    SELECT' + CHAR(13) + CHAR(10)
        + N'        @XtMonSimulationEnabled = [cfg].[IsEnabled],' + CHAR(13) + CHAR(10)
        + N'        @XtMonDelayLiteral = [cfg].[DelayLiteral],' + CHAR(13) + CHAR(10)
        + N'        @XtMonRowsToTouch = [cfg].[RowsToTouch],' + CHAR(13) + CHAR(10)
        + N'        @XtMonUpdatePasses = [cfg].[UpdatePasses],' + CHAR(13) + CHAR(10)
        + N'        @XtMonTriggerPnlDate = [cfg].[TriggerPnlDate]' + CHAR(13) + CHAR(10)
        + N'    FROM [monitoring].[HeavyRollbackSimulationConfig] AS [cfg]' + CHAR(13) + CHAR(10)
        + N'    WHERE [cfg].[SimulationKey] = N''__SIMKEY__'';' + CHAR(13) + CHAR(10)
        + N'' + CHAR(13) + CHAR(10)
        + N'    IF ISNULL(@XtMonSimulationEnabled, 0) = 1' + CHAR(13) + CHAR(10)
        + N'       AND ISNULL(@Execute, 1) = 1' + CHAR(13) + CHAR(10)
        + N'       AND (@XtMonTriggerPnlDate IS NULL OR @XtMonTriggerPnlDate = @PnlDate)' + CHAR(13) + CHAR(10)
        + N'       AND ISNULL(@XtMonRowsToTouch, 0) > 0' + CHAR(13) + CHAR(10)
        + N'       AND ISNULL(@XtMonUpdatePasses, 0) > 0' + CHAR(13) + CHAR(10)
        + N'    BEGIN' + CHAR(13) + CHAR(10)
        + N'        BEGIN TRANSACTION;' + CHAR(13) + CHAR(10)
        + N'' + CHAR(13) + CHAR(10)
        + N'        CREATE TABLE #XtMonTargetRows' + CHAR(13) + CHAR(10)
        + N'        (' + CHAR(13) + CHAR(10)
        + N'            [HarnessId] bigint NOT NULL PRIMARY KEY CLUSTERED' + CHAR(13) + CHAR(10)
        + N'        );' + CHAR(13) + CHAR(10)
        + N'' + CHAR(13) + CHAR(10)
        + N'        INSERT INTO #XtMonTargetRows ([HarnessId])' + CHAR(13) + CHAR(10)
        + N'        SELECT TOP (@XtMonRowsToTouch) [h].[HarnessId]' + CHAR(13) + CHAR(10)
        + N'        FROM [monitoring].[__HARNESS__] AS [h] WITH (UPDLOCK, HOLDLOCK)' + CHAR(13) + CHAR(10)
        + N'        ORDER BY [h].[HarnessId];' + CHAR(13) + CHAR(10)
        + N'' + CHAR(13) + CHAR(10)
        + N'        DECLARE @XtMonPassNumber int = 0;' + CHAR(13) + CHAR(10)
        + N'' + CHAR(13) + CHAR(10)
        + N'        WHILE @XtMonPassNumber < @XtMonUpdatePasses' + CHAR(13) + CHAR(10)
        + N'        BEGIN' + CHAR(13) + CHAR(10)
        + N'            SET @XtMonPassNumber += 1;' + CHAR(13) + CHAR(10)
        + N'' + CHAR(13) + CHAR(10)
        + N'            UPDATE [h]' + CHAR(13) + CHAR(10)
        + N'            SET' + CHAR(13) + CHAR(10)
        + N'                [FlipBit] = CASE WHEN @XtMonPassNumber % 2 = 1 THEN 1 ELSE 0 END,' + CHAR(13) + CHAR(10)
        + N'                [TouchStamp] = DATEADD(MILLISECOND, @XtMonPassNumber, SYSUTCDATETIME()),' + CHAR(13) + CHAR(10)
        + N'                [Payload] = REPLICATE(CASE WHEN @XtMonPassNumber % 3 = 1 THEN ''Z'' WHEN @XtMonPassNumber % 3 = 2 THEN ''Q'' ELSE ''A'' END, 2000),' + CHAR(13) + CHAR(10)
        + N'                [Payload2] = REPLICATE(CASE WHEN @XtMonPassNumber % 3 = 1 THEN ''Y'' WHEN @XtMonPassNumber % 3 = 2 THEN ''R'' ELSE ''B'' END, 2000),' + CHAR(13) + CHAR(10)
        + N'                [StressPayload] = REPLICATE(CAST(CASE WHEN @XtMonPassNumber % 3 = 1 THEN ''M'' WHEN @XtMonPassNumber % 3 = 2 THEN ''N'' ELSE ''P'' END AS varchar(max)), 16000)' + CHAR(13) + CHAR(10)
        + N'            FROM [monitoring].[__HARNESS__] AS [h]' + CHAR(13) + CHAR(10)
        + N'            INNER JOIN #XtMonTargetRows AS [t] ON [t].[HarnessId] = [h].[HarnessId];' + CHAR(13) + CHAR(10)
        + N'        END' + CHAR(13) + CHAR(10)
        + N'' + CHAR(13) + CHAR(10)
        + N'        DECLARE @XtMonWaitCommand nvarchar(64) = N''WAITFOR DELAY '''''' + @XtMonDelayLiteral + N'''''';'';' + CHAR(13) + CHAR(10)
        + N'        EXEC sys.sp_executesql @XtMonWaitCommand;' + CHAR(13) + CHAR(10)
        + N'' + CHAR(13) + CHAR(10)
        + N'        COMMIT TRANSACTION;' + CHAR(13) + CHAR(10)
        + N'' + CHAR(13) + CHAR(10)
        + N'        SET @Query = N''SELECT CAST(''''XTMon heavy rollback simulation completed.'''' AS nvarchar(128)) AS [SimulationStatus], CAST(''''__SIMKEY__'''' AS sysname) AS [SimulationKey], CAST(''''completed'''' AS nvarchar(16)) AS [SimulationResult];'';' + CHAR(13) + CHAR(10)
        + N'        SELECT' + CHAR(13) + CHAR(10)
        + N'            CAST(N''XTMon heavy rollback simulation completed.'' AS nvarchar(128)) AS [SimulationStatus],' + CHAR(13) + CHAR(10)
        + N'            CAST(N''__SIMKEY__'' AS sysname) AS [SimulationKey],' + CHAR(13) + CHAR(10)
        + N'            CAST(N''completed'' AS nvarchar(16)) AS [SimulationResult],' + CHAR(13) + CHAR(10)
        + N'            @PnlDate AS [PnlDate],' + CHAR(13) + CHAR(10)
        + N'            @XtMonRowsToTouch AS [RowsTouched],' + CHAR(13) + CHAR(10)
        + N'            @XtMonUpdatePasses AS [UpdatePasses],' + CHAR(13) + CHAR(10)
        + N'            @XtMonDelayLiteral AS [DelayLiteral];' + CHAR(13) + CHAR(10)
        + N'' + CHAR(13) + CHAR(10)
        + N'        RETURN;' + CHAR(13) + CHAR(10)
        + N'    END' + CHAR(13) + CHAR(10)
        + N'    -- XTMon heavy rollback simulation end';

    SET @Injection = REPLACE(@Injection, N'__SIMKEY__', @SimulationKey);
    SET @Injection = REPLACE(@Injection, N'__HARNESS__', @HarnessTableName);

    SET @Altered = STUFF(@Altered, @InsertIndex, 0, @Injection);

    EXEC sys.sp_executesql @Altered;

    PRINT N'Injected heavy rollback simulation into ' + @ObjectName + N'.';

    FETCH NEXT FROM TargetCursor INTO @ProcedureName, @SimulationKey, @HarnessTableName;
END;

CLOSE TargetCursor;
DEALLOCATE TargetCursor;

SELECT
    [SimulationKey],
    [IsEnabled],
    [DelayLiteral],
    [RowsToTouch],
    [UpdatePasses],
    [TriggerPnlDate],
    [UpdatedAtUtc]
FROM [monitoring].[HeavyRollbackSimulationConfig]
ORDER BY [SimulationKey];
GO