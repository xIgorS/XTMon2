USE [STAGING_FI_ALMT]
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

IF SCHEMA_ID(N'monitoring') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA [monitoring] AUTHORIZATION [dbo];');
END
GO

IF OBJECT_ID(N'[monitoring].[XtMonFunctionalRejectionDelayConfig]', N'U') IS NULL
BEGIN
    CREATE TABLE [monitoring].[XtMonFunctionalRejectionDelayConfig]
    (
        [SimulationKey] sysname NOT NULL,
        [IsEnabled] bit NOT NULL CONSTRAINT [DF_XtMonFunctionalRejectionDelayConfig_IsEnabled] DEFAULT ((0)),
        [DelayLiteral] char(8) NOT NULL,
        [UpdatedAtUtc] datetime2(3) NOT NULL CONSTRAINT [DF_XtMonFunctionalRejectionDelayConfig_UpdatedAtUtc] DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT [PK_XtMonFunctionalRejectionDelayConfig] PRIMARY KEY CLUSTERED ([SimulationKey])
    );
END;

MERGE [monitoring].[XtMonFunctionalRejectionDelayConfig] AS [target]
USING (VALUES (N'TechnicalReject', CONVERT(bit, 1), CONVERT(char(8), '00:02:00')))
    AS [source] ([SimulationKey], [IsEnabled], [DelayLiteral])
    ON [target].[SimulationKey] = [source].[SimulationKey]
WHEN MATCHED THEN
    UPDATE
       SET [IsEnabled] = [source].[IsEnabled],
           [DelayLiteral] = [source].[DelayLiteral],
           [UpdatedAtUtc] = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT ([SimulationKey], [IsEnabled], [DelayLiteral])
    VALUES ([source].[SimulationKey], [source].[IsEnabled], [source].[DelayLiteral]);
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspXtgMonitoringTechnicalRejectXtMonDelay]
    @PnlDate date,
    @businessdatatypeid int,
    @SourcesystemName varchar(50),
    @Execute bit,
    @Query nvarchar(max) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsEnabled bit = 0;
    DECLARE @DelayLiteral char(8) = '00:00:00';

    SELECT TOP (1)
        @IsEnabled = [cfg].[IsEnabled],
        @DelayLiteral = [cfg].[DelayLiteral]
    FROM [monitoring].[XtMonFunctionalRejectionDelayConfig] AS [cfg]
    WHERE [cfg].[SimulationKey] = N'TechnicalReject';

    IF @IsEnabled = 1
       AND ISNULL(@Execute, 1) = 1
       AND @DelayLiteral <> '00:00:00'
    BEGIN
        DECLARE @WaitCommand nvarchar(64) = N'WAITFOR DELAY ''' + CONVERT(nvarchar(8), @DelayLiteral) + N''';';
        EXEC (@WaitCommand);
    END

    EXEC [monitoring].[UspXtgMonitoringTechnicalReject]
        @PnlDate = @PnlDate,
        @businessdatatypeid = @businessdatatypeid,
        @SourcesystemName = @SourcesystemName,
        @Execute = @Execute,
        @Query = @Query OUTPUT;
END
GO

USE [DTM_FI]
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

IF SCHEMA_ID(N'monitoring') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA [monitoring] AUTHORIZATION [dbo];');
END
GO

IF OBJECT_ID(N'[monitoring].[XtMonFunctionalRejectionDelayConfig]', N'U') IS NULL
BEGIN
    CREATE TABLE [monitoring].[XtMonFunctionalRejectionDelayConfig]
    (
        [SimulationKey] sysname NOT NULL,
        [IsEnabled] bit NOT NULL CONSTRAINT [DF_XtMonFunctionalRejectionDelayConfig_IsEnabled] DEFAULT ((0)),
        [DelayLiteral] char(8) NOT NULL,
        [UpdatedAtUtc] datetime2(3) NOT NULL CONSTRAINT [DF_XtMonFunctionalRejectionDelayConfig_UpdatedAtUtc] DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT [PK_XtMonFunctionalRejectionDelayConfig] PRIMARY KEY CLUSTERED ([SimulationKey])
    );
END;

MERGE [monitoring].[XtMonFunctionalRejectionDelayConfig] AS [target]
USING (VALUES (N'TechnicalReject', CONVERT(bit, 1), CONVERT(char(8), '00:02:00')))
    AS [source] ([SimulationKey], [IsEnabled], [DelayLiteral])
    ON [target].[SimulationKey] = [source].[SimulationKey]
WHEN MATCHED THEN
    UPDATE
       SET [IsEnabled] = [source].[IsEnabled],
           [DelayLiteral] = [source].[DelayLiteral],
           [UpdatedAtUtc] = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT ([SimulationKey], [IsEnabled], [DelayLiteral])
    VALUES ([source].[SimulationKey], [source].[IsEnabled], [source].[DelayLiteral]);
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspXtgMonitoringTechnicalRejectXtMonDelay]
    @PnlDate date,
    @businessdatatypeid int,
    @SourcesystemName varchar(50),
    @Execute bit,
    @Query nvarchar(max) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsEnabled bit = 0;
    DECLARE @DelayLiteral char(8) = '00:00:00';

    SELECT TOP (1)
        @IsEnabled = [cfg].[IsEnabled],
        @DelayLiteral = [cfg].[DelayLiteral]
    FROM [monitoring].[XtMonFunctionalRejectionDelayConfig] AS [cfg]
    WHERE [cfg].[SimulationKey] = N'TechnicalReject';

    IF @IsEnabled = 1
       AND ISNULL(@Execute, 1) = 1
       AND @DelayLiteral <> '00:00:00'
    BEGIN
        DECLARE @WaitCommand nvarchar(64) = N'WAITFOR DELAY ''' + CONVERT(nvarchar(8), @DelayLiteral) + N''';';
        EXEC (@WaitCommand);
    END

    EXEC [monitoring].[UspXtgMonitoringTechnicalReject]
        @PnlDate = @PnlDate,
        @businessdatatypeid = @businessdatatypeid,
        @SourcesystemName = @SourcesystemName,
        @Execute = @Execute,
        @Query = @Query OUTPUT;
END
GO