USE [LOG_FI_ALMT]
GO

IF EXISTS
(
    SELECT 1
    FROM [sys].[columns] AS [col]
    INNER JOIN [sys].[tables] AS [tbl] ON [tbl].[object_id] = [col].[object_id]
    INNER JOIN [sys].[schemas] AS [sch] ON [sch].[schema_id] = [tbl].[schema_id]
    INNER JOIN [sys].[types] AS [typ] ON [typ].[user_type_id] = [col].[user_type_id]
    WHERE [sch].[name] = N'monitoring'
      AND [tbl].[name] = N'APSActionsLogs'
      AND [col].[name] = N'TimeStamp'
      AND ([typ].[name] <> N'datetime2' OR [col].[scale] <> 3)
)
BEGIN
    ALTER TABLE [monitoring].[APSActionsLogs]
        ALTER COLUMN [TimeStamp] DATETIME2(3) NULL;
END
GO

IF NOT EXISTS
(
    SELECT 1
    FROM [sys].[indexes] AS [idx]
    INNER JOIN [sys].[tables] AS [tbl] ON [tbl].[object_id] = [idx].[object_id]
    INNER JOIN [sys].[schemas] AS [sch] ON [sch].[schema_id] = [tbl].[schema_id]
    WHERE [sch].[name] = N'monitoring'
      AND [tbl].[name] = N'APSActionsLogs'
      AND [idx].[name] = N'IX_APSActionsLogs_TimeStamp_Level'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_APSActionsLogs_TimeStamp_Level]
        ON [monitoring].[APSActionsLogs] ([TimeStamp] DESC)
        INCLUDE ([Level]);
END
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspGetApplicationLogs]
    @TopN INT = 200,
    @FromTimeStamp DATETIME2(3) = NULL,
    @ToTimeStamp DATETIME2(3) = NULL,
    @LevelsCsv NVARCHAR(256) = NULL,
    @MessageContains NVARCHAR(400) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @TopN IS NULL OR @TopN < 1 SET @TopN = 200;
    IF @TopN > 10000 SET @TopN = 10000;

    SET @LevelsCsv = NULLIF(LTRIM(RTRIM(@LevelsCsv)), N'');
    SET @MessageContains = NULLIF(LTRIM(RTRIM(@MessageContains)), N'');

    DECLARE @Levels TABLE
    (
        [Level] NVARCHAR(32) NOT NULL PRIMARY KEY
    );

    IF @LevelsCsv IS NOT NULL
    BEGIN
        INSERT INTO @Levels ([Level])
        SELECT DISTINCT LTRIM(RTRIM([value]))
        FROM STRING_SPLIT(@LevelsCsv, N',')
        WHERE LTRIM(RTRIM([value])) <> N'';
    END

    SELECT TOP (@TopN)
        [Id],
        [TimeStamp],
        [Level],
        [Message],
        [Exception],
        [Properties]
    FROM [monitoring].[APSActionsLogs] WITH (NOLOCK)
    WHERE (@FromTimeStamp IS NULL OR [TimeStamp] >= @FromTimeStamp)
      AND (@ToTimeStamp IS NULL OR [TimeStamp] <= @ToTimeStamp)
      AND (NOT EXISTS (SELECT 1 FROM @Levels) OR EXISTS (SELECT 1 FROM @Levels AS [levels] WHERE [levels].[Level] = [APSActionsLogs].[Level]))
      AND (@MessageContains IS NULL OR [Message] LIKE N'%' + @MessageContains + N'%')
    ORDER BY [TimeStamp] DESC, [Id] DESC;
END
GO