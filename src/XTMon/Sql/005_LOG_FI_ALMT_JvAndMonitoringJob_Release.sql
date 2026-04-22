USE [LOG_FI_ALMT]
GO

/*
    Destructive release script for JV and Monitoring job orchestration.
    Existing orchestration tables are dropped and recreated, so job history and latest-result data are removed.
*/

IF SCHEMA_ID(N'monitoring') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA [monitoring]');
END
GO

DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobExpireStale];
DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobGetLatestByCategory];
DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobGetLatestByKey];
DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobGetById];
DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobMarkFailed];
DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobMarkCompleted];
DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobSaveResult];
DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobHeartbeat];
DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobTakeNext];
DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobEnqueue];
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobExpireStale];
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobGetLatestByUserPnlDate];
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobGetById];
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobMarkFailed];
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobMarkCompleted];
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobSaveResult];
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobHeartbeat];
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobTakeNext];
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobEnqueue];
GO

DROP TABLE IF EXISTS [monitoring].[MonitoringLatestResults];
DROP TABLE IF EXISTS [monitoring].[MonitoringJobs];
DROP TABLE IF EXISTS [monitoring].[JvCalculationJobResults];
DROP TABLE IF EXISTS [monitoring].[JvCalculationJobs];
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [monitoring].[JvCalculationJobs]
(
    [JobId] BIGINT IDENTITY(1,1) NOT NULL,
    [UserId] VARCHAR(256) NOT NULL,
    [PnlDate] DATE NOT NULL,
    [RequestType] VARCHAR(20) NOT NULL,
    [Status] VARCHAR(20) NOT NULL,
    [WorkerId] VARCHAR(100) NULL,
    [EnqueuedAt] DATETIME2(3) NOT NULL CONSTRAINT [DF_JvCalculationJobs_EnqueuedAt] DEFAULT (SYSUTCDATETIME()),
    [StartedAt] DATETIME2(3) NULL,
    [LastHeartbeatAt] DATETIME2(3) NULL,
    [CompletedAt] DATETIME2(3) NULL,
    [FailedAt] DATETIME2(3) NULL,
    [ErrorMessage] NVARCHAR(MAX) NULL,
    CONSTRAINT [PK_JvCalculationJobs] PRIMARY KEY CLUSTERED ([JobId] ASC),
    CONSTRAINT [CK_JvCalculationJobs_Status] CHECK ([Status] IN ('Queued','Running','Completed','Failed')),
    CONSTRAINT [CK_JvCalculationJobs_RequestType] CHECK ([RequestType] IN ('CheckOnly','FixAndCheck'))
);
GO

CREATE TABLE [monitoring].[JvCalculationJobResults]
(
    [JobId] BIGINT NOT NULL,
    [QueryCheck] NVARCHAR(MAX) NULL,
    [QueryFix] NVARCHAR(MAX) NULL,
    [GridColumnsJson] NVARCHAR(MAX) NULL,
    [GridRowsJson] NVARCHAR(MAX) NULL,
    [SavedAt] DATETIME2(3) NOT NULL CONSTRAINT [DF_JvCalculationJobResults_SavedAt] DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT [PK_JvCalculationJobResults] PRIMARY KEY CLUSTERED ([JobId] ASC),
    CONSTRAINT [FK_JvCalculationJobResults_JvCalculationJobs_JobId]
        FOREIGN KEY ([JobId]) REFERENCES [monitoring].[JvCalculationJobs]([JobId]) ON DELETE CASCADE
);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_JvCalculationJobs_Active_User_PnlDate_RequestType]
    ON [monitoring].[JvCalculationJobs]([UserId],[PnlDate],[RequestType])
    WHERE [Status] IN ('Queued','Running');
GO

CREATE NONCLUSTERED INDEX [IX_JvCalculationJobs_Status_EnqueuedAt_JobId]
    ON [monitoring].[JvCalculationJobs]([Status],[EnqueuedAt],[JobId]);
GO

CREATE NONCLUSTERED INDEX [IX_JvCalculationJobs_User_PnlDate_RequestType_JobId]
    ON [monitoring].[JvCalculationJobs]([UserId],[PnlDate],[RequestType],[JobId] DESC);
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [monitoring].[UspJvJobEnqueue]
    @UserId VARCHAR(256),
    @PnlDate DATE,
    @RequestType VARCHAR(20) = 'FixAndCheck',
    @JobId BIGINT OUTPUT,
    @AlreadyActive BIT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    IF @RequestType IS NULL
        SET @RequestType = 'FixAndCheck';

    DECLARE @Now DATETIME2(3) = SYSUTCDATETIME();

    SET @JobId = NULL;
    SET @AlreadyActive = 0;

    BEGIN TRAN;

    SELECT TOP (1)
        @JobId = j.[JobId]
    FROM [monitoring].[JvCalculationJobs] j WITH (UPDLOCK, HOLDLOCK)
    WHERE j.[UserId] = @UserId
      AND j.[PnlDate] = @PnlDate
      AND j.[RequestType] = @RequestType
      AND j.[Status] IN ('Queued','Running')
    ORDER BY j.[JobId] DESC;

    IF @JobId IS NOT NULL
    BEGIN
        SET @AlreadyActive = 1;
        COMMIT TRAN;
        RETURN;
    END;

    INSERT INTO [monitoring].[JvCalculationJobs]
    (
        [UserId],
        [PnlDate],
        [RequestType],
        [Status],
        [EnqueuedAt]
    )
    VALUES
    (
        @UserId,
        @PnlDate,
        @RequestType,
        'Queued',
        @Now
    );

    SET @JobId = CONVERT(BIGINT, SCOPE_IDENTITY());
    SET @AlreadyActive = 0;

    COMMIT TRAN;
END
GO

CREATE PROCEDURE [monitoring].[UspJvJobTakeNext]
    @WorkerId VARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE @Now DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @Claimed TABLE ([JobId] BIGINT PRIMARY KEY);

    BEGIN TRAN;

    ;WITH [NextJob] AS
    (
        SELECT TOP (1)
            j.[JobId]
        FROM [monitoring].[JvCalculationJobs] j WITH (READPAST, UPDLOCK, ROWLOCK)
        WHERE j.[Status] = 'Queued'
        ORDER BY j.[EnqueuedAt], j.[JobId]
    )
    UPDATE j
    SET
        j.[Status] = 'Running',
        j.[WorkerId] = COALESCE(@WorkerId, j.[WorkerId]),
        j.[StartedAt] = ISNULL(j.[StartedAt], @Now),
        j.[LastHeartbeatAt] = @Now
    OUTPUT INSERTED.[JobId] INTO @Claimed([JobId])
    FROM [monitoring].[JvCalculationJobs] j
    INNER JOIN [NextJob] n ON n.[JobId] = j.[JobId];

    COMMIT TRAN;

    SELECT
        j.[JobId],
        j.[UserId],
        j.[PnlDate],
        j.[RequestType],
        j.[Status],
        j.[WorkerId],
        j.[EnqueuedAt],
        j.[StartedAt],
        j.[LastHeartbeatAt],
        j.[CompletedAt],
        j.[FailedAt],
        j.[ErrorMessage],
        r.[QueryCheck],
        r.[QueryFix],
        r.[GridColumnsJson],
        r.[GridRowsJson],
        r.[SavedAt]
    FROM @Claimed c
    INNER JOIN [monitoring].[JvCalculationJobs] j ON j.[JobId] = c.[JobId]
    LEFT JOIN [monitoring].[JvCalculationJobResults] r ON r.[JobId] = j.[JobId];
END
GO

CREATE PROCEDURE [monitoring].[UspJvJobHeartbeat]
    @JobId BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    UPDATE [monitoring].[JvCalculationJobs]
    SET [LastHeartbeatAt] = SYSUTCDATETIME()
    WHERE [JobId] = @JobId
      AND [Status] = 'Running';
END
GO

CREATE PROCEDURE [monitoring].[UspJvJobSaveResult]
    @JobId BIGINT,
    @QueryCheck NVARCHAR(MAX) = NULL,
    @QueryFix NVARCHAR(MAX) = NULL,
    @GridColumnsJson NVARCHAR(MAX) = NULL,
    @GridRowsJson NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    IF EXISTS (SELECT 1 FROM [monitoring].[JvCalculationJobResults] WHERE [JobId] = @JobId)
    BEGIN
        UPDATE [monitoring].[JvCalculationJobResults]
        SET
            [QueryCheck] = @QueryCheck,
            [QueryFix] = @QueryFix,
            [GridColumnsJson] = @GridColumnsJson,
            [GridRowsJson] = @GridRowsJson,
            [SavedAt] = SYSUTCDATETIME()
        WHERE [JobId] = @JobId;
    END
    ELSE
    BEGIN
        INSERT INTO [monitoring].[JvCalculationJobResults]
        (
            [JobId],
            [QueryCheck],
            [QueryFix],
            [GridColumnsJson],
            [GridRowsJson],
            [SavedAt]
        )
        VALUES
        (
            @JobId,
            @QueryCheck,
            @QueryFix,
            @GridColumnsJson,
            @GridRowsJson,
            SYSUTCDATETIME()
        );
    END
END
GO

CREATE PROCEDURE [monitoring].[UspJvJobMarkCompleted]
    @JobId BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    UPDATE [monitoring].[JvCalculationJobs]
    SET
        [Status] = 'Completed',
        [CompletedAt] = SYSUTCDATETIME(),
        [FailedAt] = NULL,
        [ErrorMessage] = NULL,
        [LastHeartbeatAt] = SYSUTCDATETIME()
    WHERE [JobId] = @JobId
      AND [Status] IN ('Running','Queued');
END
GO

CREATE PROCEDURE [monitoring].[UspJvJobMarkFailed]
    @JobId BIGINT,
    @ErrorMessage NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    UPDATE [monitoring].[JvCalculationJobs]
    SET
        [Status] = 'Failed',
        [FailedAt] = SYSUTCDATETIME(),
        [CompletedAt] = NULL,
        [ErrorMessage] = @ErrorMessage,
        [LastHeartbeatAt] = SYSUTCDATETIME()
    WHERE [JobId] = @JobId;
END
GO

CREATE PROCEDURE [monitoring].[UspJvJobGetById]
    @JobId BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    SELECT
        j.[JobId],
        j.[UserId],
        j.[PnlDate],
        j.[RequestType],
        j.[Status],
        j.[WorkerId],
        j.[EnqueuedAt],
        j.[StartedAt],
        j.[LastHeartbeatAt],
        j.[CompletedAt],
        j.[FailedAt],
        j.[ErrorMessage],
        r.[QueryCheck],
        r.[QueryFix],
        r.[GridColumnsJson],
        r.[GridRowsJson],
        r.[SavedAt]
    FROM [monitoring].[JvCalculationJobs] j
    LEFT JOIN [monitoring].[JvCalculationJobResults] r ON r.[JobId] = j.[JobId]
    WHERE j.[JobId] = @JobId;
END
GO

CREATE PROCEDURE [monitoring].[UspJvJobGetLatestByUserPnlDate]
    @UserId VARCHAR(256),
    @PnlDate DATE,
    @RequestType VARCHAR(20) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    SELECT TOP (1)
        j.[JobId],
        j.[UserId],
        j.[PnlDate],
        j.[RequestType],
        j.[Status],
        j.[WorkerId],
        j.[EnqueuedAt],
        j.[StartedAt],
        j.[LastHeartbeatAt],
        j.[CompletedAt],
        j.[FailedAt],
        j.[ErrorMessage],
        r.[QueryCheck],
        r.[QueryFix],
        r.[GridColumnsJson],
        r.[GridRowsJson],
        r.[SavedAt]
    FROM [monitoring].[JvCalculationJobs] j
    LEFT JOIN [monitoring].[JvCalculationJobResults] r ON r.[JobId] = j.[JobId]
    WHERE j.[UserId] = @UserId
      AND j.[PnlDate] = @PnlDate
      AND (@RequestType IS NULL OR j.[RequestType] = @RequestType)
    ORDER BY j.[JobId] DESC;
END
GO

CREATE PROCEDURE [monitoring].[UspJvJobExpireStale]
    @StaleTimeoutSeconds INT,
    @ErrorMessage NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT OFF;

    UPDATE [monitoring].[JvCalculationJobs]
    SET
        [Status] = 'Failed',
        [FailedAt] = SYSUTCDATETIME(),
        [CompletedAt] = NULL,
        [ErrorMessage] = CASE
                            WHEN [ErrorMessage] IS NULL
                              OR LTRIM(RTRIM([ErrorMessage])) = ''
                            THEN @ErrorMessage
                            ELSE [ErrorMessage]
                         END,
        [LastHeartbeatAt] = SYSUTCDATETIME()
    WHERE [Status] = 'Running'
      AND DATEADD(SECOND, @StaleTimeoutSeconds, COALESCE([LastHeartbeatAt], [StartedAt], [EnqueuedAt])) <= SYSUTCDATETIME();
END
GO

CREATE TABLE [monitoring].[MonitoringJobs]
(
    [JobId] BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_MonitoringJobs] PRIMARY KEY,
    [Category] VARCHAR(64) NOT NULL,
    [SubmenuKey] NVARCHAR(512) NOT NULL,
    [DisplayName] NVARCHAR(256) NULL,
    [PnlDate] DATE NOT NULL,
    [Status] VARCHAR(20) NOT NULL,
    [ParametersJson] NVARCHAR(MAX) NULL,
    [ParameterSummary] NVARCHAR(1024) NULL,
    [WorkerId] VARCHAR(100) NULL,
    [EnqueuedAt] DATETIME2(0) NOT NULL CONSTRAINT [DF_MonitoringJobs_EnqueuedAt] DEFAULT SYSUTCDATETIME(),
    [StartedAt] DATETIME2(0) NULL,
    [LastHeartbeatAt] DATETIME2(0) NULL,
    [CompletedAt] DATETIME2(0) NULL,
    [FailedAt] DATETIME2(0) NULL,
    [ErrorMessage] NVARCHAR(MAX) NULL,
    [KeyHash] BINARY(32) NOT NULL,
    [ActivityAt] AS COALESCE([LastHeartbeatAt], [StartedAt], [EnqueuedAt]) PERSISTED
);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_MonitoringJobs_Active]
    ON [monitoring].[MonitoringJobs]([KeyHash])
    WHERE [Status] IN ('Queued', 'Running');
GO

CREATE NONCLUSTERED INDEX [IX_MonitoringJobs_Queued_EnqueuedAt_JobId]
    ON [monitoring].[MonitoringJobs]([EnqueuedAt], [JobId])
    WHERE [Status] = 'Queued';
GO

CREATE NONCLUSTERED INDEX [IX_MonitoringJobs_Running_ActivityAt_JobId]
    ON [monitoring].[MonitoringJobs]([ActivityAt], [JobId])
    WHERE [Status] = 'Running';
GO

CREATE NONCLUSTERED INDEX [IX_MonitoringJobs_KeyHash_JobId]
    ON [monitoring].[MonitoringJobs]([KeyHash], [JobId] DESC);
GO

CREATE NONCLUSTERED INDEX [IX_MonitoringJobs_Category_PnlDate_KeyHash_JobId]
    ON [monitoring].[MonitoringJobs]([Category], [PnlDate], [KeyHash], [JobId] DESC);
GO

CREATE TABLE [monitoring].[MonitoringLatestResults]
(
    [LatestResultId] BIGINT IDENTITY(1,1) NOT NULL,
    [Category] VARCHAR(64) NOT NULL,
    [SubmenuKey] NVARCHAR(512) NOT NULL,
    [PnlDate] DATE NOT NULL,
    [KeyHash] BINARY(32) NOT NULL,
    [LastJobId] BIGINT NOT NULL,
    [ParsedQuery] NVARCHAR(MAX) NULL,
    [GridColumnsJson] NVARCHAR(MAX) NULL,
    [GridRowsJson] NVARCHAR(MAX) NULL,
    [MetadataJson] NVARCHAR(MAX) NULL,
    [SavedAt] DATETIME2(0) NOT NULL,
    CONSTRAINT [PK_MonitoringLatestResults] PRIMARY KEY CLUSTERED ([LatestResultId]),
    CONSTRAINT [FK_MonitoringLatestResults_MonitoringJobs] FOREIGN KEY ([LastJobId])
        REFERENCES [monitoring].[MonitoringJobs]([JobId])
);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_MonitoringLatestResults_KeyHash]
    ON [monitoring].[MonitoringLatestResults]([KeyHash]);
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [monitoring].[UspMonitoringJobEnqueue]
    @Category VARCHAR(64),
    @SubmenuKey NVARCHAR(512),
    @DisplayName NVARCHAR(256) = NULL,
    @PnlDate DATE,
    @ParametersJson NVARCHAR(MAX) = NULL,
    @ParameterSummary NVARCHAR(1024) = NULL,
    @JobId BIGINT OUTPUT,
    @AlreadyActive BIT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @KeyHash BINARY(32) = CONVERT(BINARY(32), HASHBYTES('SHA2_256', CONCAT(CONVERT(NVARCHAR(64), @Category), N'|', @SubmenuKey, N'|', CONVERT(NCHAR(10), @PnlDate, 23))));

    SET @JobId = NULL;
    SET @AlreadyActive = 0;

    BEGIN TRANSACTION;

    SELECT TOP (1)
        @JobId = [JobId]
    FROM [monitoring].[MonitoringJobs] WITH (UPDLOCK, HOLDLOCK)
    WHERE [KeyHash] = @KeyHash
      AND [Status] IN ('Queued', 'Running')
    ORDER BY [JobId] DESC;

    IF @JobId IS NOT NULL
    BEGIN
        SET @AlreadyActive = 1;
        COMMIT TRANSACTION;
        RETURN;
    END

    INSERT INTO [monitoring].[MonitoringJobs]
    (
        [Category],
        [SubmenuKey],
        [DisplayName],
        [PnlDate],
        [KeyHash],
        [Status],
        [ParametersJson],
        [ParameterSummary]
    )
    VALUES
    (
        @Category,
        @SubmenuKey,
        @DisplayName,
        @PnlDate,
        @KeyHash,
        'Queued',
        @ParametersJson,
        @ParameterSummary
    );

    SET @JobId = SCOPE_IDENTITY();
    SET @AlreadyActive = 0;

    COMMIT TRANSACTION;
END
GO

CREATE PROCEDURE [monitoring].[UspMonitoringJobTakeNext]
    @WorkerId VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    SET LOCK_TIMEOUT 5000;

    DECLARE @Selected TABLE ([JobId] BIGINT NOT NULL);

    BEGIN TRANSACTION;

    ;WITH [next_job] AS
    (
        SELECT TOP (1) [JobId]
        FROM [monitoring].[MonitoringJobs] WITH (UPDLOCK, READPAST, ROWLOCK)
        WHERE [Status] = 'Queued'
        ORDER BY [EnqueuedAt], [JobId]
    )
    UPDATE [jobs]
        SET [Status] = 'Running',
            [WorkerId] = @WorkerId,
            [StartedAt] = COALESCE([StartedAt], SYSUTCDATETIME()),
            [LastHeartbeatAt] = SYSUTCDATETIME(),
            [CompletedAt] = NULL,
            [FailedAt] = NULL,
            [ErrorMessage] = NULL
    OUTPUT INSERTED.[JobId] INTO @Selected([JobId])
    FROM [monitoring].[MonitoringJobs] AS [jobs]
    INNER JOIN [next_job] ON [next_job].[JobId] = [jobs].[JobId];

    COMMIT TRANSACTION;

    SELECT
        [jobs].[JobId],
        [jobs].[Category],
        [jobs].[SubmenuKey],
        [jobs].[DisplayName],
        [jobs].[PnlDate],
        [jobs].[Status],
        [jobs].[WorkerId],
        [jobs].[ParametersJson],
        [jobs].[ParameterSummary],
        [jobs].[EnqueuedAt],
        [jobs].[StartedAt],
        [jobs].[LastHeartbeatAt],
        [jobs].[CompletedAt],
        [jobs].[FailedAt],
        [jobs].[ErrorMessage],
        [results].[ParsedQuery],
        [results].[GridColumnsJson],
        [results].[GridRowsJson],
        [results].[MetadataJson],
        [results].[SavedAt]
    FROM @Selected AS [selected]
    INNER JOIN [monitoring].[MonitoringJobs] AS [jobs] ON [jobs].[JobId] = [selected].[JobId]
    LEFT JOIN [monitoring].[MonitoringLatestResults] AS [results]
        ON [results].[KeyHash] = [jobs].[KeyHash];
END
GO

CREATE PROCEDURE [monitoring].[UspMonitoringJobHeartbeat]
    @JobId BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE [monitoring].[MonitoringJobs]
       SET [LastHeartbeatAt] = SYSUTCDATETIME()
     WHERE [JobId] = @JobId;
END
GO

CREATE PROCEDURE [monitoring].[UspMonitoringJobSaveResult]
    @JobId BIGINT,
    @ParsedQuery NVARCHAR(MAX) = NULL,
    @GridColumnsJson NVARCHAR(MAX) = NULL,
    @GridRowsJson NVARCHAR(MAX) = NULL,
    @MetadataJson NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @Category VARCHAR(64);
    DECLARE @SubmenuKey NVARCHAR(512);
    DECLARE @PnlDate DATE;
    DECLARE @KeyHash BINARY(32);

    SELECT
        @Category = [Category],
        @SubmenuKey = [SubmenuKey],
        @PnlDate = [PnlDate],
        @KeyHash = [KeyHash]
    FROM [monitoring].[MonitoringJobs]
    WHERE [JobId] = @JobId;

    IF @Category IS NULL OR @SubmenuKey IS NULL OR @PnlDate IS NULL OR @KeyHash IS NULL
    BEGIN
        ;THROW 50001, 'Monitoring job does not exist.', 1;
    END

    MERGE [monitoring].[MonitoringLatestResults] AS [target]
    USING
    (
        SELECT
            @Category AS [Category],
            @SubmenuKey AS [SubmenuKey],
            @PnlDate AS [PnlDate],
            @KeyHash AS [KeyHash]
    ) AS [source]
    ON [target].[KeyHash] = [source].[KeyHash]
    WHEN MATCHED THEN
        UPDATE SET
            [LastJobId] = @JobId,
            [ParsedQuery] = @ParsedQuery,
            [GridColumnsJson] = @GridColumnsJson,
            [GridRowsJson] = @GridRowsJson,
            [MetadataJson] = @MetadataJson,
            [SavedAt] = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT ([Category], [SubmenuKey], [PnlDate], [KeyHash], [LastJobId], [ParsedQuery], [GridColumnsJson], [GridRowsJson], [MetadataJson], [SavedAt])
        VALUES (@Category, @SubmenuKey, @PnlDate, @KeyHash, @JobId, @ParsedQuery, @GridColumnsJson, @GridRowsJson, @MetadataJson, SYSUTCDATETIME());
END
GO

CREATE PROCEDURE [monitoring].[UspMonitoringJobMarkCompleted]
    @JobId BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE [monitoring].[MonitoringJobs]
       SET [Status] = 'Completed',
           [CompletedAt] = SYSUTCDATETIME(),
           [FailedAt] = NULL,
           [ErrorMessage] = NULL,
           [LastHeartbeatAt] = SYSUTCDATETIME()
     WHERE [JobId] = @JobId;
END
GO

CREATE PROCEDURE [monitoring].[UspMonitoringJobMarkFailed]
    @JobId BIGINT,
    @ErrorMessage NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE [monitoring].[MonitoringJobs]
       SET [Status] = 'Failed',
           [FailedAt] = SYSUTCDATETIME(),
           [CompletedAt] = NULL,
           [ErrorMessage] = @ErrorMessage,
           [LastHeartbeatAt] = SYSUTCDATETIME()
     WHERE [JobId] = @JobId;
END
GO

CREATE PROCEDURE [monitoring].[UspMonitoringJobGetById]
    @JobId BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        [jobs].[JobId],
        [jobs].[Category],
        [jobs].[SubmenuKey],
        [jobs].[DisplayName],
        [jobs].[PnlDate],
        [jobs].[Status],
        [jobs].[WorkerId],
        [jobs].[ParametersJson],
        [jobs].[ParameterSummary],
        [jobs].[EnqueuedAt],
        [jobs].[StartedAt],
        [jobs].[LastHeartbeatAt],
        [jobs].[CompletedAt],
        [jobs].[FailedAt],
        [jobs].[ErrorMessage],
        [results].[ParsedQuery],
        [results].[GridColumnsJson],
        [results].[GridRowsJson],
        [results].[MetadataJson],
        [results].[SavedAt]
    FROM [monitoring].[MonitoringJobs] AS [jobs]
    LEFT JOIN [monitoring].[MonitoringLatestResults] AS [results]
        ON [results].[KeyHash] = [jobs].[KeyHash]
    WHERE [jobs].[JobId] = @JobId;
END
GO

CREATE PROCEDURE [monitoring].[UspMonitoringJobGetLatestByKey]
    @Category VARCHAR(64),
    @SubmenuKey NVARCHAR(512),
    @PnlDate DATE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @KeyHash BINARY(32) = CONVERT(BINARY(32), HASHBYTES('SHA2_256', CONCAT(CONVERT(NVARCHAR(64), @Category), N'|', @SubmenuKey, N'|', CONVERT(NCHAR(10), @PnlDate, 23))));

    SELECT TOP (1)
        [jobs].[JobId],
        [jobs].[Category],
        [jobs].[SubmenuKey],
        [jobs].[DisplayName],
        [jobs].[PnlDate],
        [jobs].[Status],
        [jobs].[WorkerId],
        [jobs].[ParametersJson],
        [jobs].[ParameterSummary],
        [jobs].[EnqueuedAt],
        [jobs].[StartedAt],
        [jobs].[LastHeartbeatAt],
        [jobs].[CompletedAt],
        [jobs].[FailedAt],
        [jobs].[ErrorMessage],
        [results].[ParsedQuery],
        [results].[GridColumnsJson],
        [results].[GridRowsJson],
        [results].[MetadataJson],
        [results].[SavedAt]
    FROM [monitoring].[MonitoringJobs] AS [jobs]
    LEFT JOIN [monitoring].[MonitoringLatestResults] AS [results]
        ON [results].[KeyHash] = [jobs].[KeyHash]
    WHERE [jobs].[KeyHash] = @KeyHash
    ORDER BY [jobs].[JobId] DESC;
END
GO

CREATE PROCEDURE [monitoring].[UspMonitoringJobGetLatestByCategory]
    @Category VARCHAR(64),
    @PnlDate DATE
AS
BEGIN
    SET NOCOUNT ON;

    WITH [LatestJobs] AS
    (
        SELECT
            [jobs].[JobId],
            [jobs].[Category],
            [jobs].[SubmenuKey],
            [jobs].[DisplayName],
            [jobs].[PnlDate],
            [jobs].[Status],
            [jobs].[WorkerId],
            [jobs].[ParametersJson],
            [jobs].[ParameterSummary],
            [jobs].[EnqueuedAt],
            [jobs].[StartedAt],
            [jobs].[LastHeartbeatAt],
            [jobs].[CompletedAt],
            [jobs].[FailedAt],
            [jobs].[ErrorMessage],
            [jobs].[KeyHash],
            ROW_NUMBER() OVER (PARTITION BY [jobs].[KeyHash] ORDER BY [jobs].[JobId] DESC) AS [RowNumber]
        FROM [monitoring].[MonitoringJobs] AS [jobs]
        WHERE [jobs].[Category] = @Category
          AND [jobs].[PnlDate] = @PnlDate
    )
    SELECT
        [jobs].[JobId],
        [jobs].[Category],
        [jobs].[SubmenuKey],
        [jobs].[DisplayName],
        [jobs].[PnlDate],
        [jobs].[Status],
        [jobs].[WorkerId],
        [jobs].[ParametersJson],
        [jobs].[ParameterSummary],
        [jobs].[EnqueuedAt],
        [jobs].[StartedAt],
        [jobs].[LastHeartbeatAt],
        [jobs].[CompletedAt],
        [jobs].[FailedAt],
        [jobs].[ErrorMessage],
        NULL AS [ParsedQuery],
        NULL AS [GridColumnsJson],
        NULL AS [GridRowsJson],
        [results].[MetadataJson],
        [results].[SavedAt]
    FROM [LatestJobs] AS [jobs]
    LEFT JOIN [monitoring].[MonitoringLatestResults] AS [results]
        ON [results].[KeyHash] = [jobs].[KeyHash]
    WHERE [jobs].[RowNumber] = 1
    ORDER BY [jobs].[SubmenuKey];
END
GO

CREATE PROCEDURE [monitoring].[UspMonitoringJobExpireStale]
    @StaleTimeoutSeconds INT,
    @ErrorMessage NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    SET LOCK_TIMEOUT 5000;

    DECLARE @Cutoff DATETIME2(0) = DATEADD(SECOND, -@StaleTimeoutSeconds, SYSUTCDATETIME());

    UPDATE [jobs]
       SET [Status] = 'Failed',
           [FailedAt] = SYSUTCDATETIME(),
           [CompletedAt] = NULL,
           [ErrorMessage] = @ErrorMessage,
           [LastHeartbeatAt] = SYSUTCDATETIME()
    FROM [monitoring].[MonitoringJobs] AS [jobs] WITH (READPAST, UPDLOCK, ROWLOCK)
    WHERE [jobs].[Status] = 'Running'
      AND [jobs].[ActivityAt] <= @Cutoff;
END
GO