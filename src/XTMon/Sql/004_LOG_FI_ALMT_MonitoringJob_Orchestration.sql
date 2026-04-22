IF SCHEMA_ID(N'monitoring') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA [monitoring]');
END
GO

IF OBJECT_ID(N'[monitoring].[MonitoringJobs]', N'U') IS NULL
BEGIN
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
        [ErrorMessage] NVARCHAR(MAX) NULL
    );
END
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[monitoring].[MonitoringJobs]')
      AND name = N'KeyHash'
)
BEGIN
    ALTER TABLE [monitoring].[MonitoringJobs]
                ADD [KeyHash] BINARY(32) NULL;
END
GO

IF OBJECT_ID(N'[monitoring].[MonitoringJobs]', N'U') IS NOT NULL
   AND COLUMNPROPERTY(OBJECT_ID(N'[monitoring].[MonitoringJobs]'), N'KeyHash', 'ColumnId') IS NOT NULL
   AND COLUMNPROPERTY(OBJECT_ID(N'[monitoring].[MonitoringJobs]'), N'KeyHash', 'IsComputed') = 0
BEGIN
        UPDATE [monitoring].[MonitoringJobs]
             SET [KeyHash] = CONVERT(BINARY(32), HASHBYTES('SHA2_256', CONCAT(CONVERT(NVARCHAR(64), [Category]), N'|', [SubmenuKey], N'|', CONVERT(NCHAR(10), [PnlDate], 23))))
         WHERE [KeyHash] IS NULL;
END
GO

IF OBJECT_ID(N'[monitoring].[MonitoringJobs]', N'U') IS NOT NULL
   AND COLUMNPROPERTY(OBJECT_ID(N'[monitoring].[MonitoringJobs]'), N'KeyHash', 'ColumnId') IS NOT NULL
   AND COLUMNPROPERTY(OBJECT_ID(N'[monitoring].[MonitoringJobs]'), N'KeyHash', 'IsComputed') = 0
   AND COLUMNPROPERTY(OBJECT_ID(N'[monitoring].[MonitoringJobs]'), N'KeyHash', 'AllowsNull') = 1
BEGIN
        ALTER TABLE [monitoring].[MonitoringJobs]
                ALTER COLUMN [KeyHash] BINARY(32) NOT NULL;
END
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[monitoring].[MonitoringJobs]')
      AND name = N'ActivityAt'
)
BEGIN
    ALTER TABLE [monitoring].[MonitoringJobs]
        ADD [ActivityAt] AS COALESCE([LastHeartbeatAt], [StartedAt], [EnqueuedAt]) PERSISTED;
END
GO

IF EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'UX_MonitoringJobs_Active'
      AND object_id = OBJECT_ID(N'[monitoring].[MonitoringJobs]')
)
BEGIN
    DROP INDEX [UX_MonitoringJobs_Active] ON [monitoring].[MonitoringJobs];
END
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_MonitoringJobs_Active]
    ON [monitoring].[MonitoringJobs]([KeyHash])
    WHERE [Status] IN ('Queued', 'Running');
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_MonitoringJobs_Queued_EnqueuedAt_JobId'
      AND object_id = OBJECT_ID(N'[monitoring].[MonitoringJobs]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_MonitoringJobs_Queued_EnqueuedAt_JobId]
        ON [monitoring].[MonitoringJobs]([EnqueuedAt], [JobId])
        WHERE [Status] = 'Queued';
END
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_MonitoringJobs_Running_ActivityAt_JobId'
      AND object_id = OBJECT_ID(N'[monitoring].[MonitoringJobs]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_MonitoringJobs_Running_ActivityAt_JobId]
        ON [monitoring].[MonitoringJobs]([ActivityAt], [JobId])
        WHERE [Status] = 'Running';
END
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_MonitoringJobs_KeyHash_JobId'
      AND object_id = OBJECT_ID(N'[monitoring].[MonitoringJobs]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_MonitoringJobs_KeyHash_JobId]
        ON [monitoring].[MonitoringJobs]([KeyHash], [JobId] DESC);
END
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_MonitoringJobs_Category_PnlDate_KeyHash_JobId'
      AND object_id = OBJECT_ID(N'[monitoring].[MonitoringJobs]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_MonitoringJobs_Category_PnlDate_KeyHash_JobId]
        ON [monitoring].[MonitoringJobs]([Category], [PnlDate], [KeyHash], [JobId] DESC);
END
GO

IF OBJECT_ID(N'[monitoring].[MonitoringLatestResults]', N'U') IS NULL
BEGIN
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
END
GO

IF OBJECT_ID(N'[monitoring].[MonitoringLatestResults]', N'U') IS NOT NULL
   AND COL_LENGTH(N'[monitoring].[MonitoringLatestResults]', N'LatestResultId') IS NULL
BEGIN
    ALTER TABLE [monitoring].[MonitoringLatestResults]
        ADD [LatestResultId] BIGINT IDENTITY(1,1) NOT NULL;
END
GO

IF OBJECT_ID(N'[monitoring].[MonitoringLatestResults]', N'U') IS NOT NULL
    AND COL_LENGTH(N'[monitoring].[MonitoringLatestResults]', N'KeyHash') IS NULL
BEGIN
     ALTER TABLE [monitoring].[MonitoringLatestResults]
          ADD [KeyHash] BINARY(32) NULL;
END
GO

IF OBJECT_ID(N'[monitoring].[MonitoringLatestResults]', N'U') IS NOT NULL
    AND COLUMNPROPERTY(OBJECT_ID(N'[monitoring].[MonitoringLatestResults]'), N'KeyHash', 'ColumnId') IS NOT NULL
    AND COLUMNPROPERTY(OBJECT_ID(N'[monitoring].[MonitoringLatestResults]'), N'KeyHash', 'IsComputed') = 0
BEGIN
    UPDATE [monitoring].[MonitoringLatestResults]
       SET [KeyHash] = CONVERT(BINARY(32), HASHBYTES('SHA2_256', CONCAT(CONVERT(NVARCHAR(64), [Category]), N'|', [SubmenuKey], N'|', CONVERT(NCHAR(10), [PnlDate], 23))))
     WHERE [KeyHash] IS NULL;
END
GO

IF OBJECT_ID(N'[monitoring].[MonitoringLatestResults]', N'U') IS NOT NULL
    AND COLUMNPROPERTY(OBJECT_ID(N'[monitoring].[MonitoringLatestResults]'), N'KeyHash', 'ColumnId') IS NOT NULL
    AND COLUMNPROPERTY(OBJECT_ID(N'[monitoring].[MonitoringLatestResults]'), N'KeyHash', 'IsComputed') = 0
    AND COLUMNPROPERTY(OBJECT_ID(N'[monitoring].[MonitoringLatestResults]'), N'KeyHash', 'AllowsNull') = 1
BEGIN
    ALTER TABLE [monitoring].[MonitoringLatestResults]
        ALTER COLUMN [KeyHash] BINARY(32) NOT NULL;
END
GO

IF OBJECT_ID(N'[monitoring].[MonitoringLatestResults]', N'U') IS NOT NULL
   AND NOT EXISTS
   (
       SELECT 1
       FROM sys.key_constraints AS [kc]
       INNER JOIN sys.index_columns AS [ic]
           ON [ic].[object_id] = [kc].[parent_object_id]
          AND [ic].[index_id] = [kc].[unique_index_id]
       INNER JOIN sys.columns AS [c]
           ON [c].[object_id] = [ic].[object_id]
          AND [c].[column_id] = [ic].[column_id]
       WHERE [kc].[parent_object_id] = OBJECT_ID(N'[monitoring].[MonitoringLatestResults]')
         AND [kc].[name] = N'PK_MonitoringLatestResults'
         AND [c].[name] = N'LatestResultId'
   )
BEGIN
    IF EXISTS
    (
        SELECT 1
        FROM sys.key_constraints
        WHERE [parent_object_id] = OBJECT_ID(N'[monitoring].[MonitoringLatestResults]')
          AND [name] = N'PK_MonitoringLatestResults'
    )
    BEGIN
        ALTER TABLE [monitoring].[MonitoringLatestResults]
            DROP CONSTRAINT [PK_MonitoringLatestResults];
    END

    ALTER TABLE [monitoring].[MonitoringLatestResults]
        ADD CONSTRAINT [PK_MonitoringLatestResults]
            PRIMARY KEY CLUSTERED ([LatestResultId]);
END
GO

IF EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'UX_MonitoringLatestResults_Category_SubmenuKey_PnlDate'
      AND [object_id] = OBJECT_ID(N'[monitoring].[MonitoringLatestResults]')
)
BEGIN
    DROP INDEX [UX_MonitoringLatestResults_Category_SubmenuKey_PnlDate] ON [monitoring].[MonitoringLatestResults];
END
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'UX_MonitoringLatestResults_KeyHash'
      AND [object_id] = OBJECT_ID(N'[monitoring].[MonitoringLatestResults]')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UX_MonitoringLatestResults_KeyHash]
        ON [monitoring].[MonitoringLatestResults]([KeyHash]);
END
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspMonitoringJobEnqueue]
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

CREATE OR ALTER PROCEDURE [monitoring].[UspMonitoringJobTakeNext]
    @WorkerId VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

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

CREATE OR ALTER PROCEDURE [monitoring].[UspMonitoringJobHeartbeat]
    @JobId BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE [monitoring].[MonitoringJobs]
       SET [LastHeartbeatAt] = SYSUTCDATETIME()
     WHERE [JobId] = @JobId;
END
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspMonitoringJobSaveResult]
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
    USING (
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

CREATE OR ALTER PROCEDURE [monitoring].[UspMonitoringJobMarkCompleted]
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

CREATE OR ALTER PROCEDURE [monitoring].[UspMonitoringJobMarkFailed]
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

CREATE OR ALTER PROCEDURE [monitoring].[UspMonitoringJobGetById]
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

CREATE OR ALTER PROCEDURE [monitoring].[UspMonitoringJobGetLatestByKey]
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

CREATE OR ALTER PROCEDURE [monitoring].[UspMonitoringJobGetLatestByCategory]
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
        [results].[ParsedQuery],
        [results].[GridColumnsJson],
        [results].[GridRowsJson],
        [results].[MetadataJson],
        [results].[SavedAt]
    FROM [LatestJobs] AS [jobs]
    LEFT JOIN [monitoring].[MonitoringLatestResults] AS [results]
        ON [results].[KeyHash] = [jobs].[KeyHash]
    WHERE [jobs].[RowNumber] = 1
    ORDER BY [jobs].[SubmenuKey];
END
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspMonitoringJobExpireStale]
    @StaleTimeoutSeconds INT,
    @ErrorMessage NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

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