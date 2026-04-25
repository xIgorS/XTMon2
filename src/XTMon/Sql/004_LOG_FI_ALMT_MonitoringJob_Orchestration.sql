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
    @WorkerId VARCHAR(100),
    @ExcludedCategoriesCsv NVARCHAR(4000) = NULL
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
          AND (
                @ExcludedCategoriesCsv IS NULL
                OR NOT EXISTS
                (
                    SELECT 1
                    FROM STRING_SPLIT(@ExcludedCategoriesCsv, ',') AS [excluded]
                    WHERE LTRIM(RTRIM([excluded].[value])) = [MonitoringJobs].[Category]
                )
              )
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
        SET LOCK_TIMEOUT 5000;

        UPDATE [jobs]
       SET [LastHeartbeatAt] = SYSUTCDATETIME()
        FROM [monitoring].[MonitoringJobs] AS [jobs] WITH (ROWLOCK)
        WHERE [JobId] = @JobId
            AND [Status] = 'Running';
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
      WHERE [JobId] = @JobId
         AND [Status] IN ('Running', 'Queued');
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
      WHERE [JobId] = @JobId
         AND [Status] IN ('Running', 'Queued');
END
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspMonitoringJobMarkCancelled]
    @JobId BIGINT,
    @ErrorMessage NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE [monitoring].[MonitoringJobs]
       SET [Status] = 'Cancelled',
           [FailedAt] = CASE WHEN [StartedAt] IS NULL THEN NULL ELSE SYSUTCDATETIME() END,
           [CompletedAt] = NULL,
           [ErrorMessage] = @ErrorMessage,
           [LastHeartbeatAt] = SYSUTCDATETIME()
      WHERE [JobId] = @JobId
         AND [Status] IN ('Running', 'Queued');
END
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspMonitoringJobCancelActive]
    @ErrorMessage NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE [monitoring].[MonitoringJobs]
       SET [Status] = 'Cancelled',
           [FailedAt] = CASE WHEN [StartedAt] IS NULL THEN NULL ELSE SYSUTCDATETIME() END,
           [CompletedAt] = NULL,
           [ErrorMessage] = @ErrorMessage,
           [LastHeartbeatAt] = SYSUTCDATETIME()
     WHERE [Status] IN ('Queued', 'Running');

    SELECT @@ROWCOUNT AS [CancelledCount];
END
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspMonitoringJobGetRuntimeByDmv]
    @JobId BIGINT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    ;WITH [runtime] AS
    (
        SELECT
            [JobId] = CONVERT(BIGINT, SUBSTRING([ses].[context_info], 9, 8)),
            [SPID] = [er].[session_id],
            [BlkBy] = CASE WHEN [lb].[lead_blocker] = 1 THEN -1 ELSE [er].[blocking_session_id] END,
            [ElapsedMS] = [er].[total_elapsed_time],
            [CPU] = [er].[cpu_time],
            [IOReads] = [er].[logical_reads] + [er].[reads],
            [IOWrites] = [er].[writes],
            [Executions] = [ec].[execution_count],
            [CommandType] = [er].[command],
            [LastWaitType] = [er].[last_wait_type],
            [ObjectName] = OBJECT_SCHEMA_NAME([qt].[objectid], [qt].[dbid]) + N'.' + OBJECT_NAME([qt].[objectid], [qt].[dbid]),
            [SQLStatement] = SUBSTRING(
                [qt].[text],
                ([er].[statement_start_offset] / 2) + 1,
                ((CASE WHEN [er].[statement_end_offset] = -1
                    THEN LEN(CONVERT(NVARCHAR(MAX), [qt].[text])) * 2
                    ELSE [er].[statement_end_offset]
                    END - [er].[statement_start_offset]) / 2) + 1),
            [Status] = [ses].[status],
            [Login] = [ses].[login_name],
            [Host] = [ses].[host_name],
            [DBName] = DB_NAME([er].[database_id]),
            [StartTime] = [er].[start_time],
            [Protocol] = [con].[net_transport],
            [TransactionIsolation] = CASE [ses].[transaction_isolation_level]
                WHEN 0 THEN 'Unspecified'
                WHEN 1 THEN 'Read Uncommitted'
                WHEN 2 THEN 'Read Committed'
                WHEN 3 THEN 'Repeatable'
                WHEN 4 THEN 'Serializable'
                WHEN 5 THEN 'Snapshot'
            END,
            [ConnectionWrites] = [con].[num_writes],
            [ConnectionReads] = [con].[num_reads],
            [ClientAddress] = [con].[client_net_address],
            [Authentication] = [con].[auth_scheme],
            [DatetimeSnapshot] = GETDATE()
        FROM [sys].[dm_exec_requests] AS [er]
        INNER JOIN [sys].[dm_exec_sessions] AS [ses]
            ON [ses].[session_id] = [er].[session_id]
        LEFT JOIN [sys].[dm_exec_connections] AS [con]
            ON [con].[session_id] = [ses].[session_id]
        CROSS APPLY [sys].[dm_exec_sql_text]([er].[sql_handle]) AS [qt]
        OUTER APPLY
        (
            SELECT [execution_count] = MAX([cp].[usecounts])
            FROM [sys].[dm_exec_cached_plans] AS [cp]
            WHERE [cp].[plan_handle] = [er].[plan_handle]
        ) AS [ec]
        OUTER APPLY
        (
                        SELECT TOP (1) [lead_blocker] = 1
                        FROM [sys].[dm_exec_requests] AS [blocked]
                        WHERE [blocked].[blocking_session_id] = [er].[session_id]
                            AND [blocked].[blocking_session_id] > 0
        ) AS [lb]
        WHERE DATALENGTH([ses].[context_info]) >= 16
          AND SUBSTRING([ses].[context_info], 1, 8) = 0x58544D4F4E4A4F42
          AND (@JobId IS NULL OR CONVERT(BIGINT, SUBSTRING([ses].[context_info], 9, 8)) = @JobId)
    )
    SELECT
        [runtime].[JobId],
        [jobs].[Category],
        [jobs].[SubmenuKey],
        [jobs].[DisplayName],
        [jobs].[PnlDate],
        [jobs].[WorkerId],
        [runtime].[SPID],
        [runtime].[BlkBy],
        [runtime].[ElapsedMS],
        [runtime].[CPU],
        [runtime].[IOReads],
        [runtime].[IOWrites],
        [runtime].[Executions],
        [runtime].[CommandType],
        [runtime].[LastWaitType],
        [runtime].[ObjectName],
        [runtime].[SQLStatement],
        [runtime].[Status],
        [runtime].[Login],
        [runtime].[Host],
        [runtime].[DBName],
        [runtime].[StartTime],
        [runtime].[Protocol],
        [runtime].[TransactionIsolation],
        [runtime].[ConnectionWrites],
        [runtime].[ConnectionReads],
        [runtime].[ClientAddress],
        [runtime].[Authentication],
        [runtime].[DatetimeSnapshot]
    FROM [runtime]
    LEFT JOIN [monitoring].[MonitoringJobs] AS [jobs]
        ON [jobs].[JobId] = [runtime].[JobId]
    ORDER BY
        [runtime].[BlkBy] DESC,
        [runtime].[IOReads] DESC,
        [runtime].[SPID];
END
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspMonitoringJobRecoverOrphanedRunningByDmv]
    @MinimumActivityAgeSeconds INT = 0,
    @ErrorMessage NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    IF @MinimumActivityAgeSeconds IS NULL OR @MinimumActivityAgeSeconds < 0
        SET @MinimumActivityAgeSeconds = 0;

    ;WITH [ActiveRequests] AS
    (
        SELECT DISTINCT CONVERT(BIGINT, SUBSTRING([ses].[context_info], 9, 8)) AS [JobId]
        FROM [sys].[dm_exec_requests] AS [er]
        INNER JOIN [sys].[dm_exec_sessions] AS [ses]
            ON [ses].[session_id] = [er].[session_id]
        WHERE DATALENGTH([ses].[context_info]) >= 16
          AND SUBSTRING([ses].[context_info], 1, 8) = 0x58544D4F4E4A4F42
    )
    UPDATE [jobs]
       SET [Status] = 'Failed',
           [FailedAt] = SYSUTCDATETIME(),
           [CompletedAt] = NULL,
           [ErrorMessage] = @ErrorMessage,
           [LastHeartbeatAt] = SYSUTCDATETIME()
    FROM [monitoring].[MonitoringJobs] AS [jobs]
    LEFT JOIN [ActiveRequests] AS [active]
        ON [active].[JobId] = [jobs].[JobId]
    WHERE [jobs].[Status] = 'Running'
      AND [active].[JobId] IS NULL
      AND DATEDIFF(SECOND, [jobs].[ActivityAt], SYSUTCDATETIME()) >= @MinimumActivityAgeSeconds;

    SELECT @@ROWCOUNT AS [RecoveredCount];
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
                        [jobs].[KeyHash],
                        ROW_NUMBER() OVER (PARTITION BY [jobs].[KeyHash] ORDER BY [jobs].[JobId] DESC) AS [RowNumber]
                FROM [monitoring].[MonitoringJobs] AS [jobs] WITH (NOLOCK)
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
    LEFT JOIN [monitoring].[MonitoringLatestResults] AS [results] WITH (NOLOCK)
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