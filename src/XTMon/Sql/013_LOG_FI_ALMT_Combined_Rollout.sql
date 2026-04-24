/*
    Combined non-destructive rollout for LOG_FI_ALMT.

    Purpose:
    - provide one deployment entry point for the current LOG database changes
    - avoid the destructive reset behavior in 005_LOG_FI_ALMT_JvAndMonitoringJob_Release.sql

    Scope included:
    - JV job orchestration
    - JV cancelled status constraint update
    - JV latest-by-user proc optional-user update
    - Monitoring job orchestration
    - System Diagnostics cleanup procedures
    - Replay flow recovery procedures

    Prerequisite:
    - base LOG_FI_ALMT objects from 002_LOG_FI_ALMT_Logging_Setup.sql already exist
*/

/* ===== Begin 003_LOG_FI_ALMT_JvJob_Orchestration.sql ===== */
USE [LOG_FI_ALMT]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID(N'[monitoring].[JvCalculationJobs]', N'U') IS NULL
BEGIN
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
        CONSTRAINT [CK_JvCalculationJobs_Status] CHECK ([Status] IN ('Queued','Running','Completed','Failed','Cancelled')),
        CONSTRAINT [CK_JvCalculationJobs_RequestType] CHECK ([RequestType] IN ('CheckOnly','FixAndCheck'))
    )
END
GO

IF OBJECT_ID(N'[monitoring].[JvCalculationJobResults]', N'U') IS NULL
BEGIN
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
    )
END
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'UX_JvCalculationJobs_Active_User_PnlDate_RequestType'
      AND object_id = OBJECT_ID(N'[monitoring].[JvCalculationJobs]')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UX_JvCalculationJobs_Active_User_PnlDate_RequestType]
    ON [monitoring].[JvCalculationJobs]([UserId],[PnlDate],[RequestType])
    WHERE [Status] IN ('Queued','Running')
END
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_JvCalculationJobs_Status_EnqueuedAt_JobId'
      AND object_id = OBJECT_ID(N'[monitoring].[JvCalculationJobs]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_JvCalculationJobs_Status_EnqueuedAt_JobId]
    ON [monitoring].[JvCalculationJobs]([Status],[EnqueuedAt],[JobId])
END
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_JvCalculationJobs_User_PnlDate_RequestType_JobId'
      AND object_id = OBJECT_ID(N'[monitoring].[JvCalculationJobs]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_JvCalculationJobs_User_PnlDate_RequestType_JobId]
    ON [monitoring].[JvCalculationJobs]([UserId],[PnlDate],[RequestType],[JobId] DESC)
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobEnqueue]
GO
CREATE PROCEDURE [monitoring].[UspJvJobEnqueue]
    @UserId VARCHAR(256),
    @PnlDate DATE,
    @RequestType VARCHAR(20) = 'FixAndCheck',
    @JobId BIGINT OUTPUT,
    @AlreadyActive BIT OUTPUT
AS
-- Example:
-- DECLARE @JobId BIGINT, @AlreadyActive BIT;
-- EXEC [monitoring].[UspJvJobEnqueue]
--     @UserId = 'ISWIN\\igorsedykh',
--     @PnlDate = '2026-02-13',
--     @RequestType = 'CheckOnly',
--     @JobId = @JobId OUTPUT,
--     @AlreadyActive = @AlreadyActive OUTPUT;
-- SELECT @JobId AS JobId, @AlreadyActive AS AlreadyActive;
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

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobTakeNext]
GO
CREATE PROCEDURE [monitoring].[UspJvJobTakeNext]
    @WorkerId VARCHAR(100) = NULL
AS
-- Example:
-- EXEC [monitoring].[UspJvJobTakeNext] @WorkerId = 'XTMonWorker01';
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE @Now DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @Claimed TABLE ([JobId] BIGINT PRIMARY KEY);

    BEGIN TRAN;

    ;WITH NextJob AS
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
    INNER JOIN NextJob n ON n.[JobId] = j.[JobId];

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

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobHeartbeat]
GO
CREATE PROCEDURE [monitoring].[UspJvJobHeartbeat]
    @JobId BIGINT
AS
-- Example:
-- EXEC [monitoring].[UspJvJobHeartbeat] @JobId = 1;
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    UPDATE [monitoring].[JvCalculationJobs]
    SET [LastHeartbeatAt] = SYSUTCDATETIME()
    WHERE [JobId] = @JobId
      AND [Status] = 'Running';
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobSaveResult]
GO
CREATE PROCEDURE [monitoring].[UspJvJobSaveResult]
    @JobId BIGINT,
    @QueryCheck NVARCHAR(MAX) = NULL,
    @QueryFix NVARCHAR(MAX) = NULL,
    @GridColumnsJson NVARCHAR(MAX) = NULL,
    @GridRowsJson NVARCHAR(MAX) = NULL
AS
-- Example:
-- EXEC [monitoring].[UspJvJobSaveResult]
--     @JobId = 1,
--     @QueryCheck = N'SELECT 1 AS CheckResult',
--     @QueryFix = N'UPDATE ...',
--     @GridColumnsJson = N'["Status","Message"]',
--     @GridRowsJson = N'[["OK","Done"]]';
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

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobMarkCompleted]
GO
CREATE PROCEDURE [monitoring].[UspJvJobMarkCompleted]
    @JobId BIGINT
AS
-- Example:
-- EXEC [monitoring].[UspJvJobMarkCompleted] @JobId = 1;
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

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobMarkFailed]
GO
CREATE PROCEDURE [monitoring].[UspJvJobMarkFailed]
    @JobId BIGINT,
    @ErrorMessage NVARCHAR(MAX)
AS
-- Example:
-- EXEC [monitoring].[UspJvJobMarkFailed]
--     @JobId = 1,
--     @ErrorMessage = N'JV calculation failed due to timeout.';
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
    WHERE [JobId] = @JobId
      AND [Status] IN ('Running','Queued');
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobGetById]
GO
CREATE PROCEDURE [monitoring].[UspJvJobGetById]
    @JobId BIGINT
AS
-- Example:
-- EXEC [monitoring].[UspJvJobGetById] @JobId = 1;
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

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobGetLatestByUserPnlDate]
GO
CREATE PROCEDURE [monitoring].[UspJvJobGetLatestByUserPnlDate]
    @UserId VARCHAR(256),
    @PnlDate DATE,
    @RequestType VARCHAR(20) = NULL
AS
-- Example:
-- EXEC [monitoring].[UspJvJobGetLatestByUserPnlDate]
--     @UserId = 'ISWIN\\igorsedykh',
--     @PnlDate = '2026-02-13',
--     @RequestType = NULL;
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
/* ===== End 003_LOG_FI_ALMT_JvJob_Orchestration.sql ===== */

/* ===== Begin 010_LOG_FI_ALMT_JvCancelled_Status_Constraint.sql ===== */
USE [LOG_FI_ALMT]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID(N'[monitoring].[JvCalculationJobs]', N'U') IS NOT NULL
BEGIN
    IF EXISTS
    (
        SELECT 1
        FROM sys.check_constraints
        WHERE name = N'CK_JvCalculationJobs_Status'
          AND parent_object_id = OBJECT_ID(N'[monitoring].[JvCalculationJobs]')
    )
    BEGIN
        ALTER TABLE [monitoring].[JvCalculationJobs]
            DROP CONSTRAINT [CK_JvCalculationJobs_Status];
    END

    ALTER TABLE [monitoring].[JvCalculationJobs] WITH CHECK
        ADD CONSTRAINT [CK_JvCalculationJobs_Status]
        CHECK ([Status] IN ('Queued', 'Running', 'Completed', 'Failed', 'Cancelled'));

    ALTER TABLE [monitoring].[JvCalculationJobs]
        CHECK CONSTRAINT [CK_JvCalculationJobs_Status];
END
GO
/* ===== End 010_LOG_FI_ALMT_JvCancelled_Status_Constraint.sql ===== */

/* ===== Begin 012_LOG_FI_ALMT_JvJobGetLatest_OptionalUser.sql ===== */
USE [LOG_FI_ALMT]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =========================================================================================
-- Makes @UserId optional on [monitoring].[UspJvJobGetLatestByUserPnlDate] so the sidebar
-- nav-alert refresh can query the latest JV job for a P&L date without being tied to a
-- specific user session. Behavior for callers that still pass @UserId is unchanged.
-- =========================================================================================
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobGetLatestByUserPnlDate]
GO
CREATE PROCEDURE [monitoring].[UspJvJobGetLatestByUserPnlDate]
    @UserId VARCHAR(256) = NULL,
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
    WHERE (@UserId IS NULL OR LTRIM(RTRIM(@UserId)) = N'' OR j.[UserId] = @UserId)
      AND j.[PnlDate] = @PnlDate
      AND (@RequestType IS NULL OR j.[RequestType] = @RequestType)
    ORDER BY j.[EnqueuedAt] DESC, j.[JobId] DESC;
END
GO
/* ===== End 012_LOG_FI_ALMT_JvJobGetLatest_OptionalUser.sql ===== */

/* ===== Begin 004_LOG_FI_ALMT_MonitoringJob_Orchestration.sql ===== */
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

DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobEnqueue]
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

DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobTakeNext]
GO
CREATE PROCEDURE [monitoring].[UspMonitoringJobTakeNext]
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

DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobHeartbeat]
GO
CREATE PROCEDURE [monitoring].[UspMonitoringJobHeartbeat]
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

DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobSaveResult]
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

DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobMarkCompleted]
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
      WHERE [JobId] = @JobId
         AND [Status] IN ('Running', 'Queued');
END
GO

DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobMarkFailed]
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
      WHERE [JobId] = @JobId
         AND [Status] IN ('Running', 'Queued');
END
GO

DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobMarkCancelled]
GO
CREATE PROCEDURE [monitoring].[UspMonitoringJobMarkCancelled]
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

DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobCancelActive]
GO
CREATE PROCEDURE [monitoring].[UspMonitoringJobCancelActive]
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

DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobGetRuntimeByDmv]
GO
CREATE PROCEDURE [monitoring].[UspMonitoringJobGetRuntimeByDmv]
    @JobId BIGINT = NULL
WITH EXECUTE AS OWNER
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
            SELECT [lead_blocker] = 1
            FROM [master].[dbo].[sysprocesses] AS [sp]
            WHERE [sp].[spid] IN (SELECT [blocked] FROM [master].[dbo].[sysprocesses])
              AND [sp].[blocked] = 0
              AND [sp].[spid] = [er].[session_id]
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

DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobRecoverOrphanedRunningByDmv]
GO
CREATE PROCEDURE [monitoring].[UspMonitoringJobRecoverOrphanedRunningByDmv]
    @MinimumActivityAgeSeconds INT = 0,
    @ErrorMessage NVARCHAR(MAX)
WITH EXECUTE AS OWNER
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

DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobGetById]
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

DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobGetLatestByKey]
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

DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobGetLatestByCategory]
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

DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobExpireStale]
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
/* ===== End 004_LOG_FI_ALMT_MonitoringJob_Orchestration.sql ===== */

/* ===== Begin 009_LOG_FI_ALMT_SystemDiagnostics_Cleanup_Procedures.sql ===== */
USE [LOG_FI_ALMT]
GO

IF SCHEMA_ID(N'monitoring') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA [monitoring]');
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
DROP PROCEDURE IF EXISTS [monitoring].[UspSystemDiagnosticsCleanLogging]
GO
CREATE PROCEDURE [monitoring].[UspSystemDiagnosticsCleanLogging]
    @DeletedRows INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DELETE FROM [monitoring].[APSActionsLogs];
    SET @DeletedRows = @@ROWCOUNT;
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
DROP PROCEDURE IF EXISTS [monitoring].[UspSystemDiagnosticsCleanHistory]
GO
CREATE PROCEDURE [monitoring].[UspSystemDiagnosticsCleanHistory]
    @MonitoringLatestResultsDeleted INT OUTPUT,
    @MonitoringJobsDeleted INT OUTPUT,
    @JvCalculationJobResultsDeleted INT OUTPUT,
    @JvCalculationJobsDeleted INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF EXISTS (SELECT 1 FROM [monitoring].[MonitoringJobs] WHERE [Status] IN ('Queued', 'Running'))
       OR EXISTS (SELECT 1 FROM [monitoring].[JvCalculationJobs] WHERE [Status] IN ('Queued', 'Running'))
    BEGIN
        ;THROW 50001, 'Cannot clean history while monitoring or JV jobs are queued or running.', 1;
    END

    BEGIN TRANSACTION;

    DELETE FROM [monitoring].[MonitoringLatestResults];
    SET @MonitoringLatestResultsDeleted = @@ROWCOUNT;

    DELETE FROM [monitoring].[MonitoringJobs];
    SET @MonitoringJobsDeleted = @@ROWCOUNT;

    DELETE FROM [monitoring].[JvCalculationJobResults];
    SET @JvCalculationJobResultsDeleted = @@ROWCOUNT;

    DELETE FROM [monitoring].[JvCalculationJobs];
    SET @JvCalculationJobsDeleted = @@ROWCOUNT;

    COMMIT TRANSACTION;
END
GO
/* ===== End 009_LOG_FI_ALMT_SystemDiagnostics_Cleanup_Procedures.sql ===== */

/* ===== Begin 011_LOG_FI_ALMT_ReplayFlow_Recovery.sql ===== */
USE [LOG_FI_ALMT]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =========================================================================================
-- Replay batch recovery procedures, created in LOG_FI_ALMT where the
-- administration.ReplayFlows table already lives. No cross-database writes.
-- =========================================================================================

-- =========================================================================================
-- administration.UspFailStaleReplayBatches
-- =========================================================================================
-- Marks replay batch rows that have been in an InProgress state longer than
-- @StaleTimeoutSeconds as failed with ReplayStatus = 'Timed Out'.
-- A row is considered InProgress when DateStarted IS NOT NULL AND DateCompleted IS NULL.
-- Returns the number of rows affected.
-- =========================================================================================
IF OBJECT_ID(N'[administration].[UspFailStaleReplayBatches]', N'P') IS NOT NULL
    DROP PROCEDURE [administration].[UspFailStaleReplayBatches];
GO

CREATE PROCEDURE [administration].[UspFailStaleReplayBatches]
    @StaleTimeoutSeconds INT,
    @ErrorMessage NVARCHAR(400) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @StaleTimeoutSeconds IS NULL OR @StaleTimeoutSeconds < 1
        SET @StaleTimeoutSeconds = 900;

    IF @ErrorMessage IS NULL OR LTRIM(RTRIM(@ErrorMessage)) = N''
        SET @ErrorMessage = N'Replay batch timed out while InProgress and was auto-failed.';

    UPDATE [administration].[ReplayFlows]
       SET [DateCompleted] = GETDATE(),
           [ReplayStatus]  = N'Timed Out',
           [ProcessStatus] = COALESCE([ProcessStatus], N'error')
     WHERE [DateStarted] IS NOT NULL
       AND [DateCompleted] IS NULL
       AND DATEDIFF(SECOND, [DateStarted], GETDATE()) > @StaleTimeoutSeconds;

    SELECT @@ROWCOUNT AS [ExpiredCount];
END
GO

-- =========================================================================================
-- administration.UspFailRunningReplayBatches
-- =========================================================================================
-- Called on app startup to auto-fail any replay batch row left InProgress from a previous
-- process generation. A row is considered InProgress when DateStarted IS NOT NULL AND
-- DateCompleted IS NULL. Marks ReplayStatus = 'Failed - Startup Recovery'.
-- Returns the number of rows affected.
-- =========================================================================================
IF OBJECT_ID(N'[administration].[UspFailRunningReplayBatches]', N'P') IS NOT NULL
    DROP PROCEDURE [administration].[UspFailRunningReplayBatches];
GO

CREATE PROCEDURE [administration].[UspFailRunningReplayBatches]
    @ErrorMessage NVARCHAR(400) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @ErrorMessage IS NULL OR LTRIM(RTRIM(@ErrorMessage)) = N''
        SET @ErrorMessage = N'Replay batch was InProgress when the application started and was failed during startup recovery.';

    UPDATE [administration].[ReplayFlows]
       SET [DateCompleted] = GETDATE(),
           [ReplayStatus]  = N'Failed - Startup Recovery',
           [ProcessStatus] = COALESCE([ProcessStatus], N'error')
     WHERE [DateStarted] IS NOT NULL
       AND [DateCompleted] IS NULL;

    SELECT @@ROWCOUNT AS [RecoveredCount];
END
GO

-- =========================================================================================
-- administration.UspGetStuckReplayBatches
-- =========================================================================================
-- Returns replay batch rows currently in an InProgress state (DateStarted NOT NULL,
-- DateCompleted NULL) for visibility on the System Diagnostics page. Includes the age
-- in seconds so the UI can highlight rows that exceed the configured stale threshold.
-- =========================================================================================
IF OBJECT_ID(N'[administration].[UspGetStuckReplayBatches]', N'P') IS NOT NULL
    DROP PROCEDURE [administration].[UspGetStuckReplayBatches];
GO

CREATE PROCEDURE [administration].[UspGetStuckReplayBatches]
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        [FlowId],
        [FlowIdDerivedFrom],
        [PnlDate],
        [PackageGuid],
        [CreatedBy],
        [DateCreated],
        [DateStarted],
        [DateCompleted],
        [ReplayStatus],
        [ProcessStatus],
        DATEDIFF(SECOND, [DateStarted], GETDATE()) AS [AgeSeconds]
    FROM [administration].[ReplayFlows]
    WHERE [DateStarted] IS NOT NULL
      AND [DateCompleted] IS NULL
    ORDER BY [DateStarted];
END
GO
/* ===== End 011_LOG_FI_ALMT_ReplayFlow_Recovery.sql ===== */
