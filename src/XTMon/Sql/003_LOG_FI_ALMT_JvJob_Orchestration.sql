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
CREATE OR ALTER PROCEDURE [monitoring].[UspJvJobEnqueue]
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
CREATE OR ALTER PROCEDURE [monitoring].[UspJvJobTakeNext]
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
CREATE OR ALTER PROCEDURE [monitoring].[UspJvJobHeartbeat]
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
CREATE OR ALTER PROCEDURE [monitoring].[UspJvJobSaveResult]
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
CREATE OR ALTER PROCEDURE [monitoring].[UspJvJobMarkCompleted]
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
CREATE OR ALTER PROCEDURE [monitoring].[UspJvJobMarkFailed]
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
CREATE OR ALTER PROCEDURE [monitoring].[UspJvJobGetById]
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
CREATE OR ALTER PROCEDURE [monitoring].[UspJvJobGetLatestByUserPnlDate]
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
