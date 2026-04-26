USE [LOG_FI_ALMT]
GO

/*
    Full LOG_FI_ALMT rebuild for XTMon-managed objects.

    Purpose:
    - provide one destructive migration entry point for the LOG database
    - replace the scattered LOG_FI_ALMT setup/orchestration/hotfix scripts with one file

    Scope recreated here:
    - base LOG monitoring/logging tables and procedures
    - replay recovery table support and watchdog procedures
    - JV orchestration tables, indexes, and procedures
    - monitoring-job orchestration tables, indexes, and procedures
    - system-diagnostics cleanup procedures

    Notes:
    - this script drops and recreates XTMon-managed LOG objects
    - monitoring/JV job history and cached latest results are removed
    - DBandBackup / DBSizePlusDisk / APSActionsLogs / ReplayFlows are also recreated
*/

IF SCHEMA_ID(N'logging') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA [logging]');
END
GO

IF SCHEMA_ID(N'administration') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA [administration]');
END
GO

IF SCHEMA_ID(N'monitoring') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA [monitoring]');
END
GO

DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobExpireStale];
DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobGetLatestByCategory];
DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobGetLatestByKey];
DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobGetById];
DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobRecoverOrphanedRunningByDmv];
DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobGetRuntimeByDmv];
DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobCancelActive];
DROP PROCEDURE IF EXISTS [monitoring].[UspMonitoringJobMarkCancelled];
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
DROP PROCEDURE IF EXISTS [monitoring].[UspSystemDiagnosticsCleanHistory];
DROP PROCEDURE IF EXISTS [monitoring].[UspSystemDiagnosticsCleanLogging];
DROP PROCEDURE IF EXISTS [monitoring].[UspInsertAPSActionsLog];
DROP PROCEDURE IF EXISTS [monitoring].[UspGetDBBackups];
DROP PROCEDURE IF EXISTS [monitoring].[UspGetDBbackups];
DROP PROCEDURE IF EXISTS [monitoring].[UspGetDbSizePlusDisk];
DROP PROCEDURE IF EXISTS [monitoring].[UspGetDBSizePlusDisk];
DROP PROCEDURE IF EXISTS [monitoring].[spGetDbSizeStats];
DROP PROCEDURE IF EXISTS [administration].[UspGetStuckReplayBatches];
DROP PROCEDURE IF EXISTS [administration].[UspFailRunningReplayBatches];
DROP PROCEDURE IF EXISTS [administration].[UspFailStaleReplayBatches];
GO

DROP TABLE IF EXISTS [monitoring].[MonitoringLatestResults];
DROP TABLE IF EXISTS [monitoring].[MonitoringJobs];
DROP TABLE IF EXISTS [monitoring].[JvCalculationJobResults];
DROP TABLE IF EXISTS [monitoring].[JvCalculationJobs];
DROP TABLE IF EXISTS [monitoring].[DBSizePlusDisk];
DROP TABLE IF EXISTS [monitoring].[DBandBackup];
DROP TABLE IF EXISTS [monitoring].[APSActionsLogs];
DROP TABLE IF EXISTS [administration].[ReplayFlows];
DROP TABLE IF EXISTS [administration].[Flows];
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [administration].[Flows](
    [FlowId] [bigint] NULL,
    [FlowIdDerivedFrom] [bigint] NULL,
    [BusinessDataTypeId] [smallint] NULL,
    [FeedSourceId] [smallint] NULL,
    [PnlDate] [date] NOT NULL,
    [ReportingDate] [date] NULL,
    [FileName] [varchar](500) NULL,
    [ArrivalDate] [datetime] NULL,
    [PackageGuid] [uniqueidentifier] NULL,
    [CurrentStep] [varchar](50) NULL,
    [IsFailed] [bit] NULL,
    [TypeOfCalculation] [varchar](50) NULL
) ON [PRIMARY]
GO

CREATE TABLE [administration].[ReplayFlows](
    [FlowId] [bigint] NULL,
    [FlowIdDerivedFrom] [bigint] NULL,
    [PnlDate] [date] NOT NULL,
    [PackageGuid] [uniqueidentifier] NULL,
    [WithBackdated] [bit] NOT NULL,
    [SkipCoreProcess] [bit] NOT NULL,
    [DropTableTmp] [bit] NOT NULL,
    [CreatedBy] [varchar](100) NOT NULL,
    [DateCreated] [datetime] NOT NULL,
    [DateStarted] [datetime] NULL,
    [DateSubmitted] [datetime] NULL,
    [DateCompleted] [datetime] NULL,
    [ReplayStatus] [varchar](50) NULL,
        [ProcessStatus] [varchar](50) CONSTRAINT [DF_ReplayFlows_ProcessStatus] DEFAULT ('Not Started') NULL
) ON [PRIMARY]
GO

CREATE TABLE [monitoring].[APSActionsLogs](
    [Id] [int] IDENTITY(1,1) NOT NULL,
    [Message] [nvarchar](max) NULL,
    [MessageTemplate] [nvarchar](max) NULL,
    [Level] [nvarchar](max) NULL,
    [TimeStamp] [datetime] NULL,
    [Exception] [nvarchar](max) NULL,
    [Properties] [nvarchar](max) NULL,
    CONSTRAINT [PK_APSActionsLogs] PRIMARY KEY CLUSTERED ([Id] ASC)
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

CREATE TABLE [monitoring].[DBandBackup](
    [LastUpdated] [datetime] NULL,
    [Metier] [varchar](4) NOT NULL,
    [InstanceName] [varchar](100) NULL,
    [DBName] [varchar](100) NULL,
    [SpaceAllocatedMB] [decimal](38, 2) NULL,
    [SpaceUsedMB] [decimal](38, 2) NULL,
    [RecoveryModel] [nvarchar](60) NULL,
    [LastFullBackup] [datetime] NULL,
    [LastDifferentialBackup] [datetime] NULL
) ON [PRIMARY]
GO

CREATE TABLE [monitoring].[DBSizePlusDisk](
    [ExtTime] [datetime2](0) NULL,
    [InstanceName] [nvarchar](128) NULL,
    [DatabaseName] [nvarchar](128) NULL,
    [LogicalFileName] [nvarchar](128) NULL,
    [FileGroup] [nvarchar](128) NULL,
    [PhysicalFileName] [nvarchar](512) NULL,
    [FileType] [nvarchar](10) NULL,
    [AllocatedSpaceMB] [decimal](18, 6) NULL,
    [UsedSpaceMB] [decimal](18, 0) NULL,
    [FreeSpaceMB] [decimal](18, 0) NULL,
    [UsedPercent] [int] NULL,
    [MaxSizeMB] [decimal](18, 0) NULL,
    [AutogrowSize] [nvarchar](50) NULL,
    [TotalDriveMB] [decimal](18, 0) NULL,
    [FreeDriveMB] [decimal](18, 0) NULL,
    [FreeDrivePercent] [decimal](5, 2) NULL
) ON [PRIMARY]
GO

CREATE PROCEDURE [monitoring].[UspGetDBBackups]
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        [LastUpdated],
        [Metier],
        [InstanceName],
        [DBName],
        [SpaceAllocatedMB],
        [SpaceUsedMB],
        [RecoveryModel],
        [LastFullBackup],
        [LastDifferentialBackup]
    FROM [LOG_FI_ALMT].[monitoring].[DBandBackup];
END
GO

CREATE PROCEDURE [monitoring].[UspGetDbSizePlusDisk]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @MaxExtTime DATETIME = (SELECT MAX([ExtTime]) FROM [monitoring].[DBSizePlusDisk]);

    SELECT
        [DatabaseName],
        [FileGroup],
        CAST([AllocatedSpaceMB] AS INT) AS [AllocatedDBSpaceMB],
        [UsedSpaceMB] AS [UsedDBSpaceMB],
        [FreeSpaceMB] AS [FreeDBSpaceMB],
        [MaxSizeMB],
        CASE WHEN [AutogrowSize] IS NULL THEN 0 ELSE 1 END AS [AutogrowEnabled],
        [FreeDriveMB]
    INTO #DBSize
    FROM [monitoring].[DBSizePlusDisk]
    WHERE [FileGroup] LIKE 'DATAFACT%'
      AND [ExtTime] = @MaxExtTime;

    DECLARE @PartSizeMB INT = 20248881;

    SELECT
        @MaxExtTime AS [LastUpdated],
        [DatabaseName],
        [FileGroup],
        [AllocatedSpaceMB],
        [UsedSpaceMB],
        [FreeSpaceMB],
        [Autogrow],
        [FreeDriveMB],
        @PartSizeMB AS [PartSizeMB],
        [TotalFreeSpaceMB],
        CASE
            WHEN [TotalFreeSpaceMB] < @PartSizeMB THEN 'CRITICAL'
            WHEN [TotalFreeSpaceMB] >= @PartSizeMB AND [TotalFreeSpaceMB] < [PartSizeMBx2] THEN 'WARNING'
            ELSE 'OK'
        END AS [AlertLevel]
    FROM (
        SELECT
            [DatabaseName],
            [FileGroup],
            SUM([AllocatedDBSpaceMB]) AS [AllocatedSpaceMB],
            SUM([UsedDBSpaceMB]) AS [UsedSpaceMB],
            SUM([FreeDBSpaceMB]) AS [FreeSpaceMB],
            CASE WHEN SUM([AutogrowEnabled]) > 0 THEN 'YES' ELSE 'NO' END AS [Autogrow],
            MAX([FreeDriveMB]) AS [FreeDriveMB],
            CASE
                WHEN SUM([AutogrowEnabled]) > 0 THEN SUM([FreeDBSpaceMB]) + MAX([FreeDriveMB])
                ELSE SUM([FreeDBSpaceMB])
            END AS [TotalFreeSpaceMB],
            @PartSizeMB * 2 AS [PartSizeMBx2]
        FROM #DBSize
        GROUP BY [DatabaseName], [FileGroup]
    ) AS [summary];
END
GO

CREATE PROCEDURE [monitoring].[spGetDbSizeStats]
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        [ExtTime],
        [InstanceName],
        [DatabaseName],
        [LogicalFileName],
        [FileGroup],
        [PhysicalFileName],
        [FileType],
        [AllocatedSpaceMB],
        [UsedSpaceMB],
        [FreeSpaceMB],
        [UsedPercent],
        [MaxSizeMB],
        [AutogrowSize],
        [TotalDriveMB],
        [FreeDriveMB],
        [FreeDrivePercent]
    FROM [monitoring].[DBSizePlusDisk];
END
GO

CREATE PROCEDURE [monitoring].[UspInsertAPSActionsLog]
    @TimeStamp DATETIME2(3),
    @Level NVARCHAR(32),
    @Message NVARCHAR(MAX),
    @MessageTemplate NVARCHAR(MAX) = NULL,
    @Exception NVARCHAR(MAX) = NULL,
    @Properties NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO [monitoring].[APSActionsLogs]
    (
        [TimeStamp],
        [Level],
        [Message],
        [MessageTemplate],
        [Exception],
        [Properties]
    )
    VALUES
    (
        @TimeStamp,
        @Level,
        @Message,
        @MessageTemplate,
        @Exception,
        @Properties
    );
END
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
    [ActivityAt] AS COALESCE([LastHeartbeatAt], [StartedAt], [EnqueuedAt]) PERSISTED,
    CONSTRAINT [PK_JvCalculationJobs] PRIMARY KEY CLUSTERED ([JobId] ASC),
    CONSTRAINT [CK_JvCalculationJobs_Status] CHECK ([Status] IN ('Queued','Running','Completed','Failed','Cancelled')),
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

CREATE NONCLUSTERED INDEX [IX_JvCalculationJobs_Queued_EnqueuedAt_JobId]
    ON [monitoring].[JvCalculationJobs]([EnqueuedAt],[JobId])
    WHERE [Status] = 'Queued';
GO

CREATE NONCLUSTERED INDEX [IX_JvCalculationJobs_Running_ActivityAt_JobId]
    ON [monitoring].[JvCalculationJobs]([ActivityAt],[JobId])
    WHERE [Status] = 'Running';
GO

CREATE NONCLUSTERED INDEX [IX_JvCalculationJobs_User_PnlDate_RequestType_JobId]
    ON [monitoring].[JvCalculationJobs]([UserId],[PnlDate],[RequestType],[JobId] DESC)
    INCLUDE ([Status],[WorkerId],[EnqueuedAt],[StartedAt],[LastHeartbeatAt],[CompletedAt],[FailedAt],[ErrorMessage]);
GO

CREATE NONCLUSTERED INDEX [IX_JvCalculationJobs_User_PnlDate_JobId]
    ON [monitoring].[JvCalculationJobs]([UserId],[PnlDate],[JobId] DESC)
    INCLUDE ([RequestType],[Status],[WorkerId],[EnqueuedAt],[StartedAt],[LastHeartbeatAt],[CompletedAt],[FailedAt],[ErrorMessage]);
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
    SET XACT_ABORT ON;
    SET LOCK_TIMEOUT 5000;

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
    SET XACT_ABORT ON;
    SET LOCK_TIMEOUT 5000;

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

CREATE PROCEDURE [monitoring].[UspJvJobExpireStale]
    @StaleTimeoutSeconds INT,
    @ErrorMessage NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    SET LOCK_TIMEOUT 5000;

    DECLARE @Cutoff DATETIME2(3) = DATEADD(SECOND, -@StaleTimeoutSeconds, SYSUTCDATETIME());

    UPDATE [jobs]
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
    FROM [monitoring].[JvCalculationJobs] AS [jobs] WITH (READPAST, UPDLOCK, ROWLOCK)
    WHERE [jobs].[Status] = 'Running'
      AND [jobs].[ActivityAt] <= @Cutoff;
END
GO

CREATE TABLE [monitoring].[MonitoringJobs]
(
    [JobId] BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_MonitoringJobs] PRIMARY KEY,
    [Category] VARCHAR(64) NOT NULL,
    [SubmenuKey] NVARCHAR(512) NOT NULL,
    [DisplayName] NVARCHAR(256) NULL,
    [PnlDate] DATE NOT NULL,
    [KeyHash] BINARY(32) NOT NULL,
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
    [ActivityAt] AS COALESCE([LastHeartbeatAt], [StartedAt], [EnqueuedAt]) PERSISTED,
    CONSTRAINT [CK_MonitoringJobs_Status] CHECK ([Status] IN ('Queued','Running','Completed','Failed','Cancelled'))
);
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

CREATE UNIQUE NONCLUSTERED INDEX [UX_MonitoringLatestResults_KeyHash]
    ON [monitoring].[MonitoringLatestResults]([KeyHash]);
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

CREATE PROCEDURE [monitoring].[UspMonitoringJobTakeNext]
    @WorkerId VARCHAR(100),
    @ExcludedCategoriesCsv NVARCHAR(4000) = NULL,
    @IncludedSubmenuKeysCsv NVARCHAR(4000) = NULL,
    @ExcludedSubmenuKeysCsv NVARCHAR(4000) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    SET LOCK_TIMEOUT 5000;

    DECLARE @Selected TABLE ([JobId] BIGINT NOT NULL);
    DECLARE @ExcludedCategories TABLE ([Category] VARCHAR(64) NOT NULL PRIMARY KEY);
    DECLARE @IncludedSubmenuKeys TABLE ([SubmenuKey] NVARCHAR(512) NOT NULL);
    DECLARE @ExcludedSubmenuKeys TABLE ([SubmenuKey] NVARCHAR(512) NOT NULL);

    IF @ExcludedCategoriesCsv IS NOT NULL
    BEGIN
        INSERT INTO @ExcludedCategories ([Category])
        SELECT DISTINCT CONVERT(VARCHAR(64), LTRIM(RTRIM([value])))
        FROM STRING_SPLIT(@ExcludedCategoriesCsv, ',')
        WHERE LTRIM(RTRIM([value])) <> '';
    END

    IF @IncludedSubmenuKeysCsv IS NOT NULL
    BEGIN
        INSERT INTO @IncludedSubmenuKeys ([SubmenuKey])
        SELECT DISTINCT CONVERT(NVARCHAR(512), LTRIM(RTRIM([value])))
        FROM STRING_SPLIT(@IncludedSubmenuKeysCsv, ',')
        WHERE LTRIM(RTRIM([value])) <> '';
    END

    IF @ExcludedSubmenuKeysCsv IS NOT NULL
    BEGIN
        INSERT INTO @ExcludedSubmenuKeys ([SubmenuKey])
        SELECT DISTINCT CONVERT(NVARCHAR(512), LTRIM(RTRIM([value])))
        FROM STRING_SPLIT(@ExcludedSubmenuKeysCsv, ',')
        WHERE LTRIM(RTRIM([value])) <> '';
    END

    BEGIN TRANSACTION;

    ;WITH [next_job] AS
    (
        SELECT TOP (1) [JobId]
        FROM [monitoring].[MonitoringJobs] WITH (UPDLOCK, READPAST, ROWLOCK)
        WHERE [Status] = 'Queued'
          AND (
                NOT EXISTS (SELECT 1 FROM @ExcludedCategories)
                OR NOT EXISTS
                (
                    SELECT 1
                    FROM @ExcludedCategories AS [excluded]
                    WHERE [excluded].[Category] = [MonitoringJobs].[Category]
                )
              )
          AND (
                NOT EXISTS (SELECT 1 FROM @IncludedSubmenuKeys)
                OR EXISTS
                (
                    SELECT 1
                    FROM @IncludedSubmenuKeys AS [included]
                    WHERE [included].[SubmenuKey] = [MonitoringJobs].[SubmenuKey]
                )
              )
          AND (
                NOT EXISTS (SELECT 1 FROM @ExcludedSubmenuKeys)
                OR NOT EXISTS
                (
                    SELECT 1
                    FROM @ExcludedSubmenuKeys AS [excludedSubmenu]
                    WHERE [excludedSubmenu].[SubmenuKey] = [MonitoringJobs].[SubmenuKey]
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

CREATE PROCEDURE [monitoring].[UspMonitoringJobGetRuntimeByDmv]
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

CREATE PROCEDURE [monitoring].[UspMonitoringJobRecoverOrphanedRunningByDmv]
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
