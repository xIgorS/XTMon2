USE [LOG_FI_ALMT]
GO
/****** Object:  StoredProcedure [monitoring].[UspJvJobTakeNext]    Script Date: 2/27/2026 6:18:45 PM ******/
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobTakeNext]
GO
/****** Object:  StoredProcedure [monitoring].[UspJvJobSaveResult]    Script Date: 2/27/2026 6:18:45 PM ******/
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobSaveResult]
GO
/****** Object:  StoredProcedure [monitoring].[UspJvJobMarkFailed]    Script Date: 2/27/2026 6:18:45 PM ******/
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobMarkFailed]
GO
/****** Object:  StoredProcedure [monitoring].[UspJvJobMarkCompleted]    Script Date: 2/27/2026 6:18:45 PM ******/
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobMarkCompleted]
GO
/****** Object:  StoredProcedure [monitoring].[UspJvJobHeartbeat]    Script Date: 2/27/2026 6:18:45 PM ******/
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobHeartbeat]
GO
/****** Object:  StoredProcedure [monitoring].[UspJvJobGetLatestByUserPnlDate]    Script Date: 2/27/2026 6:18:45 PM ******/
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobGetLatestByUserPnlDate]
GO
/****** Object:  StoredProcedure [monitoring].[UspJvJobGetById]    Script Date: 2/27/2026 6:18:45 PM ******/
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobGetById]
GO
/****** Object:  StoredProcedure [monitoring].[UspJvJobEnqueue]    Script Date: 2/27/2026 6:18:45 PM ******/
DROP PROCEDURE IF EXISTS [monitoring].[UspJvJobEnqueue]
GO
/****** Object:  StoredProcedure [monitoring].[UspInsertAPSActionsLog]    Script Date: 2/27/2026 6:18:45 PM ******/
DROP PROCEDURE IF EXISTS [monitoring].[UspInsertAPSActionsLog]
GO
/****** Object:  StoredProcedure [monitoring].[UspGetDBSizePlusDisk]    Script Date: 2/27/2026 6:18:45 PM ******/
DROP PROCEDURE IF EXISTS [monitoring].[UspGetDBSizePlusDisk]
GO
/****** Object:  StoredProcedure [monitoring].[UspGetDBbackups]    Script Date: 2/27/2026 6:18:45 PM ******/
DROP PROCEDURE IF EXISTS [monitoring].[UspGetDBbackups]
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[monitoring].[JvCalculationJobs]') AND type in (N'U'))
ALTER TABLE [monitoring].[JvCalculationJobs] DROP CONSTRAINT IF EXISTS [CK_JvCalculationJobs_Status]
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[monitoring].[JvCalculationJobs]') AND type in (N'U'))
ALTER TABLE [monitoring].[JvCalculationJobs] DROP CONSTRAINT IF EXISTS [CK_JvCalculationJobs_RequestType]
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[monitoring].[JvCalculationJobResults]') AND type in (N'U'))
ALTER TABLE [monitoring].[JvCalculationJobResults] DROP CONSTRAINT IF EXISTS [FK_JvCalculationJobResults_JvCalculationJobs_JobId]
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[monitoring].[JvCalculationJobs]') AND type in (N'U'))
ALTER TABLE [monitoring].[JvCalculationJobs] DROP CONSTRAINT IF EXISTS [DF_JvCalculationJobs_EnqueuedAt]
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[monitoring].[JvCalculationJobResults]') AND type in (N'U'))
ALTER TABLE [monitoring].[JvCalculationJobResults] DROP CONSTRAINT IF EXISTS [DF_JvCalculationJobResults_SavedAt]
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[administration].[ReplayFlows]') AND type in (N'U'))
ALTER TABLE [administration].[ReplayFlows] DROP CONSTRAINT IF EXISTS [DF__ReplayFlo__Proce__6EF57B66]
GO
/****** Object:  Table [monitoring].[JvCalculationJobs]    Script Date: 2/27/2026 6:18:45 PM ******/
DROP TABLE IF EXISTS [monitoring].[JvCalculationJobs]
GO
/****** Object:  Table [monitoring].[JvCalculationJobResults]    Script Date: 2/27/2026 6:18:45 PM ******/
DROP TABLE IF EXISTS [monitoring].[JvCalculationJobResults]
GO
/****** Object:  Table [monitoring].[DBSizePlusDisk]    Script Date: 2/27/2026 6:18:45 PM ******/
DROP TABLE IF EXISTS [monitoring].[DBSizePlusDisk]
GO
/****** Object:  Table [monitoring].[DBandBackup]    Script Date: 2/27/2026 6:18:45 PM ******/
DROP TABLE IF EXISTS [monitoring].[DBandBackup]
GO
/****** Object:  Table [monitoring].[APSActionsLogs]    Script Date: 2/27/2026 6:18:45 PM ******/
DROP TABLE IF EXISTS [monitoring].[APSActionsLogs]
GO
/****** Object:  Table [administration].[ReplayFlows]    Script Date: 2/27/2026 6:18:45 PM ******/
DROP TABLE IF EXISTS [administration].[ReplayFlows]
GO
/****** Object:  UserDefinedTableType [administration].[ReplayAdjAtCoreSet]    Script Date: 2/27/2026 6:18:45 PM ******/
DROP TYPE IF EXISTS [administration].[ReplayAdjAtCoreSet]
GO
/****** Object:  UserDefinedTableType [administration].[ReplayAdjAtCoreSet]    Script Date: 2/27/2026 6:18:45 PM ******/
CREATE TYPE [administration].[ReplayAdjAtCoreSet] AS TABLE(
	[FlowIdDerivedFrom] [bigint] NOT NULL,
	[FlowId] [bigint] NOT NULL,
	[PnlDate] [date] NOT NULL,
	[WithBackdated] [bit] NOT NULL DEFAULT ((1)),
	[SkipCoreProcess] [bit] NOT NULL DEFAULT ((0)),
	[Droptabletpm] [bit] NOT NULL DEFAULT ((1))
)
GO
/****** Object:  Table [administration].[ReplayFlows]    Script Date: 2/27/2026 6:18:45 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
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
	[ProcessStatus] [varchar](50) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [monitoring].[APSActionsLogs]    Script Date: 2/27/2026 6:18:45 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [monitoring].[APSActionsLogs](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Message] [nvarchar](max) NULL,
	[MessageTemplate] [nvarchar](max) NULL,
	[Level] [nvarchar](max) NULL,
	[TimeStamp] [datetime] NULL,
	[Exception] [nvarchar](max) NULL,
	[Properties] [nvarchar](max) NULL,
 CONSTRAINT [PK_APSActionsLogs] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [monitoring].[DBandBackup]    Script Date: 2/27/2026 6:18:45 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
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
/****** Object:  Table [monitoring].[DBSizePlusDisk]    Script Date: 2/27/2026 6:18:45 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
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
/****** Object:  Table [monitoring].[JvCalculationJobResults]    Script Date: 2/27/2026 6:18:45 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [monitoring].[JvCalculationJobResults](
	[JobId] [bigint] NOT NULL,
	[QueryCheck] [nvarchar](max) NULL,
	[QueryFix] [nvarchar](max) NULL,
	[GridColumnsJson] [nvarchar](max) NULL,
	[GridRowsJson] [nvarchar](max) NULL,
	[SavedAt] [datetime2](3) NOT NULL,
 CONSTRAINT [PK_JvCalculationJobResults] PRIMARY KEY CLUSTERED 
(
	[JobId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [monitoring].[JvCalculationJobs]    Script Date: 2/27/2026 6:18:45 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [monitoring].[JvCalculationJobs](
	[JobId] [bigint] IDENTITY(1,1) NOT NULL,
	[UserId] [varchar](256) NOT NULL,
	[PnlDate] [date] NOT NULL,
	[RequestType] [varchar](20) NOT NULL,
	[Status] [varchar](20) NOT NULL,
	[WorkerId] [varchar](100) NULL,
	[EnqueuedAt] [datetime2](3) NOT NULL,
	[StartedAt] [datetime2](3) NULL,
	[LastHeartbeatAt] [datetime2](3) NULL,
	[CompletedAt] [datetime2](3) NULL,
	[FailedAt] [datetime2](3) NULL,
	[ErrorMessage] [nvarchar](max) NULL,
 CONSTRAINT [PK_JvCalculationJobs] PRIMARY KEY CLUSTERED 
(
	[JobId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
ALTER TABLE [administration].[ReplayFlows] ADD  DEFAULT ('Not Started') FOR [ProcessStatus]
GO
ALTER TABLE [monitoring].[JvCalculationJobResults] ADD  CONSTRAINT [DF_JvCalculationJobResults_SavedAt]  DEFAULT (sysutcdatetime()) FOR [SavedAt]
GO
ALTER TABLE [monitoring].[JvCalculationJobs] ADD  CONSTRAINT [DF_JvCalculationJobs_EnqueuedAt]  DEFAULT (sysutcdatetime()) FOR [EnqueuedAt]
GO
ALTER TABLE [monitoring].[JvCalculationJobResults]  WITH CHECK ADD  CONSTRAINT [FK_JvCalculationJobResults_JvCalculationJobs_JobId] FOREIGN KEY([JobId])
REFERENCES [monitoring].[JvCalculationJobs] ([JobId])
ON DELETE CASCADE
GO
ALTER TABLE [monitoring].[JvCalculationJobResults] CHECK CONSTRAINT [FK_JvCalculationJobResults_JvCalculationJobs_JobId]
GO
ALTER TABLE [monitoring].[JvCalculationJobs]  WITH CHECK ADD  CONSTRAINT [CK_JvCalculationJobs_RequestType] CHECK  (([RequestType]='FixAndCheck' OR [RequestType]='CheckOnly'))
GO
ALTER TABLE [monitoring].[JvCalculationJobs] CHECK CONSTRAINT [CK_JvCalculationJobs_RequestType]
GO
ALTER TABLE [monitoring].[JvCalculationJobs]  WITH CHECK ADD  CONSTRAINT [CK_JvCalculationJobs_Status] CHECK  (([Status]='Failed' OR [Status]='Completed' OR [Status]='Running' OR [Status]='Queued'))
GO
ALTER TABLE [monitoring].[JvCalculationJobs] CHECK CONSTRAINT [CK_JvCalculationJobs_Status]
GO
/****** Object:  StoredProcedure [monitoring].[UspGetDBbackups]    Script Date: 2/27/2026 6:18:45 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCedure [monitoring].[UspGetDBbackups]
AS

select

       [LastUpdated]
      ,[Metier]
      ,[InstanceName]
      ,[DBName]
      ,[SpaceAllocatedMB]
      ,[SpaceUsedMB]
      ,[RecoveryModel]
      ,[LastFullBackup]
      ,[LastDifferentialBackup]
  FROM [LOG_FI_ALMT].[monitoring].[DBandBackup]

     
GO
/****** Object:  StoredProcedure [monitoring].[UspGetDBSizePlusDisk]    Script Date: 2/27/2026 6:18:45 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCedure [monitoring].[UspGetDBSizePlusDisk]
AS

DECLARE @MaxExtTime DATETIME = (select max(ExtTime) from [monitoring].[DBSizePlusDisk])


SELECT 
      [DatabaseName]
      ,[FileGroup] 
      ,CAST(AllocatedSpaceMB AS INT) AS AllocatedDBSpaceMB
      ,[UsedSpaceMB] as UsedDBSpaceMB
      ,[FreeSpaceMB] as FreeDBSpaceMB
      ,[MaxSizeMB]
      ,CASE WHEN [AutogrowSize] is null THEN 0 else 1 END AS [AutogrowEnabled]
     -- ,CASE WHEN [AutogrowSize] is null THEN FreeSpaceMB else FreeSpaceMB + FreeDriveMB END AS FreeSpace
      ,[FreeDriveMB] 
      INTO #DBSize   
  FROM [monitoring].[DBSizePlusDisk]
    WHERE [FileGroup] LIKE 'DATAFACT%'
      AND ExtTime = @MaxExtTime

--select * from #DBSize
DECLARE @PartSizeMB INT = 20248881

SELECT @MaxExtTime LastUpdated, DatabaseName, [FileGroup], AllocatedSpaceMB, UsedSpaceMB, FreeSpaceMB, Autogrow, FreeDriveMB, @PartSizeMB AS PartSizeMB
    , TotalFreeSpaceMB
, CASE 
    WHEN TotalFreeSpaceMB < @PartSizeMB THEN 'CRITICAL' 
    WHEN TotalFreeSpaceMB >= @PartSizeMB AND TotalFreeSpaceMB < PartSizeMBx2 THEN 'WARNING' 
    ELSE 'OK' 
  END AS AlertLevel
FROM (

SELECT  DatabaseName, [FileGroup], sum(AllocatedDBSpaceMB) as AllocatedSpaceMB, sum(UsedDBSpaceMB) as UsedSpaceMB, sum(FreeDBSpaceMB) as FreeSpaceMB
,CASE when sum(AutogrowEnabled) > 0 then 'YES' else 'NO' end as Autogrow, max(FreeDriveMB) as FreeDriveMB
,CASE when sum(AutogrowEnabled) > 0 then sum(FreeDBSpaceMB) + max(FreeDriveMB) else sum(FreeDBSpaceMB) end as TotalFreeSpaceMB
,@PartSizeMB * 2 as PartSizeMBx2
  FROM #DBSize
    GROUP BY DatabaseName, [FileGroup]  
) AS T 
     
GO
/****** Object:  StoredProcedure [monitoring].[UspInsertAPSActionsLog]    Script Date: 2/27/2026 6:18:45 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [monitoring].[UspInsertAPSActionsLog]
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
END;
GO
/****** Object:  StoredProcedure [monitoring].[UspJvJobEnqueue]    Script Date: 2/27/2026 6:18:45 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [monitoring].[UspJvJobEnqueue]
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
/****** Object:  StoredProcedure [monitoring].[UspJvJobGetById]    Script Date: 2/27/2026 6:18:45 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [monitoring].[UspJvJobGetById]
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
/****** Object:  StoredProcedure [monitoring].[UspJvJobGetLatestByUserPnlDate]    Script Date: 2/27/2026 6:18:45 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [monitoring].[UspJvJobGetLatestByUserPnlDate]
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
/****** Object:  StoredProcedure [monitoring].[UspJvJobHeartbeat]    Script Date: 2/27/2026 6:18:45 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [monitoring].[UspJvJobHeartbeat]
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
/****** Object:  StoredProcedure [monitoring].[UspJvJobMarkCompleted]    Script Date: 2/27/2026 6:18:45 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [monitoring].[UspJvJobMarkCompleted]
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
/****** Object:  StoredProcedure [monitoring].[UspJvJobMarkFailed]    Script Date: 2/27/2026 6:18:45 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [monitoring].[UspJvJobMarkFailed]
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
    WHERE [JobId] = @JobId;
END
GO
/****** Object:  StoredProcedure [monitoring].[UspJvJobSaveResult]    Script Date: 2/27/2026 6:18:45 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [monitoring].[UspJvJobSaveResult]
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
/****** Object:  StoredProcedure [monitoring].[UspJvJobTakeNext]    Script Date: 2/27/2026 6:18:45 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [monitoring].[UspJvJobTakeNext]
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
CREATE OR ALTER PROCEDURE [monitoring].[UspJvJobExpireStale]
    @StaleTimeoutSeconds INT,
    @ErrorMessage        NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT OFF; -- OFF so that @@ROWCOUNT / rows-affected is returned to the caller

    UPDATE [monitoring].[JvCalculationJobs]
    SET
        [Status]          = 'Failed',
        [FailedAt]        = SYSUTCDATETIME(),
        [CompletedAt]     = NULL,
        [ErrorMessage]    = CASE
                                WHEN [ErrorMessage] IS NULL
                                  OR LTRIM(RTRIM([ErrorMessage])) = ''
                                THEN @ErrorMessage
                                ELSE [ErrorMessage]
                            END,
        [LastHeartbeatAt] = SYSUTCDATETIME()
    WHERE [Status] = 'Running'
      AND DATEADD(SECOND, @StaleTimeoutSeconds, COALESCE([LastHeartbeatAt], [StartedAt], [EnqueuedAt])) <= SYSUTCDATETIME();
END;
GO