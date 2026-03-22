USE [LOG_FI_ALMT]
GO
/****** Object:  Schema [logging]    Script Date: 2/23/2026 5:27:08 PM ******/
CREATE SCHEMA [logging]
GO
/****** Object:  Table [administration].[Flows]    Script Date: 2/23/2026 5:27:08 PM ******/
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
/****** Object:  Table [administration].[ReplayFlows]    Script Date: 2/23/2026 5:27:08 PM ******/
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
/****** Object:  Table [monitoring].[APSActionsLogs]    Script Date: 2/23/2026 5:27:08 PM ******/
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
/****** Object:  Table [monitoring].[DBandBackup]    Script Date: 2/23/2026 5:27:08 PM ******/
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
/****** Object:  Table [monitoring].[DBSizePlusDisk]    Script Date: 2/23/2026 5:27:08 PM ******/
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
ALTER TABLE [administration].[ReplayFlows] ADD  DEFAULT ('Not Started') FOR [ProcessStatus]
GO
/****** Object:  StoredProcedure [monitoring].[UspGetDBBackups]    Script Date: 2/23/2026 5:27:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCedure [monitoring].[UspGetDBBackups]
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
/****** Object:  StoredProcedure [monitoring].[UspGetDbSizePlusDisk]    Script Date: 2/23/2026 5:27:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCedure [monitoring].[UspGetDbSizePlusDisk]
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
/****** Object:  StoredProcedure [monitoring].[spGetDbSizeStats]    Script Date: 2/23/2026 5:27:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
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
    FROM [monitoring].[DBSizePlusDisk]
END
GO
/****** Object:  StoredProcedure [monitoring].[UspInsertAPSActionsLog]    Script Date: 2/23/2026 5:27:08 PM ******/
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
