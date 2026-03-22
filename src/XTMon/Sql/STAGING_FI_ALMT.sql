USE [STAGING_FI_ALMT]
GO
/****** Object:  StoredProcedure [Replay].[UspProcessReplayFlows]    Script Date: 2/27/2026 6:22:23 PM ******/
DROP PROCEDURE IF EXISTS [Replay].[UspProcessReplayFlows]
GO
/****** Object:  StoredProcedure [Replay].[UspInsertReplayFlows]    Script Date: 2/27/2026 6:22:23 PM ******/
DROP PROCEDURE IF EXISTS [Replay].[UspInsertReplayFlows]
GO
/****** Object:  StoredProcedure [Replay].[UspGetReplayFlowStatus]    Script Date: 2/27/2026 6:22:23 PM ******/
DROP PROCEDURE IF EXISTS [Replay].[UspGetReplayFlowStatus]
GO
/****** Object:  StoredProcedure [Replay].[UspGetReplayFlowProcessStatus]    Script Date: 2/27/2026 6:22:23 PM ******/
DROP PROCEDURE IF EXISTS [Replay].[UspGetReplayFlowProcessStatus]
GO
/****** Object:  StoredProcedure [Replay].[UspGetPnlDates]    Script Date: 2/27/2026 6:22:23 PM ******/
DROP PROCEDURE IF EXISTS [Replay].[UspGetPnlDates]
GO
/****** Object:  UserDefinedTableType [Replay].[ReplayAdjAtCoreSet]    Script Date: 2/27/2026 6:22:23 PM ******/
DROP TYPE IF EXISTS [Replay].[ReplayAdjAtCoreSet]
GO
/****** Object:  UserDefinedTableType [Replay].[ReplayAdjAtCoreSet]    Script Date: 2/27/2026 6:22:23 PM ******/
CREATE TYPE [Replay].[ReplayAdjAtCoreSet] AS TABLE(
	[FlowIdDerivedFrom] [bigint] NULL,
	[FlowId] [bigint] NULL,
	[PnlDate] [date] NULL,
	[PackageGuid] [uniqueidentifier] NULL,
	[WithBackdated] [bit] NULL,
	[SkipCoreProcess] [bit] NULL,
	[DropTableTmp] [bit] NULL
)
GO
/****** Object:  StoredProcedure [Replay].[UspGetPnlDates]    Script Date: 2/27/2026 6:22:23 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [Replay].[UspGetPnlDates]
    @DefaultDate Date OUTPUT
AS
/*
test for replay.uspGetPnlDates
declare @DefaultDate date   
EXEC [replay].[UspGetPnlDates] @DefaultDate = @DefaultDate OUTPUT
select @DefaultDate
*/
BEGIN
    DECLARE @pnlDate date = '2026-02-13'
    SET NOCOUNT ON;

    SELECT  @DefaultDate = COALESCE(@DefaultDate, @pnlDate)
    
   /* write 10 days to the past from @pnlDate
    SELECT DATEADD(DAY, -number, @pnlDate) AS PnlDate
    FROM master..spt_values
    WHERE type = 'P' AND number <= 10
    ORDER BY PnlDate DESC
    */

    SELECT DATEADD(DAY, -number, @pnlDate) AS PnlDate
    FROM master..spt_values
    WHERE type = 'P' AND number <= 10
    ORDER BY PnlDate DESC

END
GO
/****** Object:  StoredProcedure [Replay].[UspGetReplayFlowProcessStatus]    Script Date: 2/27/2026 6:22:23 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [Replay].[UspGetReplayFlowProcessStatus]
    
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @DateCompleted datetime = Getdate(), @ProcessStatus varchar(20) = 'Init'

    UPDATE LOG_FI_ALMT.[administration].[ReplayFlows]
        SET ProcessStatus = @ProcessStatus
        , DateCompleted = CASE 
                                WHEN @ProcessStatus in ('error', 'done') THEN @DateCompleted 
                                ELSE DateCompleted END
        --join on PackageGuid        
    --FROM LOG_FI_ALMT.[administration].[ReplayFlows]
    WHERE ProcessStatus not in ('error', 'done')
    
END
GO
/****** Object:  StoredProcedure [Replay].[UspGetReplayFlowStatus]    Script Date: 2/27/2026 6:22:23 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [Replay].[UspGetReplayFlowStatus]
    @PnlDate date = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @PnlDate IS NULL
        SET @PnlDate = (
            SELECT MAX(PnlDate)
            FROM [LOG_FI_ALMT].[administration].[ReplayFlows]
        );

    SELECT
        FlowId,
        FlowIdDerivedFrom,
        PnlDate,
        PackageGuid,
        WithBackdated,
        SkipCoreProcess,
        DropTableTmp,
        DateCreated,
        CreatedBy,
        DateSubmitted,
        DateStarted,
        DateCompleted,
        ReplayStatus,
        ProcessStatus,
        DATEDIFF(SECOND, DateStarted, ISNULL(DateCompleted, GETDATE())) AS DurationSeconds
    FROM LOG_FI_ALMT.[administration].[ReplayFlows]
    WHERE PnlDate = @PnlDate
    ORDER BY DateCreated DESC;
END;
GO
/****** Object:  StoredProcedure [Replay].[UspInsertReplayFlows]    Script Date: 2/27/2026 6:22:23 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Replay].[UspInsertReplayFlows] @UserId varchar(100), 
    @FlowData Replay.ReplayAdjAtCoreSet READONLY
AS
BEGIN
    SET NOCOUNT ON;
    --select * from LOG_FI_ALMT.administration.ReplayFlows
    Declare @DateCreated datetime = GetDate();
    insert LOG_FI_ALMT.administration.ReplayFlows
    (
    FlowId,
    FlowIdDerivedFrom, PnlDate, PackageGuid, WithBackdated, SkipCoreProcess, 
    DropTableTmp, DateCreated, CreatedBy, ReplayStatus)

    SELECT
        FlowId,
        FlowIdDerivedFrom,
        PnlDate,
        PackageGuid,
        WithBackdated,
        SkipCoreProcess,
        DropTableTmp,
        @DateCreated,
        @UserId,
        'Inserted'

    FROM @FlowData

    SELECT FlowId,
    FlowIdDerivedFrom, PnlDate, PackageGuid, WithBackdated, SkipCoreProcess, 
    DropTableTmp, DateCreated, CreatedBy from LOG_FI_ALMT.administration.ReplayFlows
    where DateCreated = @DateCreated and CreatedBy = @UserId

END
GO
/****** Object:  StoredProcedure [Replay].[UspProcessReplayFlows]    Script Date: 2/27/2026 6:22:23 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [Replay].[UspProcessReplayFlows]
AS
BEGIN
    SET NOCOUNT ON;
    -- truncate table LOG_FI_ALMT.[administration].[ReplayFlows]
    DECLARE @FlowId bigint;
    DECLARE @PnlDate date;
    DECLARE @DateStarted datetime;
    DECLARE @DateCompleted datetime;
    
    DECLARE flow_cursor CURSOR FOR
        SELECT FlowId, PnlDate
        FROM LOG_FI_ALMT.[administration].[ReplayFlows]
        WHERE DateStarted IS NULL;
    
    OPEN flow_cursor;
    
    FETCH NEXT FROM flow_cursor 
    INTO @FlowId, @PnlDate;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Update DateStarted
        SET @DateStarted = GETDATE();
        
        UPDATE LOG_FI_ALMT.[administration].[ReplayFlows]
        SET DateStarted = @DateStarted, ReplayStatus = 'Submission Started'
        WHERE FlowId = @FlowId 
          AND PnlDate = @PnlDate
          AND DateStarted IS NULL;
        
        -- Wait for 5 seconds
        WAITFOR DELAY '00:00:05';
        
        -- Update DateCompleted
        SET @DateCompleted = GETDATE();
        
        UPDATE LOG_FI_ALMT.[administration].[ReplayFlows]
        SET DateSubmitted = @DateCompleted, ReplayStatus = 'Submission Completed'
        WHERE FlowId = @FlowId 
          AND PnlDate = @PnlDate and DateStarted = @DateStarted
        
        FETCH NEXT FROM flow_cursor 
                INTO @FlowId, @PnlDate;
    END;
    
    CLOSE flow_cursor;
    DEALLOCATE flow_cursor;
END;
GO
