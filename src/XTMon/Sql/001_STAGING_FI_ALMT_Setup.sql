USE [STAGING_FI_ALMT]
GO
/****** Object:  UserDefinedTableType [Replay].[ReplayAdjAtCoreSet]    Script Date: 2/23/2026 5:24:20 PM ******/
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
/****** Object:  StoredProcedure [Replay].[UspGetFailedFlows]    Script Date: 2/23/2026 5:24:20 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [Replay].[UspGetFailedFlows] 
    @PnlDate date, 
    @ReplayFlowSet varchar(1000) = null
AS
/*
    [Replay].[UspGetFailedFlows] @PnlDate, @ReplayFlowSet
    Description:
    Retrieves failed flows for a given PnlDate. If @ReplayFlowSet is provided, it filters flows based on the specified FlowIds.
    Parameters:
    @PnlDate - The P&L date to filter flows. If null, defaults to the latest PnlDate in the Flows table.
    @ReplayFlowSet - Optional comma-separated list of FlowIds to filter the results.
    Returns:
    A result set containing details of failed flows, including FlowId, BusinessDataType, FeedSource, and other relevant information.
    Test example: 
    [Replay].[UspGetFailedFlows]
    null, 
    '302374045,302380547'
    [Replay].[UspGetFailedFlows]
    null, 
    null
*/

BEGIN
    SET NOCOUNT ON;
    
    -- Handle null @PnlDate
    IF @PnlDate IS NULL
        SET @PnlDate = (SELECT MAX(PnlDate) FROM [LOG_FI_ALMT].[administration].[Flows]);
    
    -- Create temp table for flows status
    SELECT FlowId, FlowIdDerivedFrom, PnlDate 
    INTO #StatusFlows
    FROM [LOG_FI_ALMT].[administration].[ReplayFlows]
    WHERE DateCompleted IS NULL;
    
    -- Create temp table for results
    CREATE TABLE #FailedFlows (
        FlowId bigint,
        FlowIdDerivedFrom bigint,
        BusinessDataType varchar(100),
        FeedSource varchar(100),
        PackageGuid uniqueidentifier,
        PnlDate date,
        FileName varchar(500),
        ArrivalDate datetime,
        CurrentStep varchar(100),
        IsFailed bit,
        TypeOfCalculation varchar(100),
        IsReplay bit,
        IsAdjustment bit,
        WithBackdated bit,
        SkipCoreProcess bit,
        DropTableTmp bit
    );
    
    IF @ReplayFlowSet IS NOT NULL
    BEGIN
        -- Create temp table for parsed FlowIds
        CREATE TABLE #FlowIdsToReplay (FlowId bigint);
        
        -- Parse comma-separated integers
        INSERT INTO #FlowIdsToReplay (FlowId)
        SELECT CAST(value AS bigint)
        FROM STRING_SPLIT(@ReplayFlowSet, ',')
        WHERE ISNUMERIC(value) = 1;
        
        -- Insert filtered flows
        INSERT INTO #FailedFlows
        SELECT
            f.FlowId,
            f.FlowIdDerivedFrom,
            CAST(BusinessDataTypeId AS varchar(50)) + '-BusinessDataType',
            CAST(FeedSourceId AS varchar(50)) + '-FeedSourceName',
            PackageGuid,
            f.PnlDate,
            FileName,
            ArrivalDate,
            CurrentStep,
            1,
            TypeOfCalculation,
            1,
            1,
            1,
            0,
            0
        FROM [LOG_FI_ALMT].[administration].[Flows] f 
        INNER JOIN #FlowIdsToReplay r ON f.FlowId = r.FlowId
        WHERE NOT EXISTS (
            SELECT 1 
            FROM #StatusFlows s 
            WHERE f.FlowId = s.FlowId 
              AND f.FlowIdDerivedFrom = s.FlowIdDerivedFrom 
              AND f.PnlDate = s.PnlDate
        );
        
        DROP TABLE #FlowIdsToReplay;
    END
    ELSE
    BEGIN
        -- Insert top 5 failed flows for given PnlDate
        INSERT INTO #FailedFlows
        SELECT TOP 5 
            f.FlowId,
            f.FlowIdDerivedFrom,
            CAST(BusinessDataTypeId AS varchar(50)) + '-BusinessDataType',
            CAST(FeedSourceId AS varchar(50)) + '-FeedSourceName',
            PackageGuid,
            f.PnlDate,
            FileName,
            ArrivalDate,
            CurrentStep,
            1,
            TypeOfCalculation,
            1,
            1,
            1,
            0,
            0
        FROM [LOG_FI_ALMT].[administration].[Flows] f  
        WHERE f.PnlDate = @PnlDate
          AND NOT EXISTS (
              SELECT 1 
              FROM #StatusFlows s 
              WHERE f.FlowId = s.FlowId 
                AND f.FlowIdDerivedFrom = s.FlowIdDerivedFrom 
                AND f.PnlDate = s.PnlDate
          )
        ORDER BY f.FlowId;
    END
    
    -- Return results with explicit column list
    SELECT 
        FlowId,
        FlowIdDerivedFrom,
        BusinessDataType,
        FeedSource,
        PackageGuid,
        PnlDate,
        [FileName],
        ArrivalDate,
        CurrentStep,
        IsFailed,
        TypeOfCalculation,
        IsReplay,
        IsAdjustment,
        WithBackdated,
        SkipCoreProcess,
        DropTableTmp
    FROM #FailedFlows;
    
    -- Clean up
    DROP TABLE #StatusFlows;
    DROP TABLE #FailedFlows;
END

GO
/****** Object:  StoredProcedure [Replay].[UspGetReplayFlowProcessStatus]    Script Date: 2/23/2026 5:24:20 PM ******/
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
/****** Object:  StoredProcedure [Replay].[UspGetReplayFlowStatus]    Script Date: 2/23/2026 5:24:20 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- Created by GitHub Copilot in SSMS - review carefully before executing
CREATE   PROCEDURE [Replay].[UspGetReplayFlowStatus]
    @PnlDate date = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @PnlDate IS NULL
        SET @PnlDate = (
            SELECT MAX(PnlDate)
            FROM [LOG_FI_ALMT].[administration].[ReplayFlows]
        )

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
    ORDER BY DateCreated DESC
END
GO
/****** Object:  StoredProcedure [Replay].[UspInsertReplayFlows]    Script Date: 2/23/2026 5:24:20 PM ******/
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
/****** Object:  StoredProcedure [Replay].[UspProcessReplayFlows]    Script Date: 2/23/2026 5:24:20 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [Replay].[UspProcessReplayFlows]
AS
BEGIN
    SET NOCOUNT ON;
    
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
        SET DateCompleted = @DateCompleted, ReplayStatus = 'Submission Completed'
        WHERE FlowId = @FlowId 
          AND PnlDate = @PnlDate and DateStarted = @DateStarted
        
        FETCH NEXT FROM flow_cursor 
                INTO @FlowId, @PnlDate;
    END;
    
    CLOSE flow_cursor;
    DEALLOCATE flow_cursor;
END;
GO
