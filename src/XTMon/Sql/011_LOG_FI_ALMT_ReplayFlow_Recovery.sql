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
