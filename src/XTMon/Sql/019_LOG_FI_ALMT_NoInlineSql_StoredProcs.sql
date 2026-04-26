USE [LOG_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspJvJobExpireStale]
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

        SELECT @@ROWCOUNT AS [ExpiredCount];
END
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspJvJobMarkCancelled]
    @JobId BIGINT,
    @ErrorMessage NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE [monitoring].[JvCalculationJobs]
    SET
        [Status] = 'Cancelled',
        [FailedAt] = CASE WHEN [StartedAt] IS NULL THEN NULL ELSE SYSUTCDATETIME() END,
        [CompletedAt] = NULL,
        [ErrorMessage] = @ErrorMessage,
        [LastHeartbeatAt] = SYSUTCDATETIME()
    WHERE [JobId] = @JobId
      AND [Status] IN ('Running', 'Queued');
END
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspJvJobCancelActive]
    @ErrorMessage NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE [monitoring].[JvCalculationJobs]
    SET
        [Status] = 'Cancelled',
        [FailedAt] = CASE WHEN [StartedAt] IS NULL THEN NULL ELSE SYSUTCDATETIME() END,
        [CompletedAt] = NULL,
        [ErrorMessage] = @ErrorMessage,
        [LastHeartbeatAt] = SYSUTCDATETIME()
    WHERE [Status] IN ('Queued', 'Running');

    SELECT @@ROWCOUNT AS [CancelledCount];
END
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspJvJobCountActive]
AS
BEGIN
    SET NOCOUNT ON;

    SELECT COUNT_BIG(*) AS [ActiveCount]
    FROM [monitoring].[JvCalculationJobs]
    WHERE [Status] IN ('Queued', 'Running');
END
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspJvJobGetStuck]
    @ThresholdSeconds INT
AS
BEGIN
    SET NOCOUNT ON;

    SELECT [JobId], [UserId], [PnlDate], [RequestType], [Status], [WorkerId],
           [EnqueuedAt], [StartedAt], [LastHeartbeatAt], [CompletedAt], [FailedAt], [ErrorMessage],
           CAST(NULL AS NVARCHAR(MAX)) AS [QueryCheck],
           CAST(NULL AS NVARCHAR(MAX)) AS [QueryFix],
           CAST(NULL AS NVARCHAR(MAX)) AS [GridColumnsJson],
           CAST(NULL AS NVARCHAR(MAX)) AS [GridRowsJson],
           CAST(NULL AS DATETIME2(3))  AS [SavedAt]
    FROM [monitoring].[JvCalculationJobs]
    WHERE [Status] = 'Running'
      AND DATEDIFF(SECOND, [ActivityAt], SYSUTCDATETIME()) > @ThresholdSeconds
    ORDER BY [ActivityAt];
END
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspJvJobFailRunning]
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
    WHERE [Status] = 'Running';

    SELECT @@ROWCOUNT AS [FailedCount];
END
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspMonitoringJobCountActive]
AS
BEGIN
    SET NOCOUNT ON;

    SELECT COUNT_BIG(*) AS [ActiveCount]
    FROM [monitoring].[MonitoringJobs]
    WHERE [Status] IN ('Queued', 'Running');
END
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspMonitoringJobGetStuck]
    @ThresholdSeconds INT
AS
BEGIN
    SET NOCOUNT ON;

    SELECT [JobId], [Category], [SubmenuKey], [DisplayName], [PnlDate], [Status], [WorkerId],
           [ParametersJson], [ParameterSummary], [EnqueuedAt], [StartedAt], [LastHeartbeatAt],
           [CompletedAt], [FailedAt], [ErrorMessage],
           CAST(NULL AS NVARCHAR(MAX)) AS [ParsedQuery],
           CAST(NULL AS NVARCHAR(MAX)) AS [GridColumnsJson],
           CAST(NULL AS NVARCHAR(MAX)) AS [GridRowsJson],
           CAST(NULL AS NVARCHAR(MAX)) AS [MetadataJson],
           CAST(NULL AS DATETIME2(0))  AS [SavedAt]
    FROM [monitoring].[MonitoringJobs]
    WHERE [Status] = 'Running'
      AND DATEDIFF(SECOND, [ActivityAt], SYSUTCDATETIME()) > @ThresholdSeconds
    ORDER BY [ActivityAt];
END
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspMonitoringJobSetExecutionContext]
    @JobId BIGINT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Context VARBINARY(128) = 0x;

    IF @JobId IS NOT NULL
    BEGIN
        SET @Context = CONVERT(VARBINARY(128), 0x58544D4F4E4A4F42) + CONVERT(BINARY(8), @JobId);
    END

    SET CONTEXT_INFO @Context;
END
GO