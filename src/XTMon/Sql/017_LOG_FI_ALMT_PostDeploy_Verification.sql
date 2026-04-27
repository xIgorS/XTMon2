USE [LOG_FI_ALMT]
GO

/*
    Post-deploy verification for 016_LOG_FI_ALMT_Full_Migration.sql.

    Scope:
    - verifies XTMon-managed LOG_FI_ALMT schemas, tables, procedures, and indexes
    - verifies a few drift-sensitive signatures/constraints
    - does not execute write-path procedures

    Notes:
    - Replay.* stored procedures are intentionally not checked here because they live in STAGING_FI_ALMT,
      while this script is only for LOG_FI_ALMT.
    - The script throws at the end if any verification fails.
*/

SET NOCOUNT ON;

DECLARE @Results TABLE
(
    [CheckOrder] INT IDENTITY(1,1) NOT NULL,
    [CheckGroup] NVARCHAR(64) NOT NULL,
    [CheckName] NVARCHAR(256) NOT NULL,
    [Passed] BIT NOT NULL,
    [Details] NVARCHAR(4000) NULL
);

;WITH [RequiredSchemas] AS
(
    SELECT [SchemaName]
    FROM (VALUES
        (N'administration'),
        (N'monitoring')
    ) AS [s]([SchemaName])
)
INSERT INTO @Results ([CheckGroup], [CheckName], [Passed], [Details])
SELECT
    N'Schema',
    [SchemaName],
    CASE WHEN SCHEMA_ID([SchemaName]) IS NOT NULL THEN 1 ELSE 0 END,
    CASE WHEN SCHEMA_ID([SchemaName]) IS NOT NULL THEN NULL ELSE N'Missing schema.' END
FROM [RequiredSchemas];

;WITH [RequiredTables] AS
(
    SELECT [SchemaName], [ObjectName]
    FROM (VALUES
       
        (N'monitoring', N'JvCalculationJobs'),
        (N'monitoring', N'JvCalculationJobResults'),
        (N'monitoring', N'MonitoringJobs'),
        (N'monitoring', N'MonitoringLatestResults')
    ) AS [t]([SchemaName], [ObjectName])
)
INSERT INTO @Results ([CheckGroup], [CheckName], [Passed], [Details])
SELECT
    N'Table',
    QUOTENAME([SchemaName]) + N'.' + QUOTENAME([ObjectName]),
    CASE WHEN OBJECT_ID(QUOTENAME([SchemaName]) + N'.' + QUOTENAME([ObjectName]), N'U') IS NOT NULL THEN 1 ELSE 0 END,
    CASE WHEN OBJECT_ID(QUOTENAME([SchemaName]) + N'.' + QUOTENAME([ObjectName]), N'U') IS NOT NULL THEN NULL ELSE N'Missing table.' END
FROM [RequiredTables];

;WITH [RequiredProcedures] AS
(
    SELECT [SchemaName], [ObjectName]
    FROM (VALUES
        (N'administration', N'UspFailRunningReplayBatches'),
        (N'administration', N'UspFailStaleReplayBatches'),
        (N'administration', N'UspGetStuckReplayBatches'),
       
        (N'monitoring', N'UspGetApplicationLogs'),
        (N'monitoring', N'UspInsertAPSActionsLog'),
        (N'monitoring', N'UspJvJobEnqueue'),
        (N'monitoring', N'UspJvJobTakeNext'),
        (N'monitoring', N'UspJvJobHeartbeat'),
        (N'monitoring', N'UspJvJobSaveResult'),
        (N'monitoring', N'UspJvJobMarkCompleted'),
        (N'monitoring', N'UspJvJobMarkFailed'),
        (N'monitoring', N'UspJvJobMarkCancelled'),
        (N'monitoring', N'UspJvJobCancelActive'),
        (N'monitoring', N'UspJvJobCountActive'),
        (N'monitoring', N'UspJvJobGetStuck'),
        (N'monitoring', N'UspJvJobGetById'),
        (N'monitoring', N'UspJvJobGetLatestByUserPnlDate'),
        (N'monitoring', N'UspJvJobExpireStale'),
        (N'monitoring', N'UspJvJobFailRunning'),
        (N'monitoring', N'UspMonitoringJobEnqueue'),
        (N'monitoring', N'UspMonitoringJobTakeNext'),
        (N'monitoring', N'UspMonitoringJobHeartbeat'),
        (N'monitoring', N'UspMonitoringJobSaveResult'),
        (N'monitoring', N'UspMonitoringJobMarkCompleted'),
        (N'monitoring', N'UspMonitoringJobMarkFailed'),
        (N'monitoring', N'UspMonitoringJobMarkCancelled'),
        (N'monitoring', N'UspMonitoringJobCancelActive'),
        (N'monitoring', N'UspMonitoringJobCountActive'),
        (N'monitoring', N'UspMonitoringJobGetRuntimeByDmv'),
        (N'monitoring', N'UspMonitoringJobRecoverOrphanedRunningByDmv'),
        (N'monitoring', N'UspMonitoringJobGetActive'),
        (N'monitoring', N'UspMonitoringJobGetStuck'),
        (N'monitoring', N'UspMonitoringJobGetById'),
        (N'monitoring', N'UspMonitoringJobGetLatestByKey'),
        (N'monitoring', N'UspMonitoringJobGetLatestByCategory'),
        (N'monitoring', N'UspMonitoringJobGetFullResultCsv'),
        (N'monitoring', N'UspMonitoringJobExpireStale'),
        (N'monitoring', N'UspMonitoringJobSetExecutionContext'),
        (N'monitoring', N'UspSystemDiagnosticsCleanLogging'),
        (N'monitoring', N'UspSystemDiagnosticsCleanHistory')
    ) AS [p]([SchemaName], [ObjectName])
)
INSERT INTO @Results ([CheckGroup], [CheckName], [Passed], [Details])
SELECT
    N'Procedure',
    QUOTENAME([SchemaName]) + N'.' + QUOTENAME([ObjectName]),
    CASE WHEN OBJECT_ID(QUOTENAME([SchemaName]) + N'.' + QUOTENAME([ObjectName]), N'P') IS NOT NULL THEN 1 ELSE 0 END,
    CASE WHEN OBJECT_ID(QUOTENAME([SchemaName]) + N'.' + QUOTENAME([ObjectName]), N'P') IS NOT NULL THEN NULL ELSE N'Missing procedure.' END
FROM [RequiredProcedures];

;WITH [UnexpectedProcedures] AS
(
    SELECT [SchemaName], [ObjectName]
    FROM (VALUES
        (N'monitoring', N'UspGetDBbackups'),
        (N'monitoring', N'UspGetDBSizePlusDisk')
    ) AS [p]([SchemaName], [ObjectName])
)
INSERT INTO @Results ([CheckGroup], [CheckName], [Passed], [Details])
SELECT
    N'Drift',
    QUOTENAME([SchemaName]) + N'.' + QUOTENAME([ObjectName]) + N' absent',
    CASE WHEN OBJECT_ID(QUOTENAME([SchemaName]) + N'.' + QUOTENAME([ObjectName]), N'P') IS NULL THEN 1 ELSE 0 END,
    CASE WHEN OBJECT_ID(QUOTENAME([SchemaName]) + N'.' + QUOTENAME([ObjectName]), N'P') IS NULL THEN NULL ELSE N'Legacy procedure still exists.' END
FROM [UnexpectedProcedures];

;WITH [RequiredIndexes] AS
(
    SELECT [SchemaName], [TableName], [IndexName]
    FROM (VALUES
        (N'monitoring', N'APSActionsLogs', N'IX_APSActionsLogs_TimeStamp_Level'),
        (N'monitoring', N'JvCalculationJobs', N'UX_JvCalculationJobs_Active_User_PnlDate_RequestType'),
        (N'monitoring', N'JvCalculationJobs', N'IX_JvCalculationJobs_Queued_EnqueuedAt_JobId'),
        (N'monitoring', N'JvCalculationJobs', N'IX_JvCalculationJobs_Running_ActivityAt_JobId'),
        (N'monitoring', N'MonitoringJobs', N'UX_MonitoringJobs_Active'),
        (N'monitoring', N'MonitoringJobs', N'IX_MonitoringJobs_Queued_EnqueuedAt_JobId'),
        (N'monitoring', N'MonitoringJobs', N'IX_MonitoringJobs_Running_ActivityAt_JobId'),
        (N'monitoring', N'MonitoringLatestResults', N'UX_MonitoringLatestResults_KeyHash')
    ) AS [i]([SchemaName], [TableName], [IndexName])
)
INSERT INTO @Results ([CheckGroup], [CheckName], [Passed], [Details])
SELECT
    N'Index',
    QUOTENAME([SchemaName]) + N'.' + QUOTENAME([TableName]) + N'.' + QUOTENAME([IndexName]),
    CASE WHEN EXISTS
    (
        SELECT 1
        FROM [sys].[indexes] AS [idx]
        INNER JOIN [sys].[tables] AS [tbl] ON [tbl].[object_id] = [idx].[object_id]
        INNER JOIN [sys].[schemas] AS [sch] ON [sch].[schema_id] = [tbl].[schema_id]
        WHERE [sch].[name] = [RequiredIndexes].[SchemaName]
          AND [tbl].[name] = [RequiredIndexes].[TableName]
          AND [idx].[name] = [RequiredIndexes].[IndexName]
    ) THEN 1 ELSE 0 END,
    CASE WHEN EXISTS
    (
        SELECT 1
        FROM [sys].[indexes] AS [idx]
        INNER JOIN [sys].[tables] AS [tbl] ON [tbl].[object_id] = [idx].[object_id]
        INNER JOIN [sys].[schemas] AS [sch] ON [sch].[schema_id] = [tbl].[schema_id]
        WHERE [sch].[name] = [RequiredIndexes].[SchemaName]
          AND [tbl].[name] = [RequiredIndexes].[TableName]
          AND [idx].[name] = [RequiredIndexes].[IndexName]
    ) THEN NULL ELSE N'Missing index.' END
FROM [RequiredIndexes];

;WITH [ComputedColumns] AS
(
    SELECT [SchemaName], [TableName], [ColumnName]
    FROM (VALUES
        (N'monitoring', N'JvCalculationJobs', N'ActivityAt'),
        (N'monitoring', N'MonitoringJobs', N'ActivityAt')
    ) AS [c]([SchemaName], [TableName], [ColumnName])
)
INSERT INTO @Results ([CheckGroup], [CheckName], [Passed], [Details])
SELECT
    N'Column',
    QUOTENAME([SchemaName]) + N'.' + QUOTENAME([TableName]) + N'.' + QUOTENAME([ColumnName]) + N' computed',
    CASE WHEN EXISTS
    (
        SELECT 1
        FROM [sys].[columns] AS [col]
        INNER JOIN [sys].[tables] AS [tbl] ON [tbl].[object_id] = [col].[object_id]
        INNER JOIN [sys].[schemas] AS [sch] ON [sch].[schema_id] = [tbl].[schema_id]
        WHERE [sch].[name] = [ComputedColumns].[SchemaName]
          AND [tbl].[name] = [ComputedColumns].[TableName]
          AND [col].[name] = [ComputedColumns].[ColumnName]
          AND [col].[is_computed] = 1
    ) THEN 1 ELSE 0 END,
    CASE WHEN EXISTS
    (
        SELECT 1
        FROM [sys].[columns] AS [col]
        INNER JOIN [sys].[tables] AS [tbl] ON [tbl].[object_id] = [col].[object_id]
        INNER JOIN [sys].[schemas] AS [sch] ON [sch].[schema_id] = [tbl].[schema_id]
        WHERE [sch].[name] = [ComputedColumns].[SchemaName]
          AND [tbl].[name] = [ComputedColumns].[TableName]
          AND [col].[name] = [ComputedColumns].[ColumnName]
          AND [col].[is_computed] = 1
    ) THEN NULL ELSE N'Computed column missing or not computed.' END
FROM [ComputedColumns];

INSERT INTO @Results ([CheckGroup], [CheckName], [Passed], [Details])
SELECT
        N'Column',
        N'[monitoring].[APSActionsLogs].[TimeStamp] datatype',
        CASE WHEN EXISTS
        (
                SELECT 1
                FROM [sys].[columns] AS [col]
                INNER JOIN [sys].[tables] AS [tbl] ON [tbl].[object_id] = [col].[object_id]
                INNER JOIN [sys].[schemas] AS [sch] ON [sch].[schema_id] = [tbl].[schema_id]
                INNER JOIN [sys].[types] AS [typ] ON [typ].[user_type_id] = [col].[user_type_id]
                WHERE [sch].[name] = N'monitoring'
                    AND [tbl].[name] = N'APSActionsLogs'
                    AND [col].[name] = N'TimeStamp'
                    AND [typ].[name] = N'datetime2'
                    AND [col].[scale] = 3
        ) THEN 1 ELSE 0 END,
        CASE WHEN EXISTS
        (
                SELECT 1
                FROM [sys].[columns] AS [col]
                INNER JOIN [sys].[tables] AS [tbl] ON [tbl].[object_id] = [col].[object_id]
                INNER JOIN [sys].[schemas] AS [sch] ON [sch].[schema_id] = [tbl].[schema_id]
                INNER JOIN [sys].[types] AS [typ] ON [typ].[user_type_id] = [col].[user_type_id]
                WHERE [sch].[name] = N'monitoring'
                    AND [tbl].[name] = N'APSActionsLogs'
                    AND [col].[name] = N'TimeStamp'
                    AND [typ].[name] = N'datetime2'
                    AND [col].[scale] = 3
        ) THEN NULL ELSE N'Expected datetime2(3) column.' END;

    INSERT INTO @Results ([CheckGroup], [CheckName], [Passed], [Details])
    SELECT
        N'Column',
        N'[monitoring].[MonitoringLatestResults].[FullResultCsvGzip] datatype',
        CASE WHEN EXISTS
        (
            SELECT 1
            FROM [sys].[columns] AS [col]
            INNER JOIN [sys].[tables] AS [tbl] ON [tbl].[object_id] = [col].[object_id]
            INNER JOIN [sys].[schemas] AS [sch] ON [sch].[schema_id] = [tbl].[schema_id]
            INNER JOIN [sys].[types] AS [typ] ON [typ].[user_type_id] = [col].[user_type_id]
            WHERE [sch].[name] = N'monitoring'
                AND [tbl].[name] = N'MonitoringLatestResults'
                AND [col].[name] = N'FullResultCsvGzip'
                AND [typ].[name] = N'varbinary'
                AND [col].[max_length] = -1
        ) THEN 1 ELSE 0 END,
        CASE WHEN EXISTS
        (
            SELECT 1
            FROM [sys].[columns] AS [col]
            INNER JOIN [sys].[tables] AS [tbl] ON [tbl].[object_id] = [col].[object_id]
            INNER JOIN [sys].[schemas] AS [sch] ON [sch].[schema_id] = [tbl].[schema_id]
            INNER JOIN [sys].[types] AS [typ] ON [typ].[user_type_id] = [col].[user_type_id]
            WHERE [sch].[name] = N'monitoring'
                AND [tbl].[name] = N'MonitoringLatestResults'
                AND [col].[name] = N'FullResultCsvGzip'
                AND [typ].[name] = N'varbinary'
                AND [col].[max_length] = -1
        ) THEN NULL ELSE N'Expected varbinary(max) column.' END;

INSERT INTO @Results ([CheckGroup], [CheckName], [Passed], [Details])
SELECT
    N'Constraint',
    N'[administration].[ReplayFlows].[ProcessStatus] default constraint',
    CASE WHEN EXISTS
    (
        SELECT 1
        FROM [sys].[default_constraints] AS [dc]
        INNER JOIN [sys].[columns] AS [col]
            ON [col].[object_id] = [dc].[parent_object_id]
           AND [col].[column_id] = [dc].[parent_column_id]
        INNER JOIN [sys].[tables] AS [tbl] ON [tbl].[object_id] = [dc].[parent_object_id]
        INNER JOIN [sys].[schemas] AS [sch] ON [sch].[schema_id] = [tbl].[schema_id]
        WHERE [sch].[name] = N'administration'
          AND [tbl].[name] = N'ReplayFlows'
          AND [col].[name] = N'ProcessStatus'
          AND [dc].[name] = N'DF_ReplayFlows_ProcessStatus'
    ) THEN 1 ELSE 0 END,
    CASE WHEN EXISTS
    (
        SELECT 1
        FROM [sys].[default_constraints] AS [dc]
        INNER JOIN [sys].[columns] AS [col]
            ON [col].[object_id] = [dc].[parent_object_id]
           AND [col].[column_id] = [dc].[parent_column_id]
        INNER JOIN [sys].[tables] AS [tbl] ON [tbl].[object_id] = [dc].[parent_object_id]
        INNER JOIN [sys].[schemas] AS [sch] ON [sch].[schema_id] = [tbl].[schema_id]
        WHERE [sch].[name] = N'administration'
          AND [tbl].[name] = N'ReplayFlows'
          AND [col].[name] = N'ProcessStatus'
          AND [dc].[name] = N'DF_ReplayFlows_ProcessStatus'
    ) THEN NULL ELSE N'Expected default constraint is missing.' END;

;WITH [RequiredParameterCounts] AS
(
    SELECT [SchemaName], [ObjectName], [ExpectedCount]
    FROM (VALUES
        (N'monitoring', N'UspGetApplicationLogs', 5),
        (N'monitoring', N'UspJvJobGetLatestByUserPnlDate', 3),
        (N'monitoring', N'UspJvJobTakeNext', 1),
        (N'monitoring', N'UspMonitoringJobCancelActive', 1),
        (N'monitoring', N'UspMonitoringJobGetFullResultCsv', 1),
        (N'monitoring', N'UspMonitoringJobSaveResult', 6),
        (N'monitoring', N'UspMonitoringJobTakeNext', 4),
        (N'monitoring', N'UspSystemDiagnosticsCleanHistory', 4)
    ) AS [p]([SchemaName], [ObjectName], [ExpectedCount])
)
INSERT INTO @Results ([CheckGroup], [CheckName], [Passed], [Details])
SELECT
    N'Signature',
    QUOTENAME([SchemaName]) + N'.' + QUOTENAME([ObjectName]) + N' parameter count',
    CASE WHEN [actual].[ParameterCount] = [ExpectedCount] THEN 1 ELSE 0 END,
    CONCAT(N'Expected=', [ExpectedCount], N'; Found=', COALESCE(CONVERT(NVARCHAR(20), [actual].[ParameterCount]), N'0'))
FROM [RequiredParameterCounts]
OUTER APPLY
(
    SELECT COUNT(*) AS [ParameterCount]
    FROM [sys].[parameters] AS [prm]
    WHERE [prm].[object_id] = OBJECT_ID(QUOTENAME([RequiredParameterCounts].[SchemaName]) + N'.' + QUOTENAME([RequiredParameterCounts].[ObjectName]), N'P')
) AS [actual];

;WITH [RequiredParameters] AS
(
    SELECT [SchemaName], [ObjectName], [ParameterName]
    FROM (VALUES
        (N'monitoring', N'UspGetApplicationLogs', N'@TopN'),
        (N'monitoring', N'UspGetApplicationLogs', N'@FromTimeStamp'),
        (N'monitoring', N'UspGetApplicationLogs', N'@ToTimeStamp'),
        (N'monitoring', N'UspGetApplicationLogs', N'@LevelsCsv'),
        (N'monitoring', N'UspGetApplicationLogs', N'@MessageContains'),
        (N'monitoring', N'UspMonitoringJobGetFullResultCsv', N'@MonitoringJobId'),
        (N'monitoring', N'UspMonitoringJobSaveResult', N'@FullResultCsvGzip'),
        (N'monitoring', N'UspMonitoringJobTakeNext', N'@IncludedSubmenuKeysCsv'),
        (N'monitoring', N'UspMonitoringJobTakeNext', N'@ExcludedSubmenuKeysCsv'),
        (N'monitoring', N'UspJvJobGetLatestByUserPnlDate', N'@UserId'),
        (N'monitoring', N'UspJvJobGetLatestByUserPnlDate', N'@PnlDate'),
        (N'monitoring', N'UspJvJobGetLatestByUserPnlDate', N'@RequestType')
    ) AS [p]([SchemaName], [ObjectName], [ParameterName])
)
INSERT INTO @Results ([CheckGroup], [CheckName], [Passed], [Details])
SELECT
    N'Signature',
    QUOTENAME([SchemaName]) + N'.' + QUOTENAME([ObjectName]) + N' has ' + [ParameterName],
    CASE WHEN EXISTS
    (
        SELECT 1
        FROM [sys].[parameters] AS [prm]
        WHERE [prm].[object_id] = OBJECT_ID(QUOTENAME([RequiredParameters].[SchemaName]) + N'.' + QUOTENAME([RequiredParameters].[ObjectName]), N'P')
          AND [prm].[name] = [RequiredParameters].[ParameterName]
    ) THEN 1 ELSE 0 END,
    CASE WHEN EXISTS
    (
        SELECT 1
        FROM [sys].[parameters] AS [prm]
        WHERE [prm].[object_id] = OBJECT_ID(QUOTENAME([RequiredParameters].[SchemaName]) + N'.' + QUOTENAME([RequiredParameters].[ObjectName]), N'P')
          AND [prm].[name] = [RequiredParameters].[ParameterName]
    ) THEN NULL ELSE N'Missing parameter.' END
FROM [RequiredParameters];

SELECT
    [CheckGroup],
    [CheckName],
    [Passed],
    [Details]
FROM @Results
ORDER BY [Passed] ASC, [CheckGroup] ASC, [CheckOrder] ASC;

IF EXISTS (SELECT 1 FROM @Results WHERE [Passed] = 0)
BEGIN
    ;THROW 51000, 'LOG_FI_ALMT post-deploy verification failed. Review the result set above.', 1;
END

PRINT 'LOG_FI_ALMT post-deploy verification passed.';

/*
Optional read-only smoke tests after the metadata checks pass.
Run these manually only if you want extra confidence in the read paths.

EXEC [monitoring].[UspGetDBBackups];
EXEC [monitoring].[UspGetDbSizePlusDisk];
EXEC [monitoring].[UspGetApplicationLogs] @TopN = 5;
EXEC [monitoring].[spGetDbSizeStats];
EXEC [administration].[UspGetStuckReplayBatches];

EXEC [monitoring].[UspJvJobGetById] @JobId = -1;
EXEC [monitoring].[UspJvJobGetLatestByUserPnlDate] @UserId = NULL, @PnlDate = '2000-01-01', @RequestType = NULL;

EXEC [monitoring].[UspMonitoringJobGetById] @JobId = -1;
EXEC [monitoring].[UspMonitoringJobGetLatestByKey] @Category = 'Verification', @SubmenuKey = 'Verification', @PnlDate = '2000-01-01';
EXEC [monitoring].[UspMonitoringJobGetLatestByCategory] @Category = 'Verification', @PnlDate = '2000-01-01';
*/