# Monitoring Job Rollout Checklist

This checklist covers the monitoring-job fixes for Data Validation batch freezes and restart recovery in `LogFiAlmt`.

## Scope

Changed objects:

- `monitoring.UspMonitoringJobTakeNext`
- `monitoring.UspMonitoringJobGetLatestByCategory`
- `monitoring.UspMonitoringJobExpireStale`

Changed source files:

- `src/XTMon/Sql/004_LOG_FI_ALMT_MonitoringJob_Orchestration.sql`
- `src/XTMon/Sql/005_LOG_FI_ALMT_JvAndMonitoringJob_Release.sql`
- `src/XTMon/Sql/011_LOG_FI_ALMT_ReplayFlow_Recovery.sql`

## Choose The Correct Deployment Path

### Path A: Existing environment, preserve orchestration data

Use `004_LOG_FI_ALMT_MonitoringJob_Orchestration.sql`.

Reason:

- It uses `CREATE OR ALTER PROCEDURE` for the monitoring-job objects.
- It preserves existing `MonitoringJobs` and `MonitoringLatestResults` rows.
- It is the safe path for environments already running Data Validation or Functional Rejection jobs.

### Path B: Rebuild environment, destructive reset is acceptable

Use `005_LOG_FI_ALMT_JvAndMonitoringJob_Release.sql`.

Reason:

- It recreates JV and Monitoring orchestration objects together.
- It is destructive and clears orchestration history plus latest-result data.

Do not use Path B on a live environment unless losing job history and latest cached results is explicitly acceptable.

## Pre-Deployment Checks

1. Confirm the target database is `LogFiAlmt`.
2. Confirm no deployment is running against the destructive release script by mistake.
3. Record current definitions for rollback/reference:

```sql
SELECT [o].[name], [m].[definition]
FROM sys.objects AS [o]
INNER JOIN sys.sql_modules AS [m] ON [m].[object_id] = [o].[object_id]
WHERE [o].[schema_id] = SCHEMA_ID('monitoring')
  AND [o].[name] IN (
      'UspMonitoringJobTakeNext',
      'UspMonitoringJobGetLatestByCategory',
      'UspMonitoringJobExpireStale'
  );
```

4. Check for active monitoring jobs before deployment:

```sql
SELECT [Status], COUNT(*) AS [JobCount]
FROM [monitoring].[MonitoringJobs]
GROUP BY [Status]
ORDER BY [Status];
```

5. If any rows are `Running`, either wait for them to finish or perform deployment in a maintenance window.

## Deployment Steps

### Safe production-style rollout

1. Connect to the `LogFiAlmt` database.
2. Run the procedure changes from `src/XTMon/Sql/004_LOG_FI_ALMT_MonitoringJob_Orchestration.sql`.
3. Run `src/XTMon/Sql/011_LOG_FI_ALMT_ReplayFlow_Recovery.sql` to create the replay startup-recovery and diagnostics procedures required by current XTMon config.
4. Do not rerun unrelated object creation unless the environment requires it.
5. Restart the XTMon application after the SQL deployment so the worker picks up the new behavior immediately.

### Destructive rebuild rollout

1. Confirm orchestration history can be discarded.
2. Connect to the `LogFiAlmt` database.
3. Run `src/XTMon/Sql/005_LOG_FI_ALMT_JvAndMonitoringJob_Release.sql`.
4. The destructive release script now also recreates the replay recovery procedures used by startup recovery and System Diagnostics.
5. Restart the XTMon application.

## What To Verify After Deployment

### 1. Procedure body checks

`UspMonitoringJobTakeNext` and `UspMonitoringJobExpireStale` should contain:

```sql
SET LOCK_TIMEOUT 5000;
```

`UspMonitoringJobGetLatestByCategory` should contain:

```sql
FROM [monitoring].[MonitoringJobs] AS [jobs] WITH (NOLOCK)

NULL AS [ParsedQuery],
NULL AS [GridColumnsJson],
NULL AS [GridRowsJson],
```

Quick check:

```sql
SELECT [o].[name], [m].[definition]
FROM sys.objects AS [o]
INNER JOIN sys.sql_modules AS [m] ON [m].[object_id] = [o].[object_id]
WHERE [o].[schema_id] = SCHEMA_ID('monitoring')
  AND [o].[name] IN (
      'UspMonitoringJobTakeNext',
      'UspMonitoringJobGetLatestByCategory',
      'UspMonitoringJobExpireStale'
  );
```

### 2. Category polling returns lightweight rows

Run:

```sql
EXEC [monitoring].[UspMonitoringJobGetLatestByCategory]
    @Category = 'DataValidation',
    @PnlDate = '2026-04-21';
```

Expected:

- `ParsedQuery` is `NULL`
- `GridColumnsJson` is `NULL`
- `GridRowsJson` is `NULL`
- `MetadataJson` remains populated when a completed result exists

### 3. Worker recovery behavior

After app restart, check application logs for the new stage-specific messages around stale-expiry and take-next instead of a single opaque poll failure.

Expected symptoms improvement:

- startup is responsive even when SQL lock contention occurs
- Data Validation batch status refresh no longer drags large result payloads across repeated polls

## Rollback Guidance

If the deployment needs to be rolled back:

1. Reapply the previously captured procedure definitions from the pre-deployment query.
2. Restart the XTMon application.
3. Recheck monitoring-job polling and batch responsiveness.

If the deployment used the destructive release script, rollback of cleared orchestration data is not possible without a database backup.