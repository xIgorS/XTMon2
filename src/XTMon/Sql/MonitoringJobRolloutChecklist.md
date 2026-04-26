# Monitoring Job Rollout Checklist

This checklist covers the monitoring-job fixes for Data Validation batch freezes and restart recovery in `LogFiAlmt`.

## Scope

Changed objects:

- `monitoring.UspMonitoringJobTakeNext`
- `monitoring.UspMonitoringJobGetLatestByCategory`
- `monitoring.UspMonitoringJobExpireStale`

Changed source files:

- `src/XTMon/Sql/016_LOG_FI_ALMT_Full_Migration.sql`
- `src/XTMon/Sql/017_LOG_FI_ALMT_PostDeploy_Verification.sql`

## Choose The Correct Deployment Path

### Rebuild environment, destructive reset is acceptable

Use `016_LOG_FI_ALMT_Full_Migration.sql` and then `017_LOG_FI_ALMT_PostDeploy_Verification.sql`.

Reason:

- `016` recreates the full XTMon-managed LOG_FI_ALMT object surface in one script.
- `016` is destructive and clears orchestration history plus latest-result data.
- `017` verifies the rebuilt object surface, key signatures, and selected indexes immediately after deployment.

Do not use this path on a live environment unless losing job history and latest cached results is explicitly acceptable.

## Pre-Deployment Checks

1. Confirm the target database is `LogFiAlmt`.
2. Confirm no deployment is running against the destructive full migration by mistake.
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

### Destructive rebuild rollout

1. Confirm orchestration history can be discarded.
2. Connect to the `LogFiAlmt` database.
3. Run `src/XTMon/Sql/016_LOG_FI_ALMT_Full_Migration.sql`.
4. Run `src/XTMon/Sql/017_LOG_FI_ALMT_PostDeploy_Verification.sql`.
5. The full migration recreates the replay recovery procedures used by startup recovery and System Diagnostics.
6. Restart the XTMon application.

## What To Verify After Deployment

### 1. Verification script

Run:

```sql
:r .\017_LOG_FI_ALMT_PostDeploy_Verification.sql
```

Expected:

- the result set shows all checks with `Passed = 1`
- the script finishes with `LOG_FI_ALMT post-deploy verification passed.`

### 2. Procedure body checks

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

### 3. Category polling returns lightweight rows

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

### 4. Worker recovery behavior

After app restart, check application logs for the new stage-specific messages around stale-expiry and take-next instead of a single opaque poll failure.

Expected symptoms improvement:

- startup is responsive even when SQL lock contention occurs
- Data Validation batch status refresh no longer drags large result payloads across repeated polls

## Rollback Guidance

If the deployment needs to be rolled back:

1. Reapply the previously captured procedure definitions from the pre-deployment query.
2. Restart the XTMon application.
3. Recheck monitoring-job polling and batch responsiveness.

If the deployment used the destructive full migration, rollback of cleared orchestration data is not possible without a database backup.