# Fabric parameters. Deployment tags the notebook code cell as a parameter cell so
# RunNotebook executionData.parameters can override these defaults.
ACTION = "PLAN"
PIPELINE_RUN_ID = ""
MAIN_PIPELINE_ID = "68363adf-8890-488b-bbee-349af8279f4c"
WATCHED_ACTIVITY = "healthcare1_msft_imaging_dicom_extract_bronze_ingestion"
BUSINESS_EVENTS_ACTIVITY = "imaging_dicom_extract_bronze_ingestion"
POLL_INTERVAL_SECONDS = "120"
STALL_POLLS = "3"
MAX_WATCH_MINUTES = "180"
WRITE_SNAPSHOTS = "true"

# Fabric Notebook: Monitor Bronze Imaging DICOM Ingestion
#
# Watches the imaging-DICOM bronze ingestion activity of the parent pipeline
# (prov_healthcare1_msft_imaging_clinical_foundation_with_watermark) through the
# HDS ExecutionSummary telemetry table, keyed by pipelineRunId. ExecutionSummary
# carries one cumulative row per (pipelineRunId, activityName) that the HDS metrics
# poller upserts live during the run, so numSourceRecords / numTargetRecords grow
# while the activity is in flight.
#
# PLAN  - read-only single snapshot of the current (or given) run; no writes.
# WATCH - poll every POLL_INTERVAL_SECONDS, append a snapshot row per poll, detect
#         a stall (cumulative counts flat across STALL_POLLS consecutive polls while
#         the activity is still non-terminal), and on stall attach the offending .dcm
#         file pulled from BusinessEvents (severity=error) for the same activity.
#
# Alert transport is a Data Activator reflex on BronzeIngestionMonitorSnapshot:
# trigger when stallDetected becomes true and send the Office 365 email. The notebook
# only produces the signal; it holds no secrets and sends no mail itself.

import time
from datetime import datetime, timezone, timedelta
from uuid import uuid4

import notebookutils
import requests
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.getOrCreate()

ADMIN_LH_NAME = "healthcare1_msft_admin"
EXECUTION_SUMMARY_TABLE = "ExecutionSummary"
BUSINESS_EVENTS_TABLE = "BusinessEvents"
SNAPSHOT_TABLE = "BronzeIngestionMonitorSnapshot"
MONITOR_RUN_TABLE = "BronzeIngestionMonitorRun"

TERMINAL_STATUSES = {"succeeded", "failed", "cancelled", "canceled", "completed"}
SUPPORTED_ACTIONS = {"PLAN", "WATCH"}


def current_utc_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _runtime_context():
    try:
        return notebookutils.runtime.context
    except Exception:
        return {}


def _context_get(context, name, default=None):
    if context is None:
        return default
    if isinstance(context, dict):
        return context.get(name, default)
    try:
        return context.get(name, default)
    except TypeError:
        try:
            return context.get(name)
        except Exception:
            return default
    except Exception:
        return default


def _unwrap(value):
    return value.get("value") if isinstance(value, dict) and "value" in value else value


def get_param(name, default):
    if name in globals() and globals().get(name) is not None:
        return _unwrap(globals().get(name))
    context = _runtime_context()
    direct = _context_get(context, name, None)
    if direct is not None:
        return _unwrap(direct)
    parameters = _context_get(context, "parameters", {}) or {}
    value = _context_get(parameters, name, None)
    return _unwrap(value) if value is not None else default


def parse_bool(name, default):
    text = str(get_param(name, str(default).lower())).strip().lower()
    if text in ("true", "1", "yes", "y"):
        return True
    if text in ("false", "0", "no", "n"):
        return False
    raise ValueError(f"{name} must be true or false.")


def parse_positive_int(name, default):
    raw = str(get_param(name, default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer.") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive.")
    return value


def resolve_workspace_id():
    try:
        return notebookutils.fabric.resolve_workspace_id()
    except AttributeError:
        context = _runtime_context()
        workspace_id = _context_get(context, "currentWorkspaceId", None) or _context_get(context, "workspaceId", None)
        if workspace_id:
            return workspace_id
        raise ValueError("Unable to resolve current Fabric workspace ID.")


WORKSPACE_ID = resolve_workspace_id()


def resolve_lakehouse_id(name):
    token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
    response = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/lakehouses",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    response.raise_for_status()
    for lakehouse in response.json().get("value", []):
        if lakehouse.get("displayName") == name:
            return lakehouse["id"]
    raise ValueError(f"Lakehouse {name!r} was not found.")


def table_path(lakehouse_id, table_name):
    return f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{table_name}"


def ensure_table(path, schema):
    if not DeltaTable.isDeltaTable(spark, path):
        spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(path)


ACTION = str(get_param("ACTION", "PLAN")).strip().upper()
if ACTION not in SUPPORTED_ACTIONS:
    raise ValueError(f"ACTION must be one of {sorted(SUPPORTED_ACTIONS)}.")
PIPELINE_RUN_ID = str(get_param("PIPELINE_RUN_ID", "")).strip()
MAIN_PIPELINE_ID = str(get_param("MAIN_PIPELINE_ID", MAIN_PIPELINE_ID)).strip()
WATCHED_ACTIVITY = str(get_param("WATCHED_ACTIVITY", WATCHED_ACTIVITY)).strip()
BUSINESS_EVENTS_ACTIVITY = str(get_param("BUSINESS_EVENTS_ACTIVITY", BUSINESS_EVENTS_ACTIVITY)).strip()
POLL_INTERVAL_SECONDS = parse_positive_int("POLL_INTERVAL_SECONDS", 120)
STALL_POLLS = parse_positive_int("STALL_POLLS", 3)
MAX_WATCH_MINUTES = parse_positive_int("MAX_WATCH_MINUTES", 180)
WRITE_SNAPSHOTS = parse_bool("WRITE_SNAPSHOTS", True)

admin_id = resolve_lakehouse_id(ADMIN_LH_NAME)
execution_summary_path = table_path(admin_id, EXECUTION_SUMMARY_TABLE)
business_events_path = table_path(admin_id, BUSINESS_EVENTS_TABLE)
snapshot_path = table_path(admin_id, SNAPSHOT_TABLE)
monitor_run_path = table_path(admin_id, MONITOR_RUN_TABLE)

SNAPSHOT_SCHEMA = T.StructType([
    T.StructField("snapshotAt", T.TimestampType(), False),
    T.StructField("monitorRunId", T.StringType(), False),
    T.StructField("pipelineRunId", T.StringType(), True),
    T.StructField("activityName", T.StringType(), True),
    T.StructField("activityRunId", T.StringType(), True),
    T.StructField("executionStatus", T.StringType(), True),
    T.StructField("isTerminal", T.BooleanType(), True),
    T.StructField("pollIndex", T.IntegerType(), True),
    T.StructField("numSourceRecords", T.LongType(), True),
    T.StructField("numTargetRecords", T.LongType(), True),
    T.StructField("numSourceFiles", T.LongType(), True),
    T.StructField("numTargetFiles", T.LongType(), True),
    T.StructField("elapsedTime", T.DoubleType(), True),
    T.StructField("sourceDeltaSincePrev", T.LongType(), True),
    T.StructField("throughputRecordsPerMin", T.DoubleType(), True),
    T.StructField("consecutiveFlatPolls", T.IntegerType(), True),
    T.StructField("stallDetected", T.BooleanType(), True),
    T.StructField("alertEmitted", T.BooleanType(), True),
    T.StructField("culpritFilePath", T.StringType(), True),
    T.StructField("culpritMessage", T.StringType(), True),
    T.StructField("culpritRunId", T.StringType(), True),
    T.StructField("culpritEventAt", T.TimestampType(), True),
])

MONITOR_RUN_SCHEMA = T.StructType([
    T.StructField("monitorRunId", T.StringType(), False),
    T.StructField("pipelineRunId", T.StringType(), True),
    T.StructField("watchedActivity", T.StringType(), True),
    T.StructField("startedAt", T.TimestampType(), True),
    T.StructField("endedAt", T.TimestampType(), True),
    T.StructField("pollsExecuted", T.IntegerType(), True),
    T.StructField("finalStatus", T.StringType(), True),
    T.StructField("finalNumSourceRecords", T.LongType(), True),
    T.StructField("finalNumTargetRecords", T.LongType(), True),
    T.StructField("maxThroughputRecordsPerMin", T.DoubleType(), True),
    T.StructField("stalled", T.BooleanType(), True),
    T.StructField("culpritFilePath", T.StringType(), True),
    T.StructField("culpritMessage", T.StringType(), True),
    T.StructField("endReason", T.StringType(), True),
])


def _as_long(value):
    return int(value) if value is not None else None


def resolve_pipeline_run_id():
    """Explicit param wins; else the most recently modified pipelineRunId observed
    for the watched activity in ExecutionSummary."""
    if PIPELINE_RUN_ID:
        return PIPELINE_RUN_ID
    if not DeltaTable.isDeltaTable(spark, execution_summary_path):
        return None
    latest = (
        spark.read.format("delta").load(execution_summary_path)
        .where((F.col("activityName") == F.lit(WATCHED_ACTIVITY)) & F.col("pipelineRunId").isNotNull())
        .orderBy(F.col("msftModifiedDatetime").desc())
        .select("pipelineRunId")
        .limit(1)
        .collect()
    )
    return latest[0]["pipelineRunId"] if latest else None


def read_execution_row(target_run_id):
    """Latest ExecutionSummary row for (pipelineRunId, WATCHED_ACTIVITY)."""
    if not DeltaTable.isDeltaTable(spark, execution_summary_path):
        return None
    df = spark.read.format("delta").load(execution_summary_path).where(F.col("activityName") == F.lit(WATCHED_ACTIVITY))
    if target_run_id:
        df = df.where(F.col("pipelineRunId") == F.lit(target_run_id))
    rows = df.orderBy(F.col("msftModifiedDatetime").desc()).limit(1).collect()
    return rows[0] if rows else None


def find_culprit(target_run_id, activity_run_id, since):
    """Offending .dcm from BusinessEvents: severity=error for the DICOM extract
    activity. Prefer a runId match to the in-flight activity; else the most recent
    error at/after the run start."""
    if not DeltaTable.isDeltaTable(spark, business_events_path):
        return None
    errors = (
        spark.read.format("delta").load(business_events_path)
        .where((F.col("activityName") == F.lit(BUSINESS_EVENTS_ACTIVITY)) & (F.lower(F.col("severity")) == F.lit("error")))
    )
    candidate = None
    if activity_run_id:
        candidate = errors.where(F.col("runId") == F.lit(activity_run_id)).orderBy(F.col("eventDateTime").desc()).limit(1).collect()
    if not candidate and since is not None:
        candidate = errors.where(F.col("eventDateTime") >= F.lit(since)).orderBy(F.col("eventDateTime").desc()).limit(1).collect()
    if not candidate:
        candidate = errors.orderBy(F.col("eventDateTime").desc()).limit(1).collect()
    if not candidate:
        return None
    row = candidate[0]
    return {
        "culpritFilePath": row["sourceFilePath"],
        "culpritMessage": row["message"],
        "culpritRunId": row["runId"],
        "culpritEventAt": row["eventDateTime"],
    }


monitor_run_id = str(uuid4())
target_run_id = resolve_pipeline_run_id()
started_at = current_utc_naive()

if ACTION == "PLAN":
    row = read_execution_row(target_run_id)
    if row is None:
        print({
            "action": "PLAN",
            "pipelineRunId": target_run_id,
            "watchedActivity": WATCHED_ACTIVITY,
            "message": "No ExecutionSummary row found for the watched activity yet.",
        })
    else:
        status = (row["executionStatus"] or "").strip()
        is_terminal = status.lower() in TERMINAL_STATUSES
        culprit = find_culprit(target_run_id, row["runId"], None) if status.lower() == "failed" else None
        print({
            "action": "PLAN",
            "writesPerformed": False,
            "pipelineRunId": row["pipelineRunId"],
            "activityName": row["activityName"],
            "activityRunId": row["runId"],
            "executionStatus": status,
            "isTerminal": is_terminal,
            "numSourceRecords": _as_long(row["numSourceRecords"]),
            "numTargetRecords": _as_long(row["numTargetRecords"]),
            "numSourceFiles": _as_long(row["numSourceFiles"]),
            "numTargetFiles": _as_long(row["numTargetFiles"]),
            "elapsedTime": row["elapsedTime"],
            "lastModified": str(row["msftModifiedDatetime"]),
            "mostRecentError": culprit,
            "note": "PLAN is read-only. Run ACTION=WATCH to poll, write snapshots, and emit a stall exit value for a downstream email activity.",
        })
else:
    if WRITE_SNAPSHOTS:
        ensure_table(snapshot_path, SNAPSHOT_SCHEMA)
        ensure_table(monitor_run_path, MONITOR_RUN_SCHEMA)

    deadline = started_at + timedelta(minutes=MAX_WATCH_MINUTES)
    prev_source = None
    consecutive_flat = 0
    poll_index = 0
    stall_emitted = False
    stalled_ever = False
    max_throughput = 0.0
    final_status = None
    final_source = None
    final_target = None
    end_reason = "maxWatchElapsed"
    culprit_final = None

    while True:
        poll_index += 1
        poll_at = current_utc_naive()
        row = read_execution_row(target_run_id)

        if row is None:
            print({"poll": poll_index, "at": str(poll_at), "message": "No ExecutionSummary row yet for the run; waiting."})
            if poll_at >= deadline:
                end_reason = "maxWatchElapsed"
                break
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        if target_run_id is None:
            target_run_id = row["pipelineRunId"]

        status = (row["executionStatus"] or "").strip()
        is_terminal = status.lower() in TERMINAL_STATUSES
        num_source = _as_long(row["numSourceRecords"])
        num_target = _as_long(row["numTargetRecords"])
        elapsed = row["elapsedTime"]

        source_delta = None
        throughput = None
        if prev_source is not None and num_source is not None:
            source_delta = num_source - prev_source
            interval_min = POLL_INTERVAL_SECONDS / 60.0
            throughput = (source_delta / interval_min) if interval_min > 0 else None
            if throughput is not None and throughput > max_throughput:
                max_throughput = throughput
            if source_delta <= 0 and not is_terminal:
                consecutive_flat += 1
            else:
                consecutive_flat = 0

        stall_now = (consecutive_flat >= STALL_POLLS) and not is_terminal
        culprit = None
        alert_emitted = False
        if stall_now:
            stalled_ever = True
            culprit = find_culprit(target_run_id, row["runId"], started_at)
            culprit_final = culprit
            if not stall_emitted:
                alert_emitted = True   # first stall row: the Data Activator reflex fires here
                stall_emitted = True

        if WRITE_SNAPSHOTS:
            snap = {
                "snapshotAt": poll_at,
                "monitorRunId": monitor_run_id,
                "pipelineRunId": target_run_id,
                "activityName": row["activityName"],
                "activityRunId": row["runId"],
                "executionStatus": status,
                "isTerminal": is_terminal,
                "pollIndex": poll_index,
                "numSourceRecords": num_source,
                "numTargetRecords": num_target,
                "numSourceFiles": _as_long(row["numSourceFiles"]),
                "numTargetFiles": _as_long(row["numTargetFiles"]),
                "elapsedTime": elapsed,
                "sourceDeltaSincePrev": source_delta,
                "throughputRecordsPerMin": throughput,
                "consecutiveFlatPolls": consecutive_flat,
                "stallDetected": stall_now,
                "alertEmitted": alert_emitted,
                "culpritFilePath": culprit["culpritFilePath"] if culprit else None,
                "culpritMessage": culprit["culpritMessage"] if culprit else None,
                "culpritRunId": culprit["culpritRunId"] if culprit else None,
                "culpritEventAt": culprit["culpritEventAt"] if culprit else None,
            }
            spark.createDataFrame([snap], SNAPSHOT_SCHEMA).write.format("delta").mode("append").save(snapshot_path)

        print({
            "poll": poll_index, "at": str(poll_at), "status": status, "terminal": is_terminal,
            "numSourceRecords": num_source, "numTargetRecords": num_target,
            "sourceDeltaSincePrev": source_delta, "throughputRecordsPerMin": throughput,
            "consecutiveFlatPolls": consecutive_flat, "stallDetected": stall_now,
            "alertEmitted": alert_emitted, "culprit": culprit,
        })

        prev_source = num_source if num_source is not None else prev_source
        final_status = status
        final_source = num_source
        final_target = num_target

        if is_terminal:
            end_reason = "activityTerminal"
            if status.lower() == "failed" and culprit_final is None:
                culprit_final = find_culprit(target_run_id, row["runId"], started_at)
            break
        if poll_at >= deadline:
            end_reason = "maxWatchElapsed"
            break
        time.sleep(POLL_INTERVAL_SECONDS)

    ended_at = current_utc_naive()
    if WRITE_SNAPSHOTS:
        run_summary = {
            "monitorRunId": monitor_run_id,
            "pipelineRunId": target_run_id,
            "watchedActivity": WATCHED_ACTIVITY,
            "startedAt": started_at,
            "endedAt": ended_at,
            "pollsExecuted": poll_index,
            "finalStatus": final_status,
            "finalNumSourceRecords": final_source,
            "finalNumTargetRecords": final_target,
            "maxThroughputRecordsPerMin": max_throughput,
            "stalled": stalled_ever,
            "culpritFilePath": culprit_final["culpritFilePath"] if culprit_final else None,
            "culpritMessage": culprit_final["culpritMessage"] if culprit_final else None,
            "endReason": end_reason,
        }
        spark.createDataFrame([run_summary], MONITOR_RUN_SCHEMA).write.format("delta").mode("append").save(monitor_run_path)

    print({
        "action": "WATCH", "writesPerformed": WRITE_SNAPSHOTS, "monitorRunId": monitor_run_id,
        "pipelineRunId": target_run_id, "pollsExecuted": poll_index, "finalStatus": final_status,
        "finalNumSourceRecords": final_source, "finalNumTargetRecords": final_target,
        "maxThroughputRecordsPerMin": max_throughput, "stalled": stalled_ever,
        "endReason": end_reason, "culprit": culprit_final,
    })

    # Structured exit value so a downstream pipeline can branch on the stall signal.
    # Reflex/Data Activator cannot trigger off a Lakehouse Delta table, so the
    # reliable programmatic alert path is: an If Condition on this exit value ->
    # Office 365 Outlook "Send email" activity. The snapshot/run tables remain the
    # durable audit trail and a Power BI reflex source if the signal is surfaced there.
    exit_value = json.dumps({
        "stalled": stalled_ever,
        "finalStatus": final_status,
        "pipelineRunId": target_run_id,
        "monitorRunId": monitor_run_id,
        "culpritFilePath": culprit_final["culpritFilePath"] if culprit_final else None,
        "culpritMessage": culprit_final["culpritMessage"] if culprit_final else None,
        "endReason": end_reason,
    })
    try:
        notebookutils.notebook.exit(exit_value)
    except Exception:
        mssparkutils.notebook.exit(exit_value)
