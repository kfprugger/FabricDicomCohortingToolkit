# Fabric Notebook: Full Backfill Imaging Metastore Extension
#
# Guarded Option C workflow: plan, optional ImagingStudy compaction, full reset +
# rebuild through the existing orchestrator, and post-run verification.

# Fabric parameters. Deployment tags the notebook code cell as a parameter cell so
# RunNotebook executionData.parameters can override these defaults.
ACTION = "PLAN"
CONFIRM_FULL_BACKFILL = ""
RUN_OPTIMIZE_IMAGING_STUDY = "true"
RUN_MATERIALIZER = "true"
MAX_SOURCE_ROWS_PER_BATCH = "50000000"
SHUFFLE_PARTITIONS = "512"
ADVISORY_PARTITION_SIZE_BYTES = "67108864"
VERIFY_DICOM_TAG = "00321060"
VERIFY_COLUMN = "requestedProcedureDescription"
FULL_BACKFILL_LOCK_TIMEOUT_MINUTES = "360"
MAX_PARSE_FAILURE_FRACTION = "0.01"
MAX_PARSE_FAILURE_ROWS = ""

import json
import re
from datetime import datetime, timezone, timedelta
from uuid import uuid4

import notebookutils
import requests
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.getOrCreate()

SILVER_LH_NAME = "healthcare1_msft_silver"
ADMIN_LH_NAME = "healthcare1_msft_admin"
SOURCE_TABLE = "ImagingMetastore"
STUDY_TABLE = "ImagingStudy"
TARGET_TABLE = "ImagingMetastoreExtension"
DICTIONARY_TABLE = "DicomTagDictionary"
LEASE_LOCK_TABLE = "ImagingMetastoreExtensionLeaseLock"
FULL_BACKFILL_LOCK_TABLE = "ImagingMetastoreExtensionFullBackfillLock"
RUN_TABLE = "ImagingMetastoreExtensionRun"
ORCHESTRATION_RUN_TABLE = "ImagingMetastoreExtensionOrchestrationRun"
ORCHESTRATION_NOTEBOOK = "90_orchestrate_imaging_metastore_extension"
MATERIALIZER_NOTEBOOK = "05_materialize_imaging_ingestion_report"
SEARCH_TABLE = "ImagingMetastoreExtensionSearch"
LOCK_PIPELINE_NAME = "extract_imaging_metastore_extension"
SUPPORTED_ACTIONS = {"PLAN", "EXECUTE"}


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

def parse_fraction(name, default):
    raw = str(get_param(name, default)).strip()
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number between 0 and 1.") from exc
    if value < 0 or value > 1:
        raise ValueError(f"{name} must be between 0 and 1.")
    return value


def parse_optional_nonneg_int(name):
    raw = str(get_param(name, "")).strip()
    if raw == "":
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a non-negative integer.") from exc
    if value < 0:
        raise ValueError(f"{name} must be non-negative.")
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

FULL_BACKFILL_LOCK_SCHEMA = T.StructType([
    T.StructField("pipelineName", T.StringType(), False),
    T.StructField("targetTable", T.StringType(), False),
    T.StructField("leaseId", T.StringType(), False),
    T.StructField("ownerRunId", T.StringType(), False),
    T.StructField("acquiredAt", T.TimestampType(), False),
    T.StructField("expiresAt", T.TimestampType(), False),
    T.StructField("releasedAt", T.TimestampType(), True),
    T.StructField("status", T.StringType(), False),
])


def ensure_delta_table(path, schema):
    if not DeltaTable.isDeltaTable(spark, path):
        spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(path)


def lock_at(path):
    if not delta_exists(path):
        return None
    rows = (
        spark.read.format("delta").load(path)
        .where(
            (F.col("pipelineName") == F.lit(LOCK_PIPELINE_NAME))
            & (F.col("targetTable") == F.lit(TARGET_TABLE))
            & (F.col("status") == F.lit("Active"))
            & (F.col("expiresAt") > F.lit(current_utc_naive()).cast("timestamp"))
        )
        .limit(1)
        .collect()
    )
    return rows[0].asDict() if rows else None


def acquire_full_backfill_lock(owner_run_id, timeout_minutes):
    ensure_delta_table(FULL_BACKFILL_LOCK_PATH, FULL_BACKFILL_LOCK_SCHEMA)
    now = current_utc_naive()
    row = {
        "pipelineName": LOCK_PIPELINE_NAME,
        "targetTable": TARGET_TABLE,
        "leaseId": str(uuid4()),
        "ownerRunId": owner_run_id,
        "acquiredAt": now,
        "expiresAt": now + timedelta(minutes=timeout_minutes),
        "releasedAt": None,
        "status": "Active",
    }
    source = spark.createDataFrame([row], FULL_BACKFILL_LOCK_SCHEMA)
    lock = DeltaTable.forPath(spark, FULL_BACKFILL_LOCK_PATH)
    try:
        (
            lock.alias("t")
            .merge(
                source.alias("s"),
                "t.pipelineName = s.pipelineName AND t.targetTable = s.targetTable",
            )
            .whenMatchedUpdate(
                condition="t.status <> 'Active' OR t.expiresAt <= s.acquiredAt",
                set={field.name: f"s.{field.name}" for field in FULL_BACKFILL_LOCK_SCHEMA.fields},
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    except Exception as lock_exc:
        print(f"Full-backfill lock merge encountered a concurrent update: {lock_exc}")
    current = lock_at(FULL_BACKFILL_LOCK_PATH)
    if current is None or current["ownerRunId"] != owner_run_id or current["status"] != "Active":
        raise RuntimeError(f"Unable to acquire full-backfill lock; current owner: {current}")
    return current


def release_full_backfill_lock(owner_run_id, status):
    if not delta_exists(FULL_BACKFILL_LOCK_PATH):
        return
    DeltaTable.forPath(spark, FULL_BACKFILL_LOCK_PATH).update(
        condition=f"ownerRunId = '{owner_run_id}' AND status = 'Active'",
        set={
            "status": F.lit(status),
            "releasedAt": F.lit(current_utc_naive()).cast("timestamp"),
        },
    )


def delta_exists(path):
    return DeltaTable.isDeltaTable(spark, path)


def count_rows(path, active_only=False):
    if not delta_exists(path):
        return 0
    frame = spark.read.format("delta").load(path)
    if active_only and "isActive" in frame.columns:
        frame = frame.where(F.col("isActive") == F.lit(True))
    return frame.count()


def active_lock():
    return lock_at(LEASE_LOCK_PATH)


def print_summary(summary):
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))


ACTION = str(get_param("ACTION", "PLAN")).strip().upper()
CONFIRM_FULL_BACKFILL = str(get_param("CONFIRM_FULL_BACKFILL", "")).strip()
RUN_OPTIMIZE_IMAGING_STUDY = parse_bool("RUN_OPTIMIZE_IMAGING_STUDY", True)
RUN_MATERIALIZER = parse_bool("RUN_MATERIALIZER", True)
MAX_SOURCE_ROWS_PER_BATCH = parse_positive_int("MAX_SOURCE_ROWS_PER_BATCH", 50000000)
SHUFFLE_PARTITIONS = parse_positive_int("SHUFFLE_PARTITIONS", 512)
ADVISORY_PARTITION_SIZE_BYTES = parse_positive_int("ADVISORY_PARTITION_SIZE_BYTES", 67108864)
FULL_BACKFILL_LOCK_TIMEOUT_MINUTES = parse_positive_int("FULL_BACKFILL_LOCK_TIMEOUT_MINUTES", 360)
VERIFY_DICOM_TAG = str(get_param("VERIFY_DICOM_TAG", "00321060")).strip().upper()
VERIFY_COLUMN = str(get_param("VERIFY_COLUMN", "requestedProcedureDescription")).strip()
MAX_PARSE_FAILURE_FRACTION = parse_fraction("MAX_PARSE_FAILURE_FRACTION", 0.01)
MAX_PARSE_FAILURE_ROWS = parse_optional_nonneg_int("MAX_PARSE_FAILURE_ROWS")

if ACTION not in SUPPORTED_ACTIONS:
    raise ValueError(f"ACTION must be one of {sorted(SUPPORTED_ACTIONS)}.")
if not re.fullmatch(r"[0-9A-F]{8}", VERIFY_DICOM_TAG):
    raise ValueError("VERIFY_DICOM_TAG must contain eight uppercase hexadecimal characters.")
if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", VERIFY_COLUMN):
    raise ValueError("VERIFY_COLUMN must be a valid target column name.")

WORKSPACE_ID = resolve_workspace_id()
SILVER_LH_ID = resolve_lakehouse_id(SILVER_LH_NAME)
ADMIN_LH_ID = resolve_lakehouse_id(ADMIN_LH_NAME)
SOURCE_PATH = table_path(SILVER_LH_ID, SOURCE_TABLE)
STUDY_PATH = table_path(SILVER_LH_ID, STUDY_TABLE)
TARGET_PATH = table_path(SILVER_LH_ID, TARGET_TABLE)
DICTIONARY_PATH = table_path(ADMIN_LH_ID, DICTIONARY_TABLE)
LEASE_LOCK_PATH = table_path(ADMIN_LH_ID, LEASE_LOCK_TABLE)
FULL_BACKFILL_LOCK_PATH = table_path(ADMIN_LH_ID, FULL_BACKFILL_LOCK_TABLE)
RUN_PATH = table_path(ADMIN_LH_ID, RUN_TABLE)
ORCHESTRATION_RUN_PATH = table_path(ADMIN_LH_ID, ORCHESTRATION_RUN_TABLE)
SEARCH_PATH = table_path(ADMIN_LH_ID, SEARCH_TABLE)

if not delta_exists(SOURCE_PATH):
    raise ValueError(f"Required source table {SOURCE_TABLE} does not exist.")
if not delta_exists(STUDY_PATH):
    raise ValueError(f"Required source table {STUDY_TABLE} does not exist.")
if not delta_exists(DICTIONARY_PATH):
    raise ValueError(f"Required Admin table {DICTIONARY_TABLE} does not exist.")

source = spark.read.format("delta").load(SOURCE_PATH)
active_source = source.where(F.coalesce(F.col("msftIsDeleted"), F.lit(False)) == F.lit(False))
source_summary = active_source.agg(
    F.count(F.lit(1)).alias("activeSourceRows"),
    F.countDistinct("studyInstanceUid").alias("activeStudies"),
    F.countDistinct("msftSourceSystem").alias("activeSourceSystems"),
).collect()[0].asDict()

source_system_distribution = (
    active_source.groupBy("msftSourceSystem")
    .count()
    .orderBy(F.col("count").desc())
    .limit(20)
    .collect()
)

dictionary_rows = (
    spark.read.format("delta").load(DICTIONARY_PATH)
    .where(F.col("dicomTag") == F.lit(VERIFY_DICOM_TAG))
    .select("dicomTag", "dicomKeyword", "canonicalColumnName", "enabled", "status", "configurationHash")
    .collect()
)
if len(dictionary_rows) != 1:
    raise ValueError(f"Expected exactly one dictionary row for {VERIFY_DICOM_TAG}; found {len(dictionary_rows)}.")
tag_config = dictionary_rows[0].asDict()
if not tag_config["enabled"] or tag_config["status"] != "Enabled":
    raise ValueError(f"DICOM tag {VERIFY_DICOM_TAG} must be enabled before full backfill: {tag_config}")
if tag_config["canonicalColumnName"] != VERIFY_COLUMN:
    raise ValueError(
        f"VERIFY_COLUMN {VERIFY_COLUMN!r} does not match dictionary column {tag_config['canonicalColumnName']!r}."
    )

lock = active_lock()
backfill_lock = lock_at(FULL_BACKFILL_LOCK_PATH)
plan_summary = {
    "action": ACTION,
    "status": "READY_TO_EXECUTE" if lock is None and backfill_lock is None else "BLOCKED_ACTIVE_LEASE",
    "writesPerformed": False,
    "workspaceId": WORKSPACE_ID,
    "activeSourceRows": source_summary["activeSourceRows"],
    "activeStudies": source_summary["activeStudies"],
    "activeSourceSystems": source_summary["activeSourceSystems"],
    "currentTargetRows": count_rows(TARGET_PATH, active_only=True),
    "verifyDicomTag": VERIFY_DICOM_TAG,
    "verifyColumn": VERIFY_COLUMN,
    "tagConfiguration": tag_config,
    "activeExtractionLease": lock,
    "activeFullBackfillLease": backfill_lock,
    "runOptimizeImagingStudy": RUN_OPTIMIZE_IMAGING_STUDY,
    "runMaterializer": RUN_MATERIALIZER,
    "maxSourceRowsPerBatch": MAX_SOURCE_ROWS_PER_BATCH,
    "shufflePartitions": SHUFFLE_PARTITIONS,
    "advisoryPartitionSizeBytes": ADVISORY_PARTITION_SIZE_BYTES,
    "sourceSystemDistribution": [row.asDict() for row in source_system_distribution],
}

if ACTION == "PLAN":
    print_summary(plan_summary)
else:
    if lock is not None:
        raise RuntimeError(f"An active extraction lease blocks full backfill: {lock}")
    if backfill_lock is not None:
        raise RuntimeError(f"An active full-backfill lease is already running: {backfill_lock}")
    expected_confirmation = "FULL_BACKFILL_IMAGING_METASTORE_EXTENSION"
    if CONFIRM_FULL_BACKFILL != expected_confirmation:
        raise ValueError(f"CONFIRM_FULL_BACKFILL must equal {expected_confirmation!r}.")

    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", str(ADVISORY_PARTITION_SIZE_BYTES))

    orchestration_run_id = f"FULL_BACKFILL_{uuid4()}"
    acquire_full_backfill_lock(orchestration_run_id, FULL_BACKFILL_LOCK_TIMEOUT_MINUTES)
    started_at = current_utc_naive()
    try:
        if RUN_OPTIMIZE_IMAGING_STUDY:
            spark.sql(f"OPTIMIZE delta.`{STUDY_PATH}`")

        notebookutils.notebook.run(
            ORCHESTRATION_NOTEBOOK,
            10800,
            {
                "RUN_MODE": "fullRebuild",
                "EXECUTE": "true",
                "CONFIRM_FULL_REBUILD": "FULL_REBUILD_IMAGING_METASTORE_EXTENSION",
                "MAX_SOURCE_ROWS_PER_BATCH": str(MAX_SOURCE_ROWS_PER_BATCH),
                "ORCHESTRATION_RUN_ID": orchestration_run_id,
            },
        )

        orchestration_rows = (
            spark.read.format("delta").load(ORCHESTRATION_RUN_PATH)
            .where(F.col("orchestrationRunId") == F.lit(orchestration_run_id))
            .orderBy(F.col("startedAt").desc())
            .limit(1)
            .collect()
        )
        if not orchestration_rows or orchestration_rows[0]["status"] != "Succeeded":
            raise RuntimeError(
                f"Full backfill orchestration {orchestration_run_id} did not produce a successful audit row."
            )
        orchestration = orchestration_rows[0].asDict()

        extraction_rows = (
            spark.read.format("delta").load(RUN_PATH)
            .where(F.col("orchestrationRunId") == F.lit(orchestration_run_id))
            .orderBy(F.col("startedAt").desc())
            .limit(1)
            .collect()
        )
        if not extraction_rows:
            raise RuntimeError("Full backfill orchestration completed without an extraction audit row.")
        extraction = extraction_rows[0].asDict()
        if extraction["status"] not in ("Succeeded", "SucceededWithCleanupWarning"):
            raise RuntimeError(f"Full extraction status was {extraction['status']!r}.")
        parse_failed = int(extraction["metadataRowsParseFailed"] or 0)
        source_rows_active = int(extraction["sourceRowsActive"] or 0)
        parse_failed_fraction = (parse_failed / source_rows_active) if source_rows_active else 0.0
        # The extractor already quarantines rows with no enabled tag value and hard-fails above 1%,
        # so a Succeeded run is <= 1% by construction. Mirror that tolerance here instead of demanding
        # zero: legitimately sparse DICOM objects (e.g. structured reports, derived captures) carry none
        # of the enabled tags and must not strand an otherwise-complete rebuild. Quarantined rows are in
        # ImagingMetastoreExtensionParseQuarantine for review.
        if parse_failed:
            print(
                f"metadataRowsParseFailed={parse_failed}/{source_rows_active} "
                f"({parse_failed_fraction:.4%}); see ImagingMetastoreExtensionParseQuarantine for details."
            )
        if parse_failed_fraction > MAX_PARSE_FAILURE_FRACTION:
            raise RuntimeError(
                f"metadataRowsParseFailed {parse_failed}/{source_rows_active} "
                f"({parse_failed_fraction:.4%}) exceeds MAX_PARSE_FAILURE_FRACTION {MAX_PARSE_FAILURE_FRACTION:.4%}."
            )
        if MAX_PARSE_FAILURE_ROWS is not None and parse_failed > MAX_PARSE_FAILURE_ROWS:
            raise RuntimeError(
                f"metadataRowsParseFailed {parse_failed} exceeds MAX_PARSE_FAILURE_ROWS {MAX_PARSE_FAILURE_ROWS}."
            )
        if (extraction["sourceRowsMissingImagingStudy"] or 0) != 0:
            raise RuntimeError("Full backfill requires zero missing ImagingStudy joins.")

        target = spark.read.format("delta").load(TARGET_PATH)
        if VERIFY_COLUMN not in target.columns:
            raise RuntimeError(f"Rebuilt target is missing verification column {VERIFY_COLUMN!r}.")
        target_summary = target.where(F.col("isActive") == F.lit(True)).agg(
            F.count(F.lit(1)).alias("activeExtensionRows"),
            F.countDistinct("studyInstanceUid").alias("activeStudies"),
            F.sum(F.when(F.col(VERIFY_COLUMN).isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("verificationTagRows"),
        ).collect()[0].asDict()
        if not target_summary["activeExtensionRows"] or not target_summary["verificationTagRows"]:
            raise RuntimeError(f"Full backfill verification failed: {target_summary}")

        materializer_column_present = None
        if RUN_MATERIALIZER:
            notebookutils.notebook.run(MATERIALIZER_NOTEBOOK, 7200, {})
            if delta_exists(SEARCH_PATH):
                materializer_column_present = VERIFY_COLUMN in spark.read.format("delta").load(SEARCH_PATH).columns
            else:
                materializer_column_present = False
            if not materializer_column_present:
                raise RuntimeError(
                    f"Materialized search table does not expose {VERIFY_COLUMN!r}; "
                    "update materialize_imaging_ingestion_report.py search_schema before reporting."
                )

        release_full_backfill_lock(orchestration_run_id, "Released")
    except Exception:
        release_full_backfill_lock(orchestration_run_id, "Failed")
        raise

    print_summary({
        **plan_summary,
        "action": "EXECUTE",
        "status": "FULL_BACKFILL_SUCCEEDED",
        "writesPerformed": True,
        "orchestrationRunId": orchestration_run_id,
        "extractRunId": extraction["extractRunId"],
        "sourceRowsScanned": extraction["sourceRowsScanned"],
        "extensionRowsStaged": extraction["extensionRowsStaged"],
        "extensionRowsInserted": extraction["extensionRowsInserted"],
        "extensionRowsUpdated": extraction["extensionRowsUpdated"],
        "metadataRowsParseFailed": parse_failed,
        "metadataRowsParseFailedFraction": round(parse_failed_fraction, 6),
        **target_summary,
        "materializerRun": RUN_MATERIALIZER,
        "materializerColumnPresent": materializer_column_present,
    })
