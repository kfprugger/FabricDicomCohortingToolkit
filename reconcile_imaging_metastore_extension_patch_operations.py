# Fabric Notebook: Reconcile Imaging Metastore Extension Patch/Delete Operations
# Fabric parameters. Deployment tags the notebook code cell as a parameter cell so
# RunNotebook executionData.parameters can override these defaults.
DRY_RUN_ONLY = "true"
CONFIRM_RECONCILE = ""
FILTER_STUDY_INSTANCE_UIDS = ""
BATCH_START_SOURCE_MODIFIED_AT = ""
BATCH_END_SOURCE_MODIFIED_AT = ""

import notebookutils
import requests
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F, types as T
from datetime import datetime, timezone, timedelta
from uuid import uuid4

spark = SparkSession.builder.getOrCreate()
SILVER_LH_NAME = "healthcare1_msft_silver"
ADMIN_LH_NAME = "healthcare1_msft_admin"
PIPELINE_NAME = "extract_imaging_metastore_extension"
TARGET_TABLE = "ImagingMetastoreExtension"


def current_utc_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)


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


def _runtime_context():
    try:
        return notebookutils.runtime.context
    except Exception:
        return {}


def _unwrap_param_value(value):
    if isinstance(value, dict) and "value" in value:
        return value.get("value")
    return value


def get_param(name: str, default):
    if name in globals() and globals().get(name) is not None:
        return _unwrap_param_value(globals().get(name))
    context = _runtime_context()
    direct_value = _context_get(context, name, None)
    if direct_value is not None:
        return _unwrap_param_value(direct_value)
    params = _context_get(context, "parameters", {}) or {}
    param_value = _context_get(params, name, None)
    if param_value is not None:
        return _unwrap_param_value(param_value)
    return default


def parse_bool_param(name: str, default: bool) -> bool:
    raw = get_param(name, str(default).lower())
    if raw is None or str(raw).strip() == "":
        return default
    text = str(raw).strip().lower()
    if text in ("1", "true", "yes", "y"):
        return True
    if text in ("0", "false", "no", "n"):
        return False
    raise ValueError(f"{name} must be true or false; got {raw!r}")


def parse_csv_param(name: str):
    raw = get_param(name, "")
    if raw is None or str(raw).strip() == "":
        return []
    return [v.strip() for v in str(raw).split(",") if v.strip()]


def parse_timestamp_param(name: str):
    raw = get_param(name, "")
    if raw is None or str(raw).strip() == "":
        return None
    text = str(raw).strip().replace("Z", "+00:00")
    value = datetime.fromisoformat(text)
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def resolve_workspace_id() -> str:
    try:
        return notebookutils.fabric.resolve_workspace_id()
    except AttributeError:
        context = _runtime_context()
        workspace_id = _context_get(context, "currentWorkspaceId", None) or _context_get(context, "workspaceId", None)
        if workspace_id:
            return workspace_id
        raise ValueError("Unable to resolve current Fabric workspace ID from notebookutils.")


def resolve_lakehouse_id(name: str) -> str:
    token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
    resp = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/lakehouses",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    resp.raise_for_status()
    for lakehouse in resp.json().get("value", []):
        if lakehouse.get("displayName") == name:
            return lakehouse["id"]
    raise ValueError(f"Lakehouse '{name}' not found in workspace {WORKSPACE_ID}")


def abfss_tables(lakehouse_id: str, table_name: str) -> str:
    return f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{table_name}"


def ensure_delta_table(path: str, schema: T.StructType):
    if DeltaTable.isDeltaTable(spark, path):
        existing_cols = set(spark.read.format("delta").load(path).columns)
        missing = [field for field in schema.fields if field.name not in existing_cols]
        if missing:
            def sql_type(data_type):
                if isinstance(data_type, T.StringType): return "STRING"
                if isinstance(data_type, T.BooleanType): return "BOOLEAN"
                if isinstance(data_type, T.TimestampType): return "TIMESTAMP"
                if isinstance(data_type, T.LongType): return "BIGINT"
                if isinstance(data_type, T.IntegerType): return "INT"
                if isinstance(data_type, T.DoubleType): return "DOUBLE"
                return "STRING"
            spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMNS (" + ", ".join(f"{f.name} {sql_type(f.dataType)}" for f in missing) + ")")
        return
    spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(path)


WORKSPACE_ID = resolve_workspace_id()
SILVER_LH_ID = resolve_lakehouse_id(SILVER_LH_NAME)
ADMIN_LH_ID = resolve_lakehouse_id(ADMIN_LH_NAME)
TARGET_PATH = abfss_tables(SILVER_LH_ID, TARGET_TABLE)

DRY_RUN_ONLY = parse_bool_param("DRY_RUN_ONLY", True)
CONFIRM_RECONCILE = str(get_param("CONFIRM_RECONCILE", "")).strip()
FILTER_STUDY_INSTANCE_UIDS = parse_csv_param("FILTER_STUDY_INSTANCE_UIDS")
BATCH_START_SOURCE_MODIFIED_AT = parse_timestamp_param("BATCH_START_SOURCE_MODIFIED_AT")
BATCH_END_SOURCE_MODIFIED_AT = parse_timestamp_param("BATCH_END_SOURCE_MODIFIED_AT")
RUN_TABLE = "ImagingMetastoreExtensionReconciliationRun"
RUN_SCHEMA = T.StructType([
    T.StructField("reconcileRunId", T.StringType(), False),
    T.StructField("startedAt", T.TimestampType(), False),
    T.StructField("completedAt", T.TimestampType(), True),
    T.StructField("status", T.StringType(), False),
    T.StructField("dryRunOnly", T.BooleanType(), False),
    T.StructField("candidateRows", T.LongType(), True),
    T.StructField("rowsSoftDeleted", T.LongType(), True),
    T.StructField("message", T.StringType(), True),
])
run_id = str(uuid4())
run_path = abfss_tables(ADMIN_LH_ID, RUN_TABLE)
ensure_delta_table(run_path, RUN_SCHEMA)
if not DRY_RUN_ONLY and CONFIRM_RECONCILE != "RECONCILE_IMAGING_METASTORE_EXTENSION":
    raise ValueError("Non-dry reconciliation requires CONFIRM_RECONCILE='RECONCILE_IMAGING_METASTORE_EXTENSION'.")
if not DeltaTable.isDeltaTable(spark, TARGET_PATH):
    raise ValueError(f"Target table missing: {TARGET_PATH}")
source = spark.read.format("delta").load(abfss_tables(SILVER_LH_ID, "ImagingMetastore"))
target = spark.read.format("delta").load(TARGET_PATH).where(F.col("isActive") == F.lit(True))
if FILTER_STUDY_INSTANCE_UIDS:
    target = target.where(F.col("studyInstanceUid").isin(FILTER_STUDY_INSTANCE_UIDS))
if BATCH_START_SOURCE_MODIFIED_AT and BATCH_END_SOURCE_MODIFIED_AT:
    target = target.where((F.col("sourceModifiedAt") >= F.lit(BATCH_START_SOURCE_MODIFIED_AT).cast("timestamp")) & (F.col("sourceModifiedAt") < F.lit(BATCH_END_SOURCE_MODIFIED_AT).cast("timestamp")))
deleted_source = source.where(F.coalesce(F.col("msftIsDeleted"), F.lit(False)) == F.lit(True)).select(F.col("id").alias("imagingMetastoreId")).distinct()
candidates = target.join(deleted_source, "imagingMetastoreId", "inner").select("imagingMetastoreId").distinct()
candidate_count = candidates.count()
rows_soft_deleted = 0
if candidate_count and not DRY_RUN_ONLY:
    ids = [r["imagingMetastoreId"] for r in candidates.collect()]
    DeltaTable.forPath(spark, TARGET_PATH).update(
        condition=F.col("imagingMetastoreId").isin(ids),
        set={"isActive": F.lit(False)}
    )
    rows_soft_deleted = candidate_count
status = "DryRunSucceeded" if DRY_RUN_ONLY else "Succeeded"
row = {"reconcileRunId": run_id, "startedAt": current_utc_naive(), "completedAt": current_utc_naive(), "status": status, "dryRunOnly": DRY_RUN_ONLY, "candidateRows": candidate_count, "rowsSoftDeleted": rows_soft_deleted, "message": "Only source msftIsDeleted=true rows are reconciled; broad anti-join delete remains intentionally excluded."}
spark.createDataFrame([row], RUN_SCHEMA).write.format("delta").mode("append").save(run_path)
print(row)
