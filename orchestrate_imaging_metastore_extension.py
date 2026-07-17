# Fabric Notebook: Orchestrate Imaging Metastore Extension
# Fabric parameters. Deployment tags the notebook code cell as a parameter cell so
# RunNotebook executionData.parameters can override these defaults.
RUN_MODE = "dryRun"
EXECUTE = "false"
CONFIRM_FULL_REBUILD = ""
MAX_SOURCE_ROWS_PER_BATCH = "50000000"
ORCHESTRATION_RUN_ID = ""

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

RUN_MODE = str(get_param("RUN_MODE", "dryRun")).strip()
EXECUTE = parse_bool_param("EXECUTE", False)
CONFIRM_FULL_REBUILD = str(get_param("CONFIRM_FULL_REBUILD", "")).strip()
MAX_SOURCE_ROWS_PER_BATCH = str(get_param("MAX_SOURCE_ROWS_PER_BATCH", "50000000"))
ORCHESTRATION_RUN_ID = str(get_param("ORCHESTRATION_RUN_ID", "")).strip()
RUN_TABLE = "ImagingMetastoreExtensionOrchestrationRun"
RUN_SCHEMA = T.StructType([
    T.StructField("orchestrationRunId", T.StringType(), False),
    T.StructField("runMode", T.StringType(), False),
    T.StructField("execute", T.BooleanType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("message", T.StringType(), True),
    T.StructField("startedAt", T.TimestampType(), False),
    T.StructField("completedAt", T.TimestampType(), True),
])
run_id = ORCHESTRATION_RUN_ID or str(uuid4())
run_path = abfss_tables(ADMIN_LH_ID, RUN_TABLE)
ensure_delta_table(run_path, RUN_SCHEMA)
message = "Plan only. Set EXECUTE=true to run child notebooks."
status = "Planned"
if EXECUTE:
    if RUN_MODE == "fullRebuild":
        if CONFIRM_FULL_REBUILD != "FULL_REBUILD_IMAGING_METASTORE_EXTENSION":
            raise ValueError("fullRebuild requires CONFIRM_FULL_REBUILD='FULL_REBUILD_IMAGING_METASTORE_EXTENSION'.")
        notebookutils.notebook.run("99_reset_imaging_metastore_extension", 3600, {"CONFIRM_RESET":"RESET_IMAGING_METASTORE_EXTENSION","RESET_TARGET_TABLE":"true","RESET_CONTROL_WATERMARK":"true"})
        notebookutils.notebook.run("02_extract_imaging_metastore_extension", 7200, {"MAX_SOURCE_ROWS_PER_BATCH": MAX_SOURCE_ROWS_PER_BATCH, "ORCHESTRATION_RUN_ID": run_id})
        message = "Full rebuild reset + extraction completed."
        status = "Succeeded"
    elif RUN_MODE == "incremental":
        notebookutils.notebook.run("02_extract_imaging_metastore_extension", 7200, {"MAX_SOURCE_ROWS_PER_BATCH": MAX_SOURCE_ROWS_PER_BATCH, "ORCHESTRATION_RUN_ID": run_id})
        message = "Incremental extraction completed."
        status = "Succeeded"
    elif RUN_MODE == "dryRun":
        notebookutils.notebook.run("02_extract_imaging_metastore_extension", 3600, {"DRY_RUN_ONLY":"true", "MAX_SOURCE_ROWS_PER_BATCH": MAX_SOURCE_ROWS_PER_BATCH, "ORCHESTRATION_RUN_ID": run_id})
        message = "Dry-run extraction completed."
        status = "Succeeded"
    else:
        raise ValueError("RUN_MODE must be dryRun, incremental, or fullRebuild.")
row={"orchestrationRunId":run_id,"runMode":RUN_MODE,"execute":EXECUTE,"status":status,"message":message,"startedAt":current_utc_naive(),"completedAt":current_utc_naive()}
spark.createDataFrame([row], RUN_SCHEMA).write.format("delta").mode("append").save(run_path)
print(row)
