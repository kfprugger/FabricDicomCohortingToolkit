# Fabric Notebook: Reset Imaging Metastore Extension
#
# Guarded operational reset for the DICOM tag extension pipeline.
# Clears Silver ImagingMetastoreExtension rows and/or Admin ImagingMetastoreExtensionControl
# so the next unfiltered extraction run performs a clean initial-load style rebuild.

# Fabric parameters. Deployment tags the notebook code cell as a parameter cell so
# RunNotebook executionData.parameters can override these defaults.
CONFIRM_RESET = ""
RESET_TARGET_TABLE = "true"
RESET_CONTROL_WATERMARK = "true"

import notebookutils
import requests
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, types as T
from datetime import datetime, timezone

spark = SparkSession.builder.getOrCreate()

SILVER_LH_NAME = "healthcare1_msft_silver"
ADMIN_LH_NAME = "healthcare1_msft_admin"
TARGET_TABLE = "ImagingMetastoreExtension"
CONTROL_TABLE = "ImagingMetastoreExtensionControl"
PIPELINE_NAME = "extract_imaging_metastore_extension"


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
    except AttributeError:
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
    raw_value = get_param(name, str(default).lower())
    if raw_value is None or str(raw_value).strip() == "":
        return default
    text = str(raw_value).strip().lower()
    if text in ("1", "true", "yes", "y"):
        return True
    if text in ("0", "false", "no", "n"):
        return False
    raise ValueError(f"{name} must be true or false; got {raw_value!r}")


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


CONFIRM_RESET = str(get_param("CONFIRM_RESET", "")).strip()
RESET_TARGET_TABLE = parse_bool_param("RESET_TARGET_TABLE", True)
RESET_CONTROL_WATERMARK = parse_bool_param("RESET_CONTROL_WATERMARK", True)

if CONFIRM_RESET != "RESET_IMAGING_METASTORE_EXTENSION":
    raise ValueError(
        "Refusing to reset DICOM tag extension state. "
        "Pass CONFIRM_RESET='RESET_IMAGING_METASTORE_EXTENSION' to proceed."
    )
if not RESET_TARGET_TABLE and not RESET_CONTROL_WATERMARK:
    raise ValueError("Nothing to reset: RESET_TARGET_TABLE and RESET_CONTROL_WATERMARK are both false.")

WORKSPACE_ID = resolve_workspace_id()
SILVER_LH_ID = resolve_lakehouse_id(SILVER_LH_NAME)
ADMIN_LH_ID = resolve_lakehouse_id(ADMIN_LH_NAME)
TARGET_PATH = abfss_tables(SILVER_LH_ID, TARGET_TABLE)
CONTROL_PATH = abfss_tables(ADMIN_LH_ID, CONTROL_TABLE)

print("Reset Imaging Metastore Extension")
print(f"  Workspace: {WORKSPACE_ID}")
print(f"  Silver LH: {SILVER_LH_ID} ({SILVER_LH_NAME})")
print(f"  Admin LH:  {ADMIN_LH_ID} ({ADMIN_LH_NAME})")
print(f"  Reset target rows: {RESET_TARGET_TABLE}")
print(f"  Reset control watermark: {RESET_CONTROL_WATERMARK}")

if RESET_TARGET_TABLE:
    if DeltaTable.isDeltaTable(spark, TARGET_PATH):
        before_rows = spark.read.format("delta").load(TARGET_PATH).count()
        DeltaTable.forPath(spark, TARGET_PATH).delete()
        after_rows = spark.read.format("delta").load(TARGET_PATH).count()
        print(f"Cleared {TARGET_TABLE}: beforeRows={before_rows}, afterRows={after_rows}")
    else:
        print(f"Target table {TARGET_TABLE} does not exist at {TARGET_PATH}; nothing to clear.")

if RESET_CONTROL_WATERMARK:
    if DeltaTable.isDeltaTable(spark, CONTROL_PATH):
        control_df = spark.read.format("delta").load(CONTROL_PATH)
        before_rows = control_df.where(
            (control_df.pipelineName == PIPELINE_NAME) & (control_df.targetTable == TARGET_TABLE)
        ).count()
        DeltaTable.forPath(spark, CONTROL_PATH).delete(
            f"pipelineName = '{PIPELINE_NAME}' AND targetTable = '{TARGET_TABLE}'"
        )
        after_df = spark.read.format("delta").load(CONTROL_PATH)
        after_rows = after_df.where(
            (after_df.pipelineName == PIPELINE_NAME) & (after_df.targetTable == TARGET_TABLE)
        ).count()
        print(f"Cleared {CONTROL_TABLE} watermark row: beforeRows={before_rows}, afterRows={after_rows}")
    else:
        schema = T.StructType([
            T.StructField("pipelineName", T.StringType(), False),
            T.StructField("targetTable", T.StringType(), False),
            T.StructField("lastSuccessfulHighWatermark", T.TimestampType(), True),
            T.StructField("lastSuccessfulRunId", T.StringType(), True),
            T.StructField("updatedAt", T.TimestampType(), False),
        ])
        spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(CONTROL_PATH)
        print(f"Created empty {CONTROL_TABLE}; no watermark row existed.")

print(f"Reset completed at {datetime.now(timezone.utc).isoformat()}")
print("Next unfiltered 02_extract_imaging_metastore_extension run will scan all active ImagingMetastore rows.")
