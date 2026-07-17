# Fabric Notebook: Preflight Imaging Metastore Extension
# Fabric parameters. Deployment tags the notebook code cell as a parameter cell so
# RunNotebook executionData.parameters can override these defaults.
FAIL_ON_ERROR = "false"

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

FAIL_ON_ERROR = parse_bool_param("FAIL_ON_ERROR", False)
PREFLIGHT_TABLE = "ImagingMetastoreExtensionPreflight"
TAG_DICTIONARY_TABLE = "DicomTagDictionary"
CONTROL_TABLE = "ImagingMetastoreExtensionControl"
RUN_TABLE = "ImagingMetastoreExtensionRun"
METRICS_TABLE = "ImagingMetastoreExtensionMetrics"
HEALTH_TABLE = "ImagingMetastoreExtensionHealth"

PREFLIGHT_SCHEMA = T.StructType([
    T.StructField("preflightRunId", T.StringType(), False),
    T.StructField("checkName", T.StringType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("severity", T.StringType(), False),
    T.StructField("message", T.StringType(), True),
    T.StructField("createdAt", T.TimestampType(), False),
])

run_id = str(uuid4())
rows = []
now = current_utc_naive()

def add(check, status, severity, message):
    rows.append({"preflightRunId": run_id, "checkName": check, "status": status, "severity": severity, "message": message, "createdAt": now})
    print(f"{status} {severity} {check}: {message}")

def check_columns(path, table_name, required):
    if not DeltaTable.isDeltaTable(spark, path):
        add(f"{table_name}.exists", "Failed", "Red", f"Delta table missing at {path}")
        return None
    df = spark.read.format("delta").load(path)
    missing = [c for c in required if c not in df.columns]
    if missing:
        add(f"{table_name}.columns", "Failed", "Red", "Missing columns: " + ", ".join(missing))
    else:
        add(f"{table_name}.columns", "Passed", "Green", "Required columns present")
    return df

metastore = check_columns(abfss_tables(SILVER_LH_ID, "ImagingMetastore"), "ImagingMetastore", ["id", "msftSourceSystem", "studyInstanceUid", "seriesInstanceUid", "sopInstanceUid", "sourceModifiedAt", "msftIsDeleted"])
imaging_study = check_columns(abfss_tables(SILVER_LH_ID, "ImagingStudy"), "ImagingStudy", ["id", "msftSourceSystem", "identifier_string", "extension", "meta_lastUpdated"])
for table in [TAG_DICTIONARY_TABLE, CONTROL_TABLE, RUN_TABLE, METRICS_TABLE, HEALTH_TABLE]:
    path = abfss_tables(ADMIN_LH_ID, table)
    add(f"{table}.exists", "Passed" if DeltaTable.isDeltaTable(spark, path) else "Warning", "Green" if DeltaTable.isDeltaTable(spark, path) else "Amber", "Admin operational table check")
if metastore is not None:
    metadata_ok = "metadata" in metastore.columns or "metadata_string" in metastore.columns
    add("ImagingMetastore.metadata", "Passed" if metadata_ok else "Failed", "Green" if metadata_ok else "Red", "metadata or metadata_string column available")
if DeltaTable.isDeltaTable(spark, abfss_tables(ADMIN_LH_ID, TAG_DICTIONARY_TABLE)):
    dictionary = spark.read.format("delta").load(abfss_tables(ADMIN_LH_ID, TAG_DICTIONARY_TABLE))
    enabled = dictionary.where(F.col("enabled") == F.lit(True))
    duplicate_enabled = enabled.groupBy("dicomTag").count().where(F.col("count") > 1).count()
    enabled_count = enabled.count()
    add("DicomTagDictionary.enabledCount", "Passed" if enabled_count > 0 else "Failed", "Green" if enabled_count > 0 else "Red", f"enabled tags={enabled_count}")
    add("DicomTagDictionary.duplicates", "Passed" if duplicate_enabled == 0 else "Failed", "Green" if duplicate_enabled == 0 else "Red", f"duplicate enabled tags={duplicate_enabled}")
ensure_delta_table(abfss_tables(ADMIN_LH_ID, PREFLIGHT_TABLE), PREFLIGHT_SCHEMA)
spark.createDataFrame(rows, PREFLIGHT_SCHEMA).write.format("delta").mode("append").save(abfss_tables(ADMIN_LH_ID, PREFLIGHT_TABLE))
failed = [r for r in rows if r["status"] == "Failed"]
if FAIL_ON_ERROR and failed:
    raise RuntimeError(f"Preflight failed checks: {[r['checkName'] for r in failed]}")
print(f"Preflight complete: runId={run_id}, checks={len(rows)}, failures={len(failed)}")
