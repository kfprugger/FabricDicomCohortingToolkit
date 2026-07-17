# Fabric Notebook: Evaluate Imaging Metastore Extension Alerts
# Fabric parameters. Deployment tags the notebook code cell as a parameter cell so
# RunNotebook executionData.parameters can override these defaults.
FAIL_ON_RED = "false"
INCLUDE_ANOMALIES = "true"

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

FAIL_ON_RED = parse_bool_param("FAIL_ON_RED", False)
ALERT_TABLE = "ImagingMetastoreExtensionAlert"
HEALTH_TABLE = "ImagingMetastoreExtensionHealth"
ANOMALY_TABLE = "ImagingMetastoreExtensionAnomaly"
ALERT_SCHEMA = T.StructType([
    T.StructField("alertRunId", T.StringType(), False),
    T.StructField("severity", T.StringType(), False),
    T.StructField("alertName", T.StringType(), False),
    T.StructField("message", T.StringType(), True),
    T.StructField("isCurrent", T.BooleanType(), False),
    T.StructField("resolvedAt", T.TimestampType(), True),
    T.StructField("createdAt", T.TimestampType(), False),
])
alert_path = abfss_tables(ADMIN_LH_ID, ALERT_TABLE)
ensure_delta_table(alert_path, ALERT_SCHEMA)
run_id = str(uuid4())
rows=[]
health_path = abfss_tables(ADMIN_LH_ID, HEALTH_TABLE)
if DeltaTable.isDeltaTable(spark, health_path):
    latest = spark.read.format("delta").load(health_path).orderBy(F.col("snapshotAt").desc()).limit(1).collect()
    if latest:
        h = latest[0].asDict()
        if h.get("healthStatus") in ("Red", "Amber"):
            rows.append({"alertRunId":run_id,"severity":h.get("healthStatus"),"alertName":"HealthStatus","message":h.get("statusMessage"),"isCurrent":True,"resolvedAt":None,"createdAt":current_utc_naive()})
        if h.get("metadataRowsParseFailed"):
            rows.append({"alertRunId":run_id,"severity":"Red","alertName":"ParseFailures","message":f"metadataRowsParseFailed={h.get('metadataRowsParseFailed')}","isCurrent":True,"resolvedAt":None,"createdAt":current_utc_naive()})
anomaly_path = abfss_tables(ADMIN_LH_ID, ANOMALY_TABLE)
if DeltaTable.isDeltaTable(spark, anomaly_path):
    include_anomalies = str(get_param("INCLUDE_ANOMALIES", "true")).strip().lower() not in ("0", "false", "no")
    if not include_anomalies:
        DeltaTable.forPath(spark, anomaly_path).update(condition="isCurrent = true", set={"isCurrent": F.lit(False), "resolvedAt": F.lit(current_utc_naive()).cast("timestamp")})
        print("Resolved current anomaly rows; historical rows retained.")
    else:
        recent = spark.read.format("delta").load(anomaly_path).where(
            (F.col("isCurrent") == F.lit(True))
            & (F.col("createdAt") >= F.lit(current_utc_naive() - timedelta(hours=24)).cast("timestamp"))
        )
        for r in recent.orderBy(F.col("createdAt").desc()).limit(20).collect():
            rows.append({"alertRunId":run_id,"severity":r["severity"],"alertName":"Anomaly:" + r["metricName"],"message":r["message"],"isCurrent":True,"resolvedAt":None,"createdAt":current_utc_naive()})
if not rows:
    try:
        DeltaTable.forPath(spark, alert_path).update(condition="isCurrent = true", set={"isCurrent": F.lit(False), "resolvedAt": F.lit(current_utc_naive()).cast("timestamp")})
        print("Resolved current alert rows; historical rows retained.")
    except Exception as alert_state_exc:
        print(f"Could not resolve prior alert rows: {alert_state_exc}")
if rows:
    spark.createDataFrame(rows, ALERT_SCHEMA).write.format("delta").mode("append").save(alert_path)
    DeltaTable.forPath(spark, alert_path).update(condition=f"alertRunId <> '{run_id}' AND isCurrent = true", set={"isCurrent": F.lit(False), "resolvedAt": F.lit(current_utc_naive()).cast("timestamp")})
for row in rows:
    print(row)
if FAIL_ON_RED and any(r["severity"] == "Red" for r in rows):
    raise RuntimeError("Red alerts detected")
print(f"Alert evaluation complete: runId={run_id}, alerts={len(rows)}")
