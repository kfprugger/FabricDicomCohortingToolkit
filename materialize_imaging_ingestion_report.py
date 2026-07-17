# Fabric parameters. Deployment tags the notebook code cell as a parameter cell so
# RunNotebook executionData.parameters can override these defaults.
REPORT_REFRESH_MODE = "full"
FAIL_ON_ERROR = "true"
MAIN_PIPELINE_ID = "68363adf-8890-488b-bbee-349af8279f4c"
MAIN_PIPELINE_RUN_ID = ""
PATCH_PIPELINE_ID = "9f0b4d69-165f-459c-a30c-1a8e6f7f2c04"
PATCH_PIPELINE_RUN_ID = ""
# Fabric Notebook: Materialize Imaging Ingestion Reporting Tables
# Reads Bronze/Silver/Extension Delta data and materializes report-ready snapshots in Admin.

FAIL_ON_ERROR = "true"

import notebookutils
import requests
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F, types as T
from datetime import datetime, timezone

spark = SparkSession.builder.getOrCreate()
BRONZE_LH_NAME = "healthcare1_msft_bronze"
SILVER_LH_NAME = "healthcare1_msft_silver"
ADMIN_LH_NAME = "healthcare1_msft_admin"
BRONZE_TABLE = "ImagingDicom"
SILVER_TABLE = "ImagingMetastore"
EXTENSION_TABLE = "ImagingMetastoreExtension"
INVENTORY_TABLE = "ImagingMetastoreExtensionIngestionInventory"
SEARCH_TABLE = "ImagingMetastoreExtensionSearch"
LOG_TABLE = "ImagingMetastoreExtensionPipelineLog"
MAIN_RUN_TABLE = "ImagingMetastoreMainPipelineRun"
MAIN_ACTIVITY_TABLE = "ImagingMetastoreMainPipelineActivity"
PATCH_RUN_TABLE = "ImagingMetastorePatchPipelineRun"
PATCH_ACTIVITY_TABLE = "ImagingMetastorePatchPipelineActivity"
TAG_METRICS_TABLE = "ImagingMetastorePipelineTagMetrics"


def now_utc():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def context_get(context, key, default=None):
    try:
        value = context.get(key, default)
        return default if value is None else value
    except Exception:
        return default


def workspace_id():
    try:
        return notebookutils.fabric.resolve_workspace_id()
    except Exception:
        context = notebookutils.runtime.context
        value = context_get(context, "currentWorkspaceId") or context_get(context, "workspaceId")
        if not value:
            raise ValueError("Unable to resolve Fabric workspace ID")
        return value


WORKSPACE_ID = workspace_id()

def lakehouse_id(name):
    token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
    response = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/lakehouses",
        headers={"Authorization": f"Bearer {token}"}, timeout=60)
    response.raise_for_status()
    for lakehouse in response.json().get("value", []):
        if lakehouse.get("displayName") == name:
            return lakehouse["id"]
    raise ValueError(f"Lakehouse not found: {name}")


def table_path(lh_id, table):
    return f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{lh_id}/Tables/{table}"


def ensure_table(path, schema):
    if not DeltaTable.isDeltaTable(spark, path):
        spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(path)

bronze_id = lakehouse_id(BRONZE_LH_NAME)
silver_id = lakehouse_id(SILVER_LH_NAME)
admin_id = lakehouse_id(ADMIN_LH_NAME)
bronze = spark.read.format("delta").load(table_path(bronze_id, BRONZE_TABLE))
silver = spark.read.format("delta").load(table_path(silver_id, SILVER_TABLE))
extension = spark.read.format("delta").load(table_path(silver_id, EXTENSION_TABLE)) if DeltaTable.isDeltaTable(spark, table_path(silver_id, EXTENSION_TABLE)) else None
snapshot_at = now_utc()
active_silver = silver.where(F.coalesce(F.col("msftIsDeleted"), F.lit(False)) == F.lit(False))
active_extension = extension.where(F.col("isActive") == F.lit(True)) if extension is not None else None

def counts(df, prefix):
    if df is None:
        return {f"{prefix}Rows": 0, f"{prefix}Studies": 0, f"{prefix}Series": 0, f"{prefix}SopInstances": 0}
    row = df.agg(F.count(F.lit(1)).cast("long").alias("rows"), F.countDistinct("studyInstanceUid").cast("long").alias("studies"), F.countDistinct("seriesInstanceUid").cast("long").alias("series"), F.countDistinct("sopInstanceUid").cast("long").alias("sops")).collect()[0]
    return {f"{prefix}Rows": row["rows"], f"{prefix}Studies": row["studies"], f"{prefix}Series": row["series"], f"{prefix}SopInstances": row["sops"]}

inventory_schema = T.StructType([
    T.StructField("snapshotAt", T.TimestampType(), False), T.StructField("workspaceId", T.StringType(), False),
    T.StructField("bronzeRows", T.LongType(), False), T.StructField("bronzeStudies", T.LongType(), False), T.StructField("bronzeSeries", T.LongType(), False), T.StructField("bronzeSopInstances", T.LongType(), False),
    T.StructField("silverRows", T.LongType(), False), T.StructField("silverStudies", T.LongType(), False), T.StructField("silverSeries", T.LongType(), False), T.StructField("silverSopInstances", T.LongType(), False),
    T.StructField("extensionRows", T.LongType(), False), T.StructField("extensionStudies", T.LongType(), False), T.StructField("extensionSeries", T.LongType(), False), T.StructField("extensionSopInstances", T.LongType(), False),
    T.StructField("extensionAccessions", T.LongType(), False), T.StructField("latestExtensionAt", T.TimestampType(), True),
])
all_counts = {}
all_counts.update(counts(bronze, "bronze")); all_counts.update(counts(active_silver, "silver")); all_counts.update(counts(active_extension, "extension"))
ext_accessions = active_extension.select(F.countDistinct("accessionNumber")).collect()[0][0] if active_extension is not None else 0
latest_ext = active_extension.agg(F.max("extractedAt")).collect()[0][0] if active_extension is not None else None
inventory = {"snapshotAt": snapshot_at, "workspaceId": WORKSPACE_ID, **all_counts, "extensionAccessions": int(ext_accessions or 0), "latestExtensionAt": latest_ext}
inventory_path = table_path(admin_id, INVENTORY_TABLE)
ensure_table(inventory_path, inventory_schema)
spark.createDataFrame([inventory], inventory_schema).write.format("delta").mode("append").save(inventory_path)

SEARCH_BASE_COLUMNS = ["id","imagingMetastoreId","imagingStudyId","msftSourceSystem","studyInstanceUid","seriesInstanceUid","sopInstanceUid","filePath","sourceModifiedAt","sourceModifiedDate","studyInstanceUid_tag","patientName","patientSex","patientId","patientBirthDate","accessionNumber","referringPhysicianName","studyDate","studyDescription","seriesInstanceUid_tag","modality","modalitiesInStudy","performedProcedureStepStartDate","manufacturerModelName","sopInstanceUid_tag","studyTime","timezoneOffsetFromUtc","seriesNumber","seriesDescription","seriesDate","seriesTime","sopClassUid","instanceNumber","documentTitle","isActive","sourceRowHash","extractedAt","extractRunId"]
SEARCH_EXCLUDE_COLUMNS = {"sourceRecordKey","sourceSystemHashBucket","targetSourceHash","msftIsDeleted"}
search_dynamic_columns = [f.name for f in extension.schema.fields if f.name not in SEARCH_BASE_COLUMNS and f.name not in SEARCH_EXCLUDE_COLUMNS and not f.name.endswith("_tag")] if extension is not None else []
search_column_names = SEARCH_BASE_COLUMNS + search_dynamic_columns
search_schema = T.StructType([T.StructField("snapshotAt", T.TimestampType(), False)] + [f for f in extension.schema.fields if f.name in search_column_names]) if extension is not None else T.StructType([T.StructField("snapshotAt", T.TimestampType(), False)])
search_path = table_path(admin_id, SEARCH_TABLE)
ensure_table(search_path, search_schema)
if active_extension is not None:
    search = active_extension.select(F.lit(snapshot_at).cast("timestamp").alias("snapshotAt"), *[F.col(f.name) for f in search_schema.fields if f.name != "snapshotAt"])
    search.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(search_path)

log_schema = T.StructType([T.StructField("snapshotAt", T.TimestampType(), False), T.StructField("extractRunId", T.StringType(), True), T.StructField("batchId", T.StringType(), True), T.StructField("status", T.StringType(), True), T.StructField("startedAt", T.TimestampType(), True), T.StructField("completedAt", T.TimestampType(), True), T.StructField("sourceRowsScanned", T.LongType(), True), T.StructField("sourceRowsJoinedToImagingStudy", T.LongType(), True), T.StructField("metadataRowsParseFailed", T.LongType(), True), T.StructField("extensionRowsInserted", T.LongType(), True), T.StructField("extensionRowsUpdated", T.LongType(), True), T.StructField("errorClass", T.StringType(), True), T.StructField("errorMessage", T.StringType(), True)])
log_path = table_path(admin_id, LOG_TABLE)
ensure_table(log_path, log_schema)
run_path = table_path(admin_id, "ImagingMetastoreExtensionRun")
if DeltaTable.isDeltaTable(spark, run_path):
    runs = spark.read.format("delta").load(run_path).select(*[F.lit(snapshot_at).cast("timestamp").alias("snapshotAt")] + [F.col(c) for c in ["extractRunId","batchId","status","startedAt","completedAt","sourceRowsScanned","sourceRowsJoinedToImagingStudy","metadataRowsParseFailed","extensionRowsInserted","extensionRowsUpdated","errorClass","errorMessage"] if c in spark.read.format("delta").load(run_path).columns])
    runs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(log_path)
print({"snapshotAt": snapshot_at, **inventory})

# Materialize parent pipeline run/activity telemetry for the executive summary.
MAIN_RUN_SCHEMA = T.StructType([
    T.StructField("snapshotAt", T.TimestampType(), False),
    T.StructField("pipelineId", T.StringType(), False),
    T.StructField("pipelineRunId", T.StringType(), False),
    T.StructField("status", T.StringType(), True),
    T.StructField("invokeType", T.StringType(), True),
    T.StructField("startTimeUtc", T.StringType(), True),
    T.StructField("endTimeUtc", T.StringType(), True),
    T.StructField("failureReason", T.StringType(), True),
])
MAIN_ACTIVITY_SCHEMA = T.StructType([
    T.StructField("snapshotAt", T.TimestampType(), False),
    T.StructField("pipelineRunId", T.StringType(), False),
    T.StructField("activityName", T.StringType(), True),
    T.StructField("activityType", T.StringType(), True),
    T.StructField("activityRunId", T.StringType(), True),
    T.StructField("status", T.StringType(), True),
    T.StructField("activityRunStart", T.StringType(), True),
    T.StructField("activityRunEnd", T.StringType(), True),
    T.StructField("durationInMs", T.LongType(), True),
    T.StructField("errorCode", T.StringType(), True),
    T.StructField("errorMessage", T.StringType(), True),
    T.StructField("failureType", T.StringType(), True),
])
TAG_METRICS_SCHEMA = T.StructType([
    T.StructField("snapshotAt", T.TimestampType(), False),
    T.StructField("extractRunId", T.StringType(), True),
    T.StructField("batchId", T.StringType(), True),
    T.StructField("dicomTag", T.StringType(), True),
    T.StructField("dicomKeyword", T.StringType(), True),
    T.StructField("rowsWithValue", T.DoubleType(), True),
    T.StructField("rowsProcessed", T.DoubleType(), True),
    T.StructField("coveragePercent", T.DoubleType(), True),
])
def runtime_param(name, default):
    try:
        context = notebookutils.runtime.context
        direct = context_get(context, name, None)
        params = context_get(context, "parameters", {}) or {}
        return direct if direct is not None else context_get(params, name, default)
    except Exception:
        return default

def upsert_deduped(path, rows, schema, keys):
    incoming = spark.createDataFrame(rows, schema)
    if DeltaTable.isDeltaTable(spark, path):
        existing = spark.read.format("delta").load(path)
        combined = existing.unionByName(incoming, allowMissingColumns=True)
    else:
        combined = incoming
    combined.dropDuplicates(keys).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)

main_run_id = str(runtime_param("MAIN_PIPELINE_RUN_ID", MAIN_PIPELINE_RUN_ID) or "").strip()
main_pipeline_id = str(runtime_param("MAIN_PIPELINE_ID", MAIN_PIPELINE_ID)).strip()
api_token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
api_headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}
if not main_run_id:
    latest_response = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{main_pipeline_id}/jobs/instances?limit=1", headers=api_headers, timeout=60)
    latest_response.raise_for_status()
    latest_values = latest_response.json().get("value", [])
    if latest_values:
        main_run_id = latest_values[0].get("id", "")
if main_run_id:
    run_response = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{main_pipeline_id}/jobs/instances/{main_run_id}", headers=api_headers, timeout=60)
    run_response.raise_for_status()
    run = run_response.json()
    run_path = table_path(admin_id, MAIN_RUN_TABLE)
    ensure_table(run_path, MAIN_RUN_SCHEMA)
    upsert_deduped(run_path, [{"snapshotAt": snapshot_at, "pipelineId": main_pipeline_id, "pipelineRunId": main_run_id, "status": run.get("status"), "invokeType": run.get("invokeType"), "startTimeUtc": run.get("startTimeUtc"), "endTimeUtc": run.get("endTimeUtc"), "failureReason": str(run.get("failureReason")) if run.get("failureReason") else None}], MAIN_RUN_SCHEMA, ["pipelineRunId"])
    activity_body = {"filters": [], "orderBy": [{"orderBy": "ActivityRunStart", "order": "DESC"}], "lastUpdatedAfter": "2020-01-01T00:00:00Z", "lastUpdatedBefore": now_utc().isoformat()}
    activity_response = requests.post(f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/datapipelines/pipelineruns/{main_run_id}/queryactivityruns", headers=api_headers, json=activity_body, timeout=120)
    activity_response.raise_for_status()
    activity_rows = []
    for activity in activity_response.json().get("value", []):
        error = activity.get("error") or {}
        activity_rows.append({"snapshotAt": snapshot_at, "pipelineRunId": main_run_id, "activityName": activity.get("activityName"), "activityType": activity.get("activityType"), "activityRunId": activity.get("activityRunId"), "status": activity.get("status"), "activityRunStart": activity.get("activityRunStart"), "activityRunEnd": activity.get("activityRunEnd"), "durationInMs": activity.get("durationInMs"), "errorCode": error.get("errorCode"), "errorMessage": error.get("message"), "failureType": error.get("failureType")})
    activity_path = table_path(admin_id, MAIN_ACTIVITY_TABLE)
    ensure_table(activity_path, MAIN_ACTIVITY_SCHEMA)
    if activity_rows:
        upsert_deduped(activity_path, activity_rows, MAIN_ACTIVITY_SCHEMA, ["pipelineRunId", "activityRunId"])

metrics_path = table_path(admin_id, METRICS_TABLE) if "METRICS_TABLE" in globals() else table_path(admin_id, "ImagingMetastoreExtensionMetrics")
runs_path = table_path(admin_id, "ImagingMetastoreExtensionRun")
tag_path = table_path(admin_id, TAG_METRICS_TABLE)
ensure_table(tag_path, TAG_METRICS_SCHEMA)
if DeltaTable.isDeltaTable(spark, metrics_path) and DeltaTable.isDeltaTable(spark, runs_path):
    latest_success = spark.read.format("delta").load(runs_path).where(F.col("status").isin("Succeeded", "SucceededWithCleanupWarning")).orderBy(F.col("completedAt").desc()).limit(1).select("extractRunId", "batchId", "sourceRowsActive").collect()
    if latest_success:
        run_info = latest_success[0]
        tag_rows = spark.read.format("delta").load(metrics_path).where((F.col("extractRunId") == F.lit(run_info["extractRunId"])) & (F.col("metricScope") == F.lit("tagCoverage")) & (F.col("metricName") == F.lit("nonNullValueRows"))).select(F.lit(snapshot_at).cast("timestamp").alias("snapshotAt"), "extractRunId", "batchId", "dicomTag", "dicomKeyword", F.col("metricValue").alias("rowsWithValue"), F.lit(float(run_info["sourceRowsActive"] or 0)).alias("rowsProcessed"), (F.col("metricValue") / F.lit(float(run_info["sourceRowsActive"] or 0)) * F.lit(100.0)).alias("coveragePercent"))
        tag_rows.write.format("delta").mode("append").save(tag_path)
print({"mainPipelineRunId": main_run_id, "materializedAt": snapshot_at})

# Capture the disabled-or-enabled patch pipeline independently for future executive reporting.
patch_run_id = str(runtime_param("PATCH_PIPELINE_RUN_ID", PATCH_PIPELINE_RUN_ID) or "").strip()
patch_pipeline_id = str(runtime_param("PATCH_PIPELINE_ID", PATCH_PIPELINE_ID)).strip()
if not patch_run_id:
    patch_latest = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{patch_pipeline_id}/jobs/instances?limit=1", headers=api_headers, timeout=60)
    if patch_latest.status_code == 200:
        patch_values = patch_latest.json().get("value", [])
        if patch_values:
            patch_run_id = patch_values[0].get("id", "")
if patch_run_id:
    patch_run_response = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{patch_pipeline_id}/jobs/instances/{patch_run_id}", headers=api_headers, timeout=60)
    if patch_run_response.status_code == 200:
        patch_run = patch_run_response.json()
        patch_run_path = table_path(admin_id, PATCH_RUN_TABLE)
        ensure_table(patch_run_path, MAIN_RUN_SCHEMA)
        spark.createDataFrame([{"snapshotAt": snapshot_at, "pipelineId": patch_pipeline_id, "pipelineRunId": patch_run_id, "status": patch_run.get("status"), "invokeType": patch_run.get("invokeType"), "startTimeUtc": patch_run.get("startTimeUtc"), "endTimeUtc": patch_run.get("endTimeUtc"), "failureReason": str(patch_run.get("failureReason")) if patch_run.get("failureReason") else None}], MAIN_RUN_SCHEMA).write.format("delta").mode("append").save(patch_run_path)
        patch_activity_response = requests.post(f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/datapipelines/pipelineruns/{patch_run_id}/queryactivityruns", headers=api_headers, json=activity_body, timeout=120)
        if patch_activity_response.status_code == 200:
            patch_activity_rows = []
            for activity in patch_activity_response.json().get("value", []):
                error = activity.get("error") or {}
                patch_activity_rows.append({"snapshotAt": snapshot_at, "pipelineRunId": patch_run_id, "activityName": activity.get("activityName"), "activityType": activity.get("activityType"), "activityRunId": activity.get("activityRunId"), "status": activity.get("status"), "activityRunStart": activity.get("activityRunStart"), "activityRunEnd": activity.get("activityRunEnd"), "durationInMs": activity.get("durationInMs"), "errorCode": error.get("errorCode"), "errorMessage": error.get("message"), "failureType": error.get("failureType")})
            patch_activity_path = table_path(admin_id, PATCH_ACTIVITY_TABLE)
            ensure_table(patch_activity_path, MAIN_ACTIVITY_SCHEMA)
            if patch_activity_rows:
                spark.createDataFrame(patch_activity_rows, MAIN_ACTIVITY_SCHEMA).write.format("delta").mode("append").save(patch_activity_path)
print({"patchPipelineRunId": patch_run_id, "materializedAt": snapshot_at})
