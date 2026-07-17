# Fabric Notebook: Resolve Historical Imaging Metastore Extension Anomalies
# This preserves anomaly history and marks existing current anomalies resolved.

# Fabric parameters. Deployment tags the notebook code cell as a parameter cell so
# RunNotebook executionData.parameters can override these defaults.
CONFIRM_RESOLVE = ""

import notebookutils
import requests
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
from datetime import datetime, timezone

spark = SparkSession.builder.getOrCreate()
WORKSPACE_ID = notebookutils.fabric.resolve_workspace_id()
ADMIN_LH_NAME = "healthcare1_msft_admin"
ANOMALY_TABLE = "ImagingMetastoreExtensionAnomaly"

if str(CONFIRM_RESOLVE).strip() != "RESOLVE_HISTORICAL_IMAGING_METASTORE_EXTENSION_ANOMALIES":
    raise ValueError("Refusing to resolve anomalies without explicit confirmation.")

token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
response = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/lakehouses",
    headers={"Authorization": f"Bearer {token}"},
    timeout=60,
)
response.raise_for_status()
admin_id = next(x["id"] for x in response.json().get("value", []) if x.get("displayName") == ADMIN_LH_NAME)
path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{admin_id}/Tables/{ANOMALY_TABLE}"
if not DeltaTable.isDeltaTable(spark, path):
    print("Anomaly table does not exist; nothing to resolve.")
else:
    table = spark.read.format("delta").load(path)
    if "isCurrent" not in table.columns:
        table = table.withColumn("isCurrent", F.lit(True).cast("boolean"))
    if "resolvedAt" not in table.columns:
        table = table.withColumn("resolvedAt", F.lit(None).cast("timestamp"))
    current_before = table.where(F.col("isCurrent") == F.lit(True)).count()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    resolved = table.withColumn(
        "resolvedAt",
        F.when(F.col("isCurrent") == F.lit(True), F.lit(now).cast("timestamp")).otherwise(F.col("resolvedAt")),
    ).withColumn("isCurrent", F.lit(False).cast("boolean"))
    resolved.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    print({"resolvedCurrentAnomalies": current_before, "resolvedAt": now.isoformat()})
