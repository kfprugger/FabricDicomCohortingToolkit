# Fabric Notebook: Add or Update DICOM Tag
#
# Customer-facing tag management workflow. PREVIEW performs no persistent writes.
# APPLY writes a validated configuration and runs a bounded extraction without
# advancing the production watermark. BACKFILL supports selected-study batches.

# Fabric parameters. Deployment tags the notebook code cell as a parameter cell so
# RunNotebook executionData.parameters can override these defaults.
ACTION = "PREVIEW"
DICOM_TAG = ""
DICOM_KEYWORD = ""
TARGET_COLUMN = ""
EXPECTED_VR = ""
SCOPE = "study"
IS_PHI = "PENDING"
PHI_CATEGORY = ""
APPROVAL_REFERENCE = ""
APPROVED_PLAN_ID = ""
SAMPLE_STUDY_COUNT = "5"
FILTER_STUDY_INSTANCE_UIDS = ""
DISCOVERY_SCAN_LIMIT = "50000"
CONFIRM_APPLY = ""
BACKFILL_MODE = "NONE"
CONFIRM_BACKFILL = ""
MAX_SOURCE_ROWS_PER_BATCH = "50000"
LEASE_TIMEOUT_MINUTES = "60"

import hashlib
import json
import math
import re
from datetime import datetime, timezone

import notebookutils
import requests
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.getOrCreate()

SILVER_LH_NAME = "healthcare1_msft_silver"
ADMIN_LH_NAME = "healthcare1_msft_admin"
SOURCE_TABLE = "ImagingMetastore"
TARGET_TABLE = "ImagingMetastoreExtension"
TAG_DICTIONARY_TABLE = "DicomTagDictionary"
GOVERNANCE_TABLE = "ImagingMetastoreExtensionGovernance"
RUN_TABLE = "ImagingMetastoreExtensionRun"
METRICS_TABLE = "ImagingMetastoreExtensionMetrics"
EXTRACTION_NOTEBOOK = "02_extract_imaging_metastore_extension"
SUPPORTED_ACTIONS = {"PREVIEW", "APPLY", "BACKFILL"}
SUPPORTED_SCOPES = {"patient", "study", "series", "instance", "procedure"}
SUPPORTED_VALUE_MODES = {
    "FIRST_SCALAR",
    "PERSON_NAME_ALPHABETIC",
    "ALL_STRINGS_JSON",
    "ALL_DISTINCT_STRINGS_JSON",
}
STRING_VALUE_VRS = {
    "AE", "AS", "CS", "DA", "DS", "DT", "IS", "LO", "LT", "SH", "ST", "TM", "UC", "UI", "UR", "UT",
}
TAG_PATH_PATTERN = re.compile(r"^\$\.(?:[0-9A-F]{8}\.Value\[\*\]\.)*[0-9A-F]{8}\.Value\[\*\]$")
RESERVED_TARGET_COLUMNS = {
    "id",
    "imagingMetastoreId",
    "sourceRecordKey",
    "imagingStudyId",
    "msftSourceSystem",
    "studyInstanceUid",
    "seriesInstanceUid",
    "sopInstanceUid",
    "filePath",
    "sourceModifiedAt",
    "sourceModifiedDate",
    "sourceSystemHashBucket",
    "isActive",
    "sourceRowHash",
    "extractedAt",
    "extractRunId",
}


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


def _unwrap_param_value(value):
    if isinstance(value, dict) and "value" in value:
        return value.get("value")
    return value


def get_param(name, default):
    if name in globals() and globals().get(name) is not None:
        return _unwrap_param_value(globals().get(name))
    context = _runtime_context()
    direct = _context_get(context, name, None)
    if direct is not None:
        return _unwrap_param_value(direct)
    params = _context_get(context, "parameters", {}) or {}
    value = _context_get(params, name, None)
    return _unwrap_param_value(value) if value is not None else default


def parse_int_param(name, default):
    raw = str(get_param(name, default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer; got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive.")
    return value


def parse_csv_param(name):
    raw = str(get_param(name, "") or "").strip()
    return [value.strip() for value in raw.split(",") if value.strip()]


def parse_governance_bool(raw):
    text = str(raw).strip().lower()
    if text == "pending":
        return None
    if text in ("true", "1", "yes", "y"):
        return True
    if text in ("false", "0", "no", "n"):
        return False
    raise ValueError("IS_PHI must be PENDING, true, or false.")


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
    raise ValueError(f"Lakehouse {name!r} was not found in workspace {WORKSPACE_ID}.")


def table_path(lakehouse_id, table_name):
    return f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{table_name}"


def sql_type(data_type):
    if isinstance(data_type, T.StringType):
        return "STRING"
    if isinstance(data_type, T.BooleanType):
        return "BOOLEAN"
    if isinstance(data_type, T.TimestampType):
        return "TIMESTAMP"
    raise TypeError(f"Unsupported Delta schema type: {data_type.simpleString()}")


def ensure_delta_table(path, schema):
    if DeltaTable.isDeltaTable(spark, path):
        existing_columns = set(spark.read.format("delta").load(path).columns)
        missing = [field for field in schema.fields if field.name not in existing_columns]
        if missing:
            spark.sql(
                f"ALTER TABLE delta.`{path}` ADD COLUMNS ("
                + ", ".join(f"{field.name} {sql_type(field.dataType)}" for field in missing)
                + ")"
            )
        return
    spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(path)


def metadata_json_expr(df):
    if "metadata" in df.columns:
        metadata_type = df.schema["metadata"].dataType
        if isinstance(metadata_type, (T.MapType, T.StructType)):
            return F.to_json(F.col("metadata"))
        if isinstance(metadata_type, T.StringType):
            return F.col("metadata")
    if "metadata_string" in df.columns:
        return F.col("metadata_string")
    raise ValueError("ImagingMetastore requires metadata or metadata_string.")


def discover_paths_in_payload(payload, target_tag):
    try:
        root = json.loads(payload)
    except (TypeError, json.JSONDecodeError):
        return set(), set(), False
    paths = set()
    observed_vrs = set()

    def walk(dataset, prefix):
        if not isinstance(dataset, dict):
            return
        for tag, element in dataset.items():
            if not isinstance(element, dict):
                continue
            values = element.get("Value")
            element_path = f"{prefix}.{tag}.Value[*]"
            if tag == target_tag:
                paths.add(element_path)
                vr = element.get("vr")
                if vr:
                    observed_vrs.add(str(vr))
            if isinstance(values, list):
                child_prefix = f"{prefix}.{tag}.Value[*]"
                for item in values:
                    if isinstance(item, dict):
                        walk(item, child_prefix)

    walk(root, "$")
    return paths, observed_vrs, True


def path_tag_chain(path):
    if not TAG_PATH_PATTERN.fullmatch(path):
        raise ValueError(f"Unsupported discovered DICOM JSON path: {path!r}")
    return re.findall(r"([0-9A-F]{8})\.Value\[\*\]", path)


def extract_values_from_path(root, path):
    tag_chain = path_tag_chain(path)

    def descend(dataset, index):
        if not isinstance(dataset, dict):
            return []
        element = dataset.get(tag_chain[index])
        if not isinstance(element, dict):
            return []
        values = element.get("Value")
        if not isinstance(values, list):
            return []
        if index == len(tag_chain) - 1:
            return [value for value in values if isinstance(value, (str, int, float))]
        result = []
        for item in values:
            result.extend(descend(item, index + 1))
        return result

    return descend(root, 0)


def analyze_payload_partition(rows, target_tag, cap):
    path_counts = {}
    observed_vrs = set()
    valid_json_rows = 0
    rows_with_values = 0
    maximum_values = 0
    analyzed_rows = 0
    for row in rows:
        if analyzed_rows >= cap:
            break
        analyzed_rows += 1
        payload = row["_metadata_json"]
        paths, row_vrs, valid_json = discover_paths_in_payload(payload, target_tag)
        if not valid_json:
            continue
        valid_json_rows += 1
        observed_vrs.update(row_vrs)
        try:
            root = json.loads(payload)
        except json.JSONDecodeError:
            continue
        row_values = []
        for path in paths:
            path_counts[path] = path_counts.get(path, 0) + 1
            row_values.extend(extract_values_from_path(root, path))
        non_empty = [str(value) for value in row_values if str(value).strip()]
        if non_empty:
            rows_with_values += 1
        maximum_values = max(maximum_values, len(non_empty))
    yield {
        "pathCounts": path_counts,
        "observedVrs": sorted(observed_vrs),
        "validJsonRows": valid_json_rows,
        "rowsWithValues": rows_with_values,
        "maximumValues": maximum_values,
        "analyzedRows": analyzed_rows,
    }


def infer_value_mode(paths, expected_vr, maximum_values):
    if expected_vr == "PN" and len(paths) == 1 and len(path_tag_chain(paths[0])) == 1:
        return "PERSON_NAME_ALPHABETIC"
    has_nested_path = any(len(path_tag_chain(path)) > 1 for path in paths)
    if not has_nested_path and len(paths) == 1 and maximum_values <= 1:
        return "FIRST_SCALAR"
    if expected_vr not in STRING_VALUE_VRS:
        return "CUSTOM_ENGINEERING_REQUIRED"
    return "ALL_DISTINCT_STRINGS_JSON"


def ordered_paths(paths):
    return sorted(paths, key=lambda path: (len(path_tag_chain(path)), path))


def plan_payload(paths, value_mode):
    return {
        "dicomTag": DICOM_TAG,
        "dicomKeyword": DICOM_KEYWORD,
        "canonicalColumnName": TARGET_COLUMN,
        "vrExpected": EXPECTED_VR,
        "scope": SCOPE,
        "jsonPaths": json.dumps(paths, separators=(",", ":")),
        "valueMode": value_mode,
        "pathPrecedence": "LISTED_ORDER",
        "targetDataType": "string",
    }


def configuration_hash(payload):
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def print_summary(summary):
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))


ACTION = str(get_param("ACTION", "PREVIEW")).strip().upper()
DICOM_TAG = str(get_param("DICOM_TAG", "")).strip().upper()
DICOM_KEYWORD = str(get_param("DICOM_KEYWORD", "")).strip()
TARGET_COLUMN = str(get_param("TARGET_COLUMN", "")).strip()
EXPECTED_VR = str(get_param("EXPECTED_VR", "")).strip().upper()
SCOPE = str(get_param("SCOPE", "study")).strip().lower()
IS_PHI = parse_governance_bool(get_param("IS_PHI", "PENDING"))
PHI_CATEGORY = str(get_param("PHI_CATEGORY", "")).strip()
APPROVAL_REFERENCE = str(get_param("APPROVAL_REFERENCE", "")).strip()
APPROVED_PLAN_ID = str(get_param("APPROVED_PLAN_ID", "")).strip()
SAMPLE_STUDY_COUNT = parse_int_param("SAMPLE_STUDY_COUNT", 5)
FILTER_STUDY_INSTANCE_UIDS = parse_csv_param("FILTER_STUDY_INSTANCE_UIDS")
DISCOVERY_SCAN_LIMIT = parse_int_param("DISCOVERY_SCAN_LIMIT", 50000)
CONFIRM_APPLY = str(get_param("CONFIRM_APPLY", "")).strip()
BACKFILL_MODE = str(get_param("BACKFILL_MODE", "NONE")).strip().upper()
CONFIRM_BACKFILL = str(get_param("CONFIRM_BACKFILL", "")).strip()
MAX_SOURCE_ROWS_PER_BATCH = parse_int_param("MAX_SOURCE_ROWS_PER_BATCH", 50000)
LEASE_TIMEOUT_MINUTES = parse_int_param("LEASE_TIMEOUT_MINUTES", 60)

if ACTION not in SUPPORTED_ACTIONS:
    raise ValueError(f"ACTION must be one of {sorted(SUPPORTED_ACTIONS)}.")
if not re.fullmatch(r"[0-9A-F]{8}", DICOM_TAG):
    raise ValueError("DICOM_TAG must contain eight uppercase hexadecimal characters.")
if not re.fullmatch(r"[A-Za-z][A-Za-z0-9]*", DICOM_KEYWORD):
    raise ValueError("DICOM_KEYWORD must be a non-empty alphanumeric DICOM keyword.")
if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", TARGET_COLUMN):
    raise ValueError("TARGET_COLUMN must be a valid stable column name.")
if not re.fullmatch(r"[A-Z]{2}", EXPECTED_VR):
    raise ValueError("EXPECTED_VR must be a two-letter uppercase VR.")
if SCOPE not in SUPPORTED_SCOPES:
    raise ValueError(f"SCOPE must be one of {sorted(SUPPORTED_SCOPES)}.")

WORKSPACE_ID = resolve_workspace_id()
SILVER_LH_ID = resolve_lakehouse_id(SILVER_LH_NAME)
ADMIN_LH_ID = resolve_lakehouse_id(ADMIN_LH_NAME)
SOURCE_PATH = table_path(SILVER_LH_ID, SOURCE_TABLE)
TARGET_PATH = table_path(SILVER_LH_ID, TARGET_TABLE)
DICTIONARY_PATH = table_path(ADMIN_LH_ID, TAG_DICTIONARY_TABLE)
GOVERNANCE_PATH = table_path(ADMIN_LH_ID, GOVERNANCE_TABLE)
RUN_PATH = table_path(ADMIN_LH_ID, RUN_TABLE)
METRICS_PATH = table_path(ADMIN_LH_ID, METRICS_TABLE)

source = spark.read.format("delta").load(SOURCE_PATH)
active_source = source.where(F.coalesce(F.col("msftIsDeleted"), F.lit(False)) == F.lit(False))
source_with_metadata = active_source.withColumn("_metadata_json", metadata_json_expr(active_source))
candidate_source = source_with_metadata.where(F.col("_metadata_json").contains(f'"{DICOM_TAG}"'))
# Bounded identity sample carries only small columns, so the global limit never
# materializes large _metadata_json blobs in a single task.
identity_sample = (
    candidate_source
    .select("studyInstanceUid", "msftSourceSystem", "sourceModifiedAt")
    .limit(DISCOVERY_SCAN_LIMIT + 1)
    .cache()
)
bounded_count = identity_sample.count()
discovery_truncated = bounded_count > DISCOVERY_SCAN_LIMIT
candidate_sample = identity_sample.limit(DISCOVERY_SCAN_LIMIT)
source_rows_scanned = min(bounded_count, DISCOVERY_SCAN_LIMIT)
study_count = candidate_sample.select("studyInstanceUid").where(F.col("studyInstanceUid").isNotNull()).distinct().count()
source_system_count = candidate_sample.select("msftSourceSystem").distinct().count()

# Distributed, bounded payload discovery: analyze up to DISCOVERY_SCAN_LIMIT candidate rows
# spread across Spark partitions instead of collapsing every _metadata_json blob into a single
# global-limit task. Each partition analyzes at most per_partition_cap rows.
num_partitions = max(1, candidate_source.rdd.getNumPartitions())
per_partition_cap = max(1, math.ceil((DISCOVERY_SCAN_LIMIT + num_partitions - 1) / num_partitions))
path_counts = {}
observed_vrs = set()
valid_json_rows = 0
rows_with_values = 0
maximum_values = 0
analyzed_sample_rows = 0
partition_summaries = (
    candidate_source.select("_metadata_json").rdd
    .mapPartitions(lambda rows: analyze_payload_partition(rows, DICOM_TAG, per_partition_cap))
    .collect()
)
for partition in partition_summaries:
    valid_json_rows += partition["validJsonRows"]
    rows_with_values += partition["rowsWithValues"]
    maximum_values = max(maximum_values, partition["maximumValues"])
    analyzed_sample_rows += partition["analyzedRows"]
    observed_vrs.update(partition["observedVrs"])
    for path, count in partition["pathCounts"].items():
        path_counts[path] = path_counts.get(path, 0) + count

paths = ordered_paths(path_counts)
value_mode = infer_value_mode(paths, EXPECTED_VR, maximum_values) if paths else None
payload = plan_payload(paths, value_mode) if paths and value_mode else None
config_hash = configuration_hash(payload) if payload else None
plan_id = f"TAG-{DICOM_TAG}-{config_hash[:16]}" if config_hash else None

existing_dictionary_rows = 0
same_tag_column_owner_rows = 0
canonical_collision_rows = 0
dictionary_preview = None
if DeltaTable.isDeltaTable(spark, DICTIONARY_PATH):
    dictionary_preview = spark.read.format("delta").load(DICTIONARY_PATH)
    existing_dictionary_rows = dictionary_preview.where(F.col("dicomTag") == DICOM_TAG).count()
    same_tag_column_owner_rows = dictionary_preview.where(
        (F.col("dicomTag") == DICOM_TAG) & (F.col("canonicalColumnName") == TARGET_COLUMN)
    ).count()
    canonical_collision_rows = dictionary_preview.where(
        (F.col("canonicalColumnName") == TARGET_COLUMN) & (F.col("dicomTag") != DICOM_TAG)
    ).count()
existing_target_column = False
if DeltaTable.isDeltaTable(spark, TARGET_PATH):
    existing_target_column = TARGET_COLUMN in spark.read.format("delta").load(TARGET_PATH).columns
target_column_collision = (
    TARGET_COLUMN in RESERVED_TARGET_COLUMNS
    or (existing_target_column and same_tag_column_owner_rows != 1)
)

if canonical_collision_rows or target_column_collision:
    preview_status = "BLOCKED_COLUMN_COLLISION"
elif source_rows_scanned == 0:
    preview_status = "NOT_FOUND"
elif discovery_truncated:
    preview_status = "BLOCKED_DISCOVERY_LIMIT_REACHED"
elif not paths:
    preview_status = "NEEDS_ENGINEERING"
elif value_mode == "CUSTOM_ENGINEERING_REQUIRED":
    preview_status = "NEEDS_ENGINEERING"
elif observed_vrs and observed_vrs != {EXPECTED_VR}:
    preview_status = "BLOCKED_VR_MISMATCH"
else:
    preview_status = "READY_FOR_GOVERNANCE"

preview_summary = {
    "action": ACTION,
    "status": preview_status,
    "writesPerformed": False,
    "dicomTag": DICOM_TAG,
    "dicomKeyword": DICOM_KEYWORD,
    "targetColumn": TARGET_COLUMN,
    "expectedVr": EXPECTED_VR,
    "observedVrs": sorted(observed_vrs),
    "sourceRowsScanned": source_rows_scanned,
    "sourceRowsTruncatedAtLimit": discovery_truncated,
    "studiesInScan": study_count,
    "sourceSystemsInScan": source_system_count,
    "validJsonSampleRows": valid_json_rows,
    "analyzedSampleRows": analyzed_sample_rows,
    "sampleRowsWithValues": rows_with_values,
    "maximumValuesInSampleRow": maximum_values,
    "discoveredPaths": paths,
    "pathSampleCounts": {path: path_counts[path] for path in paths},
    "recommendedValueMode": value_mode,
    "existingDictionaryRows": existing_dictionary_rows,
    "canonicalColumnCollisions": canonical_collision_rows,
    "targetColumnCollision": target_column_collision,
    "targetColumnAlreadyExists": existing_target_column,
    "planId": plan_id,
}

if ACTION == "PREVIEW":
    print_summary(preview_summary)
else:
    if preview_status != "READY_FOR_GOVERNANCE":
        raise ValueError(f"Tag plan is not ready to apply: {preview_status}")
    if IS_PHI is None:
        raise ValueError("IS_PHI must be approved as true or false before APPLY/BACKFILL.")
    if IS_PHI and not PHI_CATEGORY:
        raise ValueError("PHI_CATEGORY is required when IS_PHI=true.")
    if not APPROVAL_REFERENCE:
        raise ValueError("APPROVAL_REFERENCE is required before APPLY/BACKFILL.")

    if ACTION == "APPLY":
        if value_mode not in SUPPORTED_VALUE_MODES:
            raise ValueError(f"The proposed value mode is not supported: {value_mode}")
        if APPROVED_PLAN_ID != plan_id:
            raise ValueError(f"APPROVED_PLAN_ID must exactly match the current preview plan ID: {plan_id}")
        expected_confirmation = f"ADD_DICOM_TAG_{DICOM_TAG}"
        if CONFIRM_APPLY != expected_confirmation:
            raise ValueError(f"CONFIRM_APPLY must equal {expected_confirmation!r}.")

        TAG_DICTIONARY_SCHEMA = T.StructType([
            T.StructField("dicomTag", T.StringType(), False),
            T.StructField("dicomKeyword", T.StringType(), False),
            T.StructField("canonicalColumnName", T.StringType(), False),
            T.StructField("vrExpected", T.StringType(), True),
            T.StructField("scope", T.StringType(), True),
            T.StructField("isWellKnown29", T.BooleanType(), False),
            T.StructField("isPhi", T.BooleanType(), False),
            T.StructField("phiCategory", T.StringType(), True),
            T.StructField("enabled", T.BooleanType(), False),
            T.StructField("jsonPaths", T.StringType(), True),
            T.StructField("valueMode", T.StringType(), True),
            T.StructField("pathPrecedence", T.StringType(), True),
            T.StructField("targetDataType", T.StringType(), True),
            T.StructField("status", T.StringType(), True),
            T.StructField("approvalReference", T.StringType(), True),
            T.StructField("configurationHash", T.StringType(), True),
            T.StructField("validatedAt", T.TimestampType(), True),
            T.StructField("createdAt", T.TimestampType(), False),
            T.StructField("updatedAt", T.TimestampType(), False),
        ])
        GOVERNANCE_SCHEMA = T.StructType([
            T.StructField("targetTable", T.StringType(), False),
            T.StructField("columnName", T.StringType(), False),
            T.StructField("dicomTag", T.StringType(), True),
            T.StructField("dicomKeyword", T.StringType(), True),
            T.StructField("isPhi", T.BooleanType(), False),
            T.StructField("phiCategory", T.StringType(), True),
            T.StructField("recommendedSensitivityLabel", T.StringType(), True),
            T.StructField("createdAt", T.TimestampType(), False),
        ])
        ensure_delta_table(DICTIONARY_PATH, TAG_DICTIONARY_SCHEMA)
        ensure_delta_table(GOVERNANCE_PATH, GOVERNANCE_SCHEMA)
        now = current_utc_naive()
        dictionary_row = {
            **payload,
            "isWellKnown29": False,
            "isPhi": IS_PHI,
            "phiCategory": PHI_CATEGORY or "None",
            "enabled": False,
            "status": "Validating",
            "approvalReference": APPROVAL_REFERENCE,
            "configurationHash": config_hash,
            "validatedAt": None,
            "createdAt": now,
            "updatedAt": now,
        }
        dictionary_df = spark.createDataFrame([dictionary_row], TAG_DICTIONARY_SCHEMA)
        dictionary_target = DeltaTable.forPath(spark, DICTIONARY_PATH)
        update_fields = {
            field.name: f"s.{field.name}"
            for field in TAG_DICTIONARY_SCHEMA.fields
            if field.name not in ("dicomTag", "createdAt")
        }
        insert_fields = {field.name: f"s.{field.name}" for field in TAG_DICTIONARY_SCHEMA.fields}
        (
            dictionary_target.alias("t")
            .merge(dictionary_df.alias("s"), "t.dicomTag = s.dicomTag")
            .whenMatchedUpdate(set=update_fields)
            .whenNotMatchedInsert(values=insert_fields)
            .execute()
        )

        governance_row = {
            "targetTable": TARGET_TABLE,
            "columnName": TARGET_COLUMN,
            "dicomTag": DICOM_TAG,
            "dicomKeyword": DICOM_KEYWORD,
            "isPhi": IS_PHI,
            "phiCategory": PHI_CATEGORY or "None",
            "recommendedSensitivityLabel": "Confidential - PHI" if IS_PHI else "General",
            "createdAt": now,
        }
        governance_df = spark.createDataFrame([governance_row], GOVERNANCE_SCHEMA)
        (
            DeltaTable.forPath(spark, GOVERNANCE_PATH).alias("t")
            .merge(
                governance_df.alias("s"),
                "t.targetTable = s.targetTable AND t.columnName = s.columnName",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        if FILTER_STUDY_INSTANCE_UIDS:
            validation_uids = FILTER_STUDY_INSTANCE_UIDS
        else:
            validation_uids = [
                row["studyInstanceUid"]
                for row in (
                    candidate_sample.where(F.col("studyInstanceUid").isNotNull())
                    .groupBy("studyInstanceUid")
                    .agg(F.max("sourceModifiedAt").alias("latestSourceModifiedAt"))
                    .orderBy(F.col("latestSourceModifiedAt").desc_nulls_last(), F.col("studyInstanceUid").asc())
                    .limit(SAMPLE_STUDY_COUNT)
                    .collect()
                )
            ]
        if not validation_uids:
            raise ValueError("No validation Study Instance UIDs were selected.")

        validation_started_at = current_utc_naive()
        validation_orchestration_id = f"TAG_MANAGER_VALIDATE_{DICOM_TAG}"
        try:
            notebookutils.notebook.run(
                EXTRACTION_NOTEBOOK,
                7200,
                {
                    "FILTER_STUDY_INSTANCE_UIDS": ",".join(validation_uids),
                    "DRY_RUN_ONLY": "false",
                    "VALIDATION_TAG": DICOM_TAG,
                    "ORCHESTRATION_RUN_ID": validation_orchestration_id,
                    "MAX_SOURCE_ROWS_PER_BATCH": str(MAX_SOURCE_ROWS_PER_BATCH),
                    "LEASE_TIMEOUT_MINUTES": str(LEASE_TIMEOUT_MINUTES),
                },
            )
            run = (
                spark.read.format("delta").load(RUN_PATH)
                .where(
                    (
                        (F.col("validationTag") == DICOM_TAG)
                        | (F.col("orchestrationRunId") == validation_orchestration_id)
                    )
                    & (F.col("startedAt") >= F.lit(validation_started_at).cast("timestamp"))
                )
                .orderBy(F.col("startedAt").desc())
                .limit(1)
                .collect()
            )
            if not run:
                raise ValueError("The bounded extraction completed without a validation audit row.")
            run = run[0]
            if run["status"] not in ("Succeeded", "SucceededWithCleanupWarning"):
                raise ValueError(f"Bounded extraction status was {run['status']}.")
            if (run["metadataRowsParseFailed"] or 0) != 0:
                raise ValueError("Bounded validation requires zero metadata parse failures.")
            if (run["sourceRowsMissingImagingStudy"] or 0) != 0:
                raise ValueError("Bounded validation requires zero missing ImagingStudy joins.")
            coverage = (
                spark.read.format("delta").load(METRICS_PATH)
                .where(
                    (F.col("extractRunId") == run["extractRunId"])
                    & (F.col("dicomTag") == DICOM_TAG)
                    & (F.col("metricScope") == "tagCoverage")
                    & (F.col("metricName") == "nonNullValueRows")
                )
                .agg(F.max("metricValue").alias("coverage"))
                .collect()[0]["coverage"]
            )
            if not coverage or coverage <= 0:
                raise ValueError("Bounded validation produced no populated values for the new tag.")
            dictionary_target.update(
                condition=f"dicomTag = '{DICOM_TAG}'",
                set={
                    "enabled": "true",
                    "status": "'Enabled'",
                    "validatedAt": "current_timestamp()",
                    "updatedAt": "current_timestamp()",
                },
            )
            print_summary({
                **preview_summary,
                "action": "APPLY",
                "status": "BOUNDED_VALIDATION_PASSED",
                "writesPerformed": True,
                "extractRunId": run["extractRunId"],
                "sourceRowsScanned": run["sourceRowsScanned"],
                "extensionRowsStaged": run["extensionRowsStaged"],
                "extensionRowsInserted": run["extensionRowsInserted"],
                "extensionRowsUpdated": run["extensionRowsUpdated"],
                "tagCoverageRows": coverage,
                "watermarkAdvanced": False,
            })
        except Exception:
            dictionary_target.update(
                condition=f"dicomTag = '{DICOM_TAG}'",
                set={
                    "enabled": "false",
                    "status": "'Failed'",
                    "updatedAt": "current_timestamp()",
                },
            )
            raise

    elif ACTION == "BACKFILL":
        if dictionary_preview is None:
            raise ValueError(f"DICOM tag {DICOM_TAG} is not configured; run APPLY first.")
        required_configuration_columns = {"enabled", "status", "configurationHash"}
        if not required_configuration_columns.issubset(set(dictionary_preview.columns)):
            raise ValueError("DicomTagDictionary is missing self-service configuration columns; redeploy and run the extractor migration.")
        enabled_configuration_rows = dictionary_preview.where(
            (F.col("dicomTag") == DICOM_TAG)
            & (F.col("enabled") == F.lit(True))
            & (F.col("status") == F.lit("Enabled"))
            & (F.col("configurationHash") == F.lit(config_hash))
        ).count()
        if enabled_configuration_rows != 1:
            raise ValueError(
                f"DICOM tag {DICOM_TAG} must have exactly one enabled configuration matching plan {plan_id} before BACKFILL."
            )
        if BACKFILL_MODE == "NONE":
            print_summary({
                **preview_summary,
                "action": "BACKFILL",
                "status": "INCREMENTAL_ONLY_SELECTED",
                "writesPerformed": False,
            })
        elif BACKFILL_MODE == "SELECTED_STUDIES":
            if not FILTER_STUDY_INSTANCE_UIDS:
                raise ValueError("FILTER_STUDY_INSTANCE_UIDS is required for SELECTED_STUDIES backfill.")
            expected = f"BACKFILL_SELECTED_STUDIES_{DICOM_TAG}"
            if CONFIRM_BACKFILL != expected:
                raise ValueError(f"CONFIRM_BACKFILL must equal {expected!r}.")
            notebookutils.notebook.run(
                EXTRACTION_NOTEBOOK,
                7200,
                {
                    "FILTER_STUDY_INSTANCE_UIDS": ",".join(FILTER_STUDY_INSTANCE_UIDS),
                    "DRY_RUN_ONLY": "false",
                    "MAX_SOURCE_ROWS_PER_BATCH": str(MAX_SOURCE_ROWS_PER_BATCH),
                    "LEASE_TIMEOUT_MINUTES": str(LEASE_TIMEOUT_MINUTES),
                },
            )
            print_summary({
                **preview_summary,
                "action": "BACKFILL",
                "status": "SELECTED_STUDIES_BACKFILL_COMPLETED",
                "writesPerformed": True,
                "watermarkAdvanced": False,
            })
        elif BACKFILL_MODE == "FULL_REBUILD":
            raise ValueError(
                "FULL_REBUILD is intentionally blocked in the customer notebook. "
                "Use the guarded engineering reset/orchestration process."
            )
        else:
            raise ValueError("BACKFILL_MODE must be NONE, SELECTED_STUDIES, or FULL_REBUILD.")
