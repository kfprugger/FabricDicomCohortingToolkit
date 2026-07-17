# Fabric Notebook: Extract Imaging Metastore Extension
#
# Active incremental extraction of configured DICOM metadata tags from Silver
# ImagingMetastore into one wide Delta table named ImagingMetastoreExtension.
# Delete/patch reconciliation is intentionally deferred to
# reconcile_imaging_metastore_extension_patch_operations.

# Fabric parameters. Deployment tags the notebook code cell as a parameter cell so
# RunNotebook executionData.parameters can override these defaults.
BATCH_START_SOURCE_MODIFIED_AT = ""
BATCH_END_SOURCE_MODIFIED_AT = ""
MAX_SOURCE_ROWS_PER_BATCH = "50000000"
WATERMARK_OVERLAP_HOURS = "24"
HASH_BUCKET_COUNT = "128"
MAX_ENABLED_TAGS = "500"
FILTER_STUDY_INSTANCE_UIDS = ""
SAMPLE_STUDY_COUNT = ""
DRY_RUN_ONLY = "false"
FILTER_SOURCE_SYSTEM = ""
FILTER_SOURCE_SYSTEM_HASH_BUCKET = ""
LEASE_TIMEOUT_MINUTES = "240"
MANIFEST_BATCH_ID = ""
ORCHESTRATION_RUN_ID = ""
CLEAR_TARGET_BEFORE_RUN = ""
VALIDATION_TAG = ""

import hashlib
import json
import re
import notebookutils
import requests
from pyspark.sql import SparkSession, functions as F, types as T, Window
from delta.tables import DeltaTable
from datetime import datetime, timezone, timedelta
from uuid import uuid4

spark = SparkSession.builder.getOrCreate()

SILVER_LH_NAME = "healthcare1_msft_silver"
ADMIN_LH_NAME = "healthcare1_msft_admin"
TARGET_TABLE = "ImagingMetastoreExtension"
RUN_TABLE = "ImagingMetastoreExtensionRun"
METRICS_TABLE = "ImagingMetastoreExtensionMetrics"
CONTROL_TABLE = "ImagingMetastoreExtensionControl"
TAG_DICTIONARY_TABLE = "DicomTagDictionary"
HEALTH_TABLE = "ImagingMetastoreExtensionHealth"
SLA_CONFIG_TABLE = "ImagingMetastoreExtensionSlaConfig"
TIME_ZONE_OPTION_TABLE = "ImagingMetastoreExtensionTimeZoneOption"
LEASE_TABLE = "ImagingMetastoreExtensionLease"
LEASE_LOCK_TABLE = "ImagingMetastoreExtensionLeaseLock"
FULL_BACKFILL_LOCK_TABLE = "ImagingMetastoreExtensionFullBackfillLock"
BATCH_MANIFEST_TABLE = "ImagingMetastoreExtensionBatchManifest"
PARSE_QUARANTINE_TABLE = "ImagingMetastoreExtensionParseQuarantine"
JOIN_QUARANTINE_TABLE = "ImagingMetastoreExtensionJoinQuarantine"
VALIDATION_QUARANTINE_TABLE = "ImagingMetastoreExtensionValidationQuarantine"
ANOMALY_TABLE = "ImagingMetastoreExtensionAnomaly"
GOVERNANCE_TABLE = "ImagingMetastoreExtensionGovernance"
CANARY_STUDY_TABLE = "ImagingMetastoreExtensionCanaryStudy"
PREFLIGHT_TABLE = "ImagingMetastoreExtensionPreflight"
ALERT_TABLE = "ImagingMetastoreExtensionAlert"
ORCHESTRATION_RUN_TABLE = "ImagingMetastoreExtensionOrchestrationRun"
PIPELINE_NAME = "extract_imaging_metastore_extension"
HASH_BUCKET_COUNT_DEFAULT = 128
MAX_ENABLED_TAGS_DEFAULT = 500
MAX_SOURCE_ROWS_PER_BATCH_DEFAULT = 50_000_000
WATERMARK_OVERLAP_HOURS_DEFAULT = 24
STALE_WARNING_HOURS_DEFAULT = 26
STALE_ERROR_HOURS_DEFAULT = 48
DISPLAY_TIME_ZONE_LABEL_DEFAULT = "Pacific Standard Time"
DISPLAY_UTC_OFFSET_HOURS_DEFAULT = -8.0
FUTURE_RECONCILIATION_NOTEBOOK = "10_reconcile_imaging_metastore_extension_patch_operations"

SEED_TAGS = [
    ("0020000D", "StudyInstanceUID", "studyInstanceUid", "UI", "study", True, False, "None", True),
    ("00100010", "PatientName", "patientName", "PN", "patient", True, True, "DirectIdentifier", True),
    ("00100040", "PatientSex", "patientSex", "CS", "patient", True, True, "QuasiIdentifier", True),
    ("00100020", "PatientID", "patientId", "LO", "patient", True, True, "DirectIdentifier", True),
    ("00100030", "PatientBirthDate", "patientBirthDate", "DA", "patient", True, True, "QuasiIdentifier", True),
    ("00080050", "AccessionNumber", "accessionNumber", "SH", "study", True, True, "DirectIdentifier", True),
    ("00080090", "ReferringPhysicianName", "referringPhysicianName", "PN", "study", True, True, "DirectIdentifier", True),
    ("00080020", "StudyDate", "studyDate", "DA", "study", True, False, "None", True),
    ("00081030", "StudyDescription", "studyDescription", "LO", "study", True, False, "None", True),
    ("0020000E", "SeriesInstanceUID", "seriesInstanceUid", "UI", "series", True, False, "None", True),
    ("00080060", "Modality", "modality", "CS", "series", True, False, "None", True),
    ("00080061", "ModalitiesInStudy", "modalitiesInStudy", "CS", "study", True, False, "None", True),
    ("00400244", "PerformedProcedureStepStartDate", "performedProcedureStepStartDate", "DA", "procedure", True, False, "None", True),
    ("00081090", "ManufacturerModelName", "manufacturerModelName", "LO", "series", True, False, "None", True),
    ("00080018", "SOPInstanceUID", "sopInstanceUid", "UI", "instance", True, False, "None", True),
    ("00080030", "StudyTime", "studyTime", "TM", "study", True, False, "None", True),
    ("00080201", "TimezoneOffsetFromUTC", "timezoneOffsetFromUtc", "SH", "study", True, False, "None", True),
    ("00201206", "NumberOfStudyRelatedSeries", "numberOfStudyRelatedSeries", "IS", "study", True, False, "None", True),
    ("00201208", "NumberOfStudyRelatedInstances", "numberOfStudyRelatedInstances", "IS", "study", True, False, "None", True),
    ("00200011", "SeriesNumber", "seriesNumber", "IS", "series", True, False, "None", True),
    ("0008103E", "SeriesDescription", "seriesDescription", "LO", "series", True, False, "None", True),
    ("00201209", "NumberOfSeriesRelatedInstances", "numberOfSeriesRelatedInstances", "IS", "series", True, False, "None", True),
    ("00180015", "BodyPartExamined", "bodyPartExamined", "CS", "series", True, False, "None", True),
    ("00200060", "Laterality", "laterality", "CS", "series", True, False, "None", True),
    ("00080021", "SeriesDate", "seriesDate", "DA", "series", True, False, "None", True),
    ("00080031", "SeriesTime", "seriesTime", "TM", "series", True, False, "None", True),
    ("00080016", "SOPClassUID", "sopClassUid", "UI", "instance", True, False, "None", True),
    ("00200013", "InstanceNumber", "instanceNumber", "IS", "instance", True, False, "None", True),
    ("00420010", "DocumentTitle", "documentTitle", "ST", "instance", True, False, "None", True),
]

SUPPORTED_VALUE_MODES = {
    "FIRST_SCALAR",
    "PERSON_NAME_ALPHABETIC",
    "ALL_STRINGS_JSON",
    "ALL_DISTINCT_STRINGS_JSON",
}
TAG_PATH_PATTERN = re.compile(r"^\$\.(?:[0-9A-F]{8}\.Value\[\*\]\.)*[0-9A-F]{8}\.Value\[\*\]$")


def default_tag_configuration(tag, keyword, canonical, vr, scope, is_well_known, is_phi, phi_category, enabled):
    json_paths = json.dumps([f"$.{tag}.Value[*]"], separators=(",", ":"))
    value_mode = "PERSON_NAME_ALPHABETIC" if vr == "PN" else "FIRST_SCALAR"
    payload = {
        "dicomTag": tag,
        "dicomKeyword": keyword,
        "canonicalColumnName": canonical,
        "vrExpected": vr,
        "scope": scope,
        "jsonPaths": json_paths,
        "valueMode": value_mode,
        "pathPrecedence": "LISTED_ORDER",
        "targetDataType": "string",
        "isPhi": bool(is_phi),
        "phiCategory": phi_category,
    }
    configuration_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        **payload,
        "isWellKnown29": bool(is_well_known),
        "enabled": bool(enabled),
        "status": "Enabled" if enabled else "Disabled",
        "approvalReference": "SYSTEM_SEED_29" if is_well_known else None,
        "configurationHash": configuration_hash,
    }

UID_TAG_COLUMN_RENAMES = {
    "studyInstanceUid": "studyInstanceUid_tag",
    "seriesInstanceUid": "seriesInstanceUid_tag",
    "sopInstanceUid": "sopInstanceUid_tag",
}

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

CONTROL_SCHEMA = T.StructType([
    T.StructField("pipelineName", T.StringType(), False),
    T.StructField("targetTable", T.StringType(), False),
    T.StructField("sourceSystem", T.StringType(), True),
    T.StructField("hashBucket", T.IntegerType(), True),
    T.StructField("lastSuccessfulHighWatermark", T.TimestampType(), True),
    T.StructField("lastSuccessfulRunId", T.StringType(), True),
    T.StructField("updatedAt", T.TimestampType(), False),
])

RUN_SCHEMA = T.StructType([
    T.StructField("extractRunId", T.StringType(), False),
    T.StructField("batchId", T.StringType(), False),
    T.StructField("workspaceId", T.StringType(), False),
    T.StructField("silverLakehouseId", T.StringType(), False),
    T.StructField("adminLakehouseId", T.StringType(), False),
    T.StructField("pipelineName", T.StringType(), False),
    T.StructField("orchestrationRunId", T.StringType(), True),
    T.StructField("manifestBatchId", T.StringType(), True),
    T.StructField("controlSourceSystem", T.StringType(), True),
    T.StructField("controlHashBucket", T.IntegerType(), True),
    T.StructField("dryRunOnly", T.BooleanType(), False),
    T.StructField("validationTag", T.StringType(), True),
    T.StructField("targetTable", T.StringType(), False),
    T.StructField("startedAt", T.TimestampType(), False),
    T.StructField("completedAt", T.TimestampType(), True),
    T.StructField("status", T.StringType(), False),
    T.StructField("batchStartSourceModifiedAt", T.TimestampType(), True),
    T.StructField("batchEndSourceModifiedAt", T.TimestampType(), True),
    T.StructField("lastSuccessfulHighWatermarkBeforeRun", T.TimestampType(), True),
    T.StructField("newHighWatermarkAfterRun", T.TimestampType(), True),
    T.StructField("sourceRowsScanned", T.LongType(), True),
    T.StructField("sourceRowsActive", T.LongType(), True),
    T.StructField("sourceRowsJoinedToImagingStudy", T.LongType(), True),
    T.StructField("sourceRowsMissingImagingStudy", T.LongType(), True),
    T.StructField("metadataRowsParsed", T.LongType(), True),
    T.StructField("metadataRowsParseFailed", T.LongType(), True),
    T.StructField("extensionRowsStaged", T.LongType(), True),
    T.StructField("extensionRowsInserted", T.LongType(), True),
    T.StructField("extensionRowsUpdated", T.LongType(), True),
    T.StructField("extensionRowsSoftDeleted", T.LongType(), True),
    T.StructField("extensionRowsPhysicallyDeleted", T.LongType(), True),
    T.StructField("durationSeconds", T.DoubleType(), True),
    T.StructField("errorClass", T.StringType(), True),
    T.StructField("errorMessage", T.StringType(), True),
    T.StructField("notebookRunId", T.StringType(), True),
])

METRICS_SCHEMA = T.StructType([
    T.StructField("extractRunId", T.StringType(), False),
    T.StructField("batchId", T.StringType(), False),
    T.StructField("metricScope", T.StringType(), False),
    T.StructField("metricName", T.StringType(), False),
    T.StructField("dicomTag", T.StringType(), True),
    T.StructField("dicomKeyword", T.StringType(), True),
    T.StructField("metricValue", T.DoubleType(), True),
    T.StructField("metricJson", T.StringType(), True),
    T.StructField("createdAt", T.TimestampType(), False),
])

HEALTH_SCHEMA = T.StructType([
    T.StructField("snapshotId", T.StringType(), False),
    T.StructField("snapshotAt", T.TimestampType(), False),
    T.StructField("extractRunId", T.StringType(), False),
    T.StructField("batchId", T.StringType(), False),
    T.StructField("pipelineName", T.StringType(), False),
    T.StructField("targetTable", T.StringType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("healthStatus", T.StringType(), False),
    T.StructField("healthStatusRank", T.IntegerType(), False),
    T.StructField("statusMessage", T.StringType(), True),
    T.StructField("startedAt", T.TimestampType(), True),
    T.StructField("completedAt", T.TimestampType(), True),
    T.StructField("snapshotAtDisplay", T.TimestampType(), False),
    T.StructField("displayTimeZoneLabel", T.StringType(), False),
    T.StructField("displayUtcOffsetHours", T.DoubleType(), False),
    T.StructField("durationSeconds", T.DoubleType(), True),
    T.StructField("totalExtensionRows", T.LongType(), True),
    T.StructField("startedAtDisplay", T.TimestampType(), True),
    T.StructField("completedAtDisplay", T.TimestampType(), True),
    T.StructField("distinctStudyInstanceUids", T.LongType(), True),
    T.StructField("distinctSeriesInstanceUids", T.LongType(), True),
    T.StructField("distinctSopInstanceUids", T.LongType(), True),
    T.StructField("sourceRowsScanned", T.LongType(), True),
    T.StructField("sourceRowsActive", T.LongType(), True),
    T.StructField("sourceRowsDeduplicated", T.LongType(), True),
    T.StructField("sourceRowsJoinedToImagingStudy", T.LongType(), True),
    T.StructField("sourceRowsMissingImagingStudy", T.LongType(), True),
    T.StructField("metadataRowsParsed", T.LongType(), True),
    T.StructField("metadataRowsParseFailed", T.LongType(), True),
    T.StructField("extensionRowsStaged", T.LongType(), True),
    T.StructField("extensionRowsInserted", T.LongType(), True),
    T.StructField("extensionRowsUpdated", T.LongType(), True),
    T.StructField("studyUidMismatchRows", T.LongType(), True),
    T.StructField("seriesUidMismatchRows", T.LongType(), True),
    T.StructField("sopUidMismatchRows", T.LongType(), True),
    T.StructField("hoursSinceLastSuccess", T.DoubleType(), True),
    T.StructField("staleWarningHours", T.LongType(), False),
    T.StructField("staleErrorHours", T.LongType(), False),
    T.StructField("maxParseFailureRows", T.LongType(), False),
    T.StructField("maxMissingImagingStudyRows", T.LongType(), False),
    T.StructField("maxUidMismatchRows", T.LongType(), False),
    T.StructField("maxDeduplicationRate", T.DoubleType(), True),
    T.StructField("createdAt", T.TimestampType(), False),
    T.StructField("createdAtDisplay", T.TimestampType(), False),
])

SLA_CONFIG_SCHEMA = T.StructType([
    T.StructField("pipelineName", T.StringType(), False),
    T.StructField("targetTable", T.StringType(), False),
    T.StructField("staleWarningHours", T.LongType(), False),
    T.StructField("staleErrorHours", T.LongType(), False),
    T.StructField("maxParseFailureRows", T.LongType(), False),
    T.StructField("maxMissingImagingStudyRows", T.LongType(), False),
    T.StructField("maxUidMismatchRows", T.LongType(), False),
    T.StructField("maxDeduplicationRate", T.DoubleType(), True),
    T.StructField("displayTimeZoneLabel", T.StringType(), False),
    T.StructField("displayUtcOffsetHours", T.DoubleType(), False),
    T.StructField("updatedAt", T.TimestampType(), False),
    T.StructField("updatedBy", T.StringType(), True),
])

TIME_ZONE_OPTION_SCHEMA = T.StructType([
    T.StructField("displayTimeZoneLabel", T.StringType(), False),
    T.StructField("displayUtcOffsetHours", T.DoubleType(), False),
    T.StructField("sortOrder", T.LongType(), False),
    T.StructField("isDefault", T.BooleanType(), False),
    T.StructField("updatedAt", T.TimestampType(), False),
    T.StructField("updatedBy", T.StringType(), True),
])

LEASE_SCHEMA = T.StructType([
    T.StructField("pipelineName", T.StringType(), False),
    T.StructField("targetTable", T.StringType(), False),
    T.StructField("leaseId", T.StringType(), False),
    T.StructField("ownerRunId", T.StringType(), False),
    T.StructField("acquiredAt", T.TimestampType(), False),
    T.StructField("expiresAt", T.TimestampType(), False),
    T.StructField("releasedAt", T.TimestampType(), True),
    T.StructField("status", T.StringType(), False),
])

BATCH_MANIFEST_SCHEMA = T.StructType([
    T.StructField("manifestBatchId", T.StringType(), False),
    T.StructField("orchestrationRunId", T.StringType(), True),
    T.StructField("pipelineName", T.StringType(), False),
    T.StructField("targetTable", T.StringType(), False),
    T.StructField("batchMode", T.StringType(), False),
    T.StructField("sourceSystem", T.StringType(), True),
    T.StructField("hashBucket", T.IntegerType(), True),
    T.StructField("sourceModifiedStart", T.TimestampType(), True),
    T.StructField("sourceModifiedEnd", T.TimestampType(), True),
    T.StructField("filterStudyInstanceUids", T.StringType(), True),
    T.StructField("status", T.StringType(), False),
    T.StructField("sourceRowsExpected", T.LongType(), True),
    T.StructField("sourceRowsProcessed", T.LongType(), True),
    T.StructField("startedAt", T.TimestampType(), True),
    T.StructField("completedAt", T.TimestampType(), True),
    T.StructField("errorMessage", T.StringType(), True),
    T.StructField("updatedAt", T.TimestampType(), False),
])

PARSE_QUARANTINE_SCHEMA = T.StructType([
    T.StructField("extractRunId", T.StringType(), False),
    T.StructField("batchId", T.StringType(), False),
    T.StructField("imagingMetastoreId", T.StringType(), True),
    T.StructField("studyInstanceUid", T.StringType(), True),
    T.StructField("dicomTag", T.StringType(), True),
    T.StructField("dicomKeyword", T.StringType(), True),
    T.StructField("rawValueSnippet", T.StringType(), True),
    T.StructField("errorClass", T.StringType(), True),
    T.StructField("errorMessage", T.StringType(), True),
    T.StructField("createdAt", T.TimestampType(), False),
])

JOIN_QUARANTINE_SCHEMA = T.StructType([
    T.StructField("extractRunId", T.StringType(), False),
    T.StructField("batchId", T.StringType(), False),
    T.StructField("imagingMetastoreId", T.StringType(), True),
    T.StructField("msftSourceSystem", T.StringType(), True),
    T.StructField("studyInstanceUid", T.StringType(), True),
    T.StructField("reason", T.StringType(), False),
    T.StructField("createdAt", T.TimestampType(), False),
])

VALIDATION_QUARANTINE_SCHEMA = T.StructType([
    T.StructField("extractRunId", T.StringType(), False),
    T.StructField("batchId", T.StringType(), False),
    T.StructField("imagingMetastoreId", T.StringType(), True),
    T.StructField("validationName", T.StringType(), False),
    T.StructField("expectedValue", T.StringType(), True),
    T.StructField("actualValue", T.StringType(), True),
    T.StructField("errorMessage", T.StringType(), True),
    T.StructField("createdAt", T.TimestampType(), False),
])

ANOMALY_SCHEMA = T.StructType([
    T.StructField("extractRunId", T.StringType(), False),
    T.StructField("batchId", T.StringType(), False),
    T.StructField("metricName", T.StringType(), False),
    T.StructField("metricValue", T.DoubleType(), True),
    T.StructField("baselineValue", T.DoubleType(), True),
    T.StructField("severity", T.StringType(), False),
    T.StructField("message", T.StringType(), False),
    T.StructField("isCurrent", T.BooleanType(), False),
    T.StructField("resolvedAt", T.TimestampType(), True),
    T.StructField("createdAt", T.TimestampType(), False),
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


def target_schema(tag_rows=None):
    fields = [
        T.StructField("id", T.StringType(), False),
        T.StructField("imagingMetastoreId", T.StringType(), False),
        T.StructField("sourceRecordKey", T.StringType(), False),
        T.StructField("imagingStudyId", T.StringType(), False),
        T.StructField("msftSourceSystem", T.StringType(), True),
        T.StructField("studyInstanceUid", T.StringType(), True),
        T.StructField("seriesInstanceUid", T.StringType(), True),
        T.StructField("sopInstanceUid", T.StringType(), True),
        T.StructField("filePath", T.StringType(), True),
        T.StructField("sourceModifiedAt", T.TimestampType(), True),
        T.StructField("sourceModifiedDate", T.DateType(), False),
        T.StructField("sourceSystemHashBucket", T.IntegerType(), False),
    ]
    canonical_names = (
        [row["canonicalColumnName"] for row in tag_rows]
        if tag_rows is not None
        else [canonical for _, _, canonical, *_ in SEED_TAGS]
    )
    seen_columns = {field.name for field in fields}
    for canonical in canonical_names:
        column_name = wide_column_name(canonical)
        if column_name in seen_columns:
            raise ValueError(f"Active DICOM tag configuration collides with physical target column {column_name!r}.")
        fields.append(T.StructField(column_name, T.StringType(), True))
        seen_columns.add(column_name)
    fields.extend([
        T.StructField("isActive", T.BooleanType(), False),
        T.StructField("sourceRowHash", T.StringType(), False),
        T.StructField("extractedAt", T.TimestampType(), False),
        T.StructField("extractRunId", T.StringType(), False),
    ])
    return T.StructType(fields)


def wide_column_name(canonical_column_name: str) -> str:
    return UID_TAG_COLUMN_RENAMES.get(canonical_column_name, canonical_column_name)


def _runtime_context():
    try:
        return notebookutils.runtime.context
    except Exception:
        return {}


def _context_get(context, key, default=None):
    try:
        value = context.get(key, default)
        if value is not None:
            return value
    except Exception:
        pass
    try:
        value = context[key]
        if value is not None:
            return value
    except Exception:
        pass
    return default


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


def parse_int_param(name: str, default: int) -> int:
    raw_value = get_param(name, default)
    if raw_value is None or str(raw_value).strip() == "":
        return default
    value = int(raw_value)
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer; got {raw_value!r}")
    return value


def parse_timestamp_param(name: str):
    raw_value = get_param(name, "")
    if raw_value is None or str(raw_value).strip() == "":
        return None
    text = str(raw_value).strip().replace("Z", "+00:00")
    value = datetime.fromisoformat(text)
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value

def parse_csv_param(name: str):
    raw_value = get_param(name, "")
    if raw_value is None or str(raw_value).strip() == "":
        return []
    values = [value.strip() for value in str(raw_value).split(",")]
    return [value for value in values if value]

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


def parse_optional_int_param(name: str):
    raw_value = get_param(name, "")
    if raw_value is None or str(raw_value).strip() == "":
        return None
    return int(raw_value)


def resolve_workspace_id() -> str:
    try:
        return notebookutils.fabric.resolve_workspace_id()
    except AttributeError:
        context = _runtime_context()
        workspace_id = _context_get(context, "currentWorkspaceId", None) or _context_get(context, "workspaceId", None)
        if workspace_id:
            return workspace_id
        raise ValueError("Unable to resolve current Fabric workspace ID from notebookutils.")


WORKSPACE_ID = resolve_workspace_id()


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


def abfss_files(lakehouse_id: str, relative_path: str) -> str:
    return f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files/{relative_path}"


SILVER_LH_ID = resolve_lakehouse_id(SILVER_LH_NAME)
ADMIN_LH_ID = resolve_lakehouse_id(ADMIN_LH_NAME)

print("Resolving lakehouse IDs...")
print(f"  Workspace: {WORKSPACE_ID}")
print(f"  Silver LH: {SILVER_LH_ID} ({SILVER_LH_NAME})")
print(f"  Admin LH:  {ADMIN_LH_ID} ({ADMIN_LH_NAME})")

MAX_SOURCE_ROWS_PER_BATCH = parse_int_param("MAX_SOURCE_ROWS_PER_BATCH", MAX_SOURCE_ROWS_PER_BATCH_DEFAULT)
WATERMARK_OVERLAP_HOURS = parse_int_param("WATERMARK_OVERLAP_HOURS", WATERMARK_OVERLAP_HOURS_DEFAULT)
HASH_BUCKET_COUNT = parse_int_param("HASH_BUCKET_COUNT", HASH_BUCKET_COUNT_DEFAULT)
MAX_ENABLED_TAGS = parse_int_param("MAX_ENABLED_TAGS", MAX_ENABLED_TAGS_DEFAULT)
BATCH_START_SOURCE_MODIFIED_AT = parse_timestamp_param("BATCH_START_SOURCE_MODIFIED_AT")
BATCH_END_SOURCE_MODIFIED_AT = parse_timestamp_param("BATCH_END_SOURCE_MODIFIED_AT")
FILTER_STUDY_INSTANCE_UIDS = parse_csv_param("FILTER_STUDY_INSTANCE_UIDS")
SAMPLE_STUDY_COUNT = parse_optional_int_param("SAMPLE_STUDY_COUNT")
DRY_RUN_ONLY = parse_bool_param("DRY_RUN_ONLY", False)
FILTER_SOURCE_SYSTEM = str(get_param("FILTER_SOURCE_SYSTEM", "")).strip()
FILTER_SOURCE_SYSTEM_HASH_BUCKET = parse_optional_int_param("FILTER_SOURCE_SYSTEM_HASH_BUCKET")
LEASE_TIMEOUT_MINUTES = parse_int_param("LEASE_TIMEOUT_MINUTES", 240)
MANIFEST_BATCH_ID_PARAM = str(get_param("MANIFEST_BATCH_ID", "")).strip()
ORCHESTRATION_RUN_ID = str(get_param("ORCHESTRATION_RUN_ID", "")).strip() or None
CLEAR_TARGET_BEFORE_RUN = str(get_param("CLEAR_TARGET_BEFORE_RUN", "")).strip()
VALIDATION_TAG = str(get_param("VALIDATION_TAG", "")).strip().upper()
VALIDATION_ORCHESTRATION_PREFIX = "TAG_MANAGER_VALIDATE_"
if not VALIDATION_TAG and ORCHESTRATION_RUN_ID and ORCHESTRATION_RUN_ID.startswith(VALIDATION_ORCHESTRATION_PREFIX):
    VALIDATION_TAG = ORCHESTRATION_RUN_ID[len(VALIDATION_ORCHESTRATION_PREFIX):].strip().upper()

if (BATCH_START_SOURCE_MODIFIED_AT is None) != (BATCH_END_SOURCE_MODIFIED_AT is None):
    raise ValueError("BATCH_START_SOURCE_MODIFIED_AT and BATCH_END_SOURCE_MODIFIED_AT must be supplied together.")
if BATCH_START_SOURCE_MODIFIED_AT and BATCH_END_SOURCE_MODIFIED_AT <= BATCH_START_SOURCE_MODIFIED_AT:
    raise ValueError("BATCH_END_SOURCE_MODIFIED_AT must be greater than BATCH_START_SOURCE_MODIFIED_AT.")
if SAMPLE_STUDY_COUNT is not None and SAMPLE_STUDY_COUNT <= 0:
    raise ValueError("SAMPLE_STUDY_COUNT must be a positive integer when supplied.")
if VALIDATION_TAG and not re.fullmatch(r"[0-9A-F]{8}", VALIDATION_TAG):
    raise ValueError("VALIDATION_TAG must be empty or eight uppercase hexadecimal characters.")

EXPLICIT_BATCH_MODE = BATCH_START_SOURCE_MODIFIED_AT is not None
EXTRACT_RUN_ID = str(uuid4())
BATCH_ID = f"{EXTRACT_RUN_ID}-001"
MANIFEST_BATCH_ID = MANIFEST_BATCH_ID_PARAM or BATCH_ID
LEASE_ID = str(uuid4())
CONTROL_SOURCE_SYSTEM = FILTER_SOURCE_SYSTEM or "ALL"
CONTROL_HASH_BUCKET = FILTER_SOURCE_SYSTEM_HASH_BUCKET if FILTER_SOURCE_SYSTEM_HASH_BUCKET is not None else -1
STARTED_AT = datetime.now(timezone.utc).replace(tzinfo=None)
NOTEBOOK_RUN_ID = str(_context_get(_runtime_context(), "currentRunId", "")) or None
RUN_STAGING_ROOT = f"_staging/{PIPELINE_NAME}/{EXTRACT_RUN_ID}"
STAGING_ROOT_PATH = abfss_files(ADMIN_LH_ID, RUN_STAGING_ROOT)
SOURCE_STAGING_PATH = abfss_files(ADMIN_LH_ID, f"{RUN_STAGING_ROOT}/source")
WIDE_STAGING_PATH = abfss_files(ADMIN_LH_ID, f"{RUN_STAGING_ROOT}/wide")

TAG_DICTIONARY_PATH = abfss_tables(ADMIN_LH_ID, TAG_DICTIONARY_TABLE)
CONTROL_PATH = abfss_tables(ADMIN_LH_ID, CONTROL_TABLE)
RUN_PATH = abfss_tables(ADMIN_LH_ID, RUN_TABLE)
METRICS_PATH = abfss_tables(ADMIN_LH_ID, METRICS_TABLE)
HEALTH_PATH = abfss_tables(ADMIN_LH_ID, HEALTH_TABLE)
SLA_CONFIG_PATH = abfss_tables(ADMIN_LH_ID, SLA_CONFIG_TABLE)
TIME_ZONE_OPTION_PATH = abfss_tables(ADMIN_LH_ID, TIME_ZONE_OPTION_TABLE)
LEASE_PATH = abfss_tables(ADMIN_LH_ID, LEASE_TABLE)
LEASE_LOCK_PATH = abfss_tables(ADMIN_LH_ID, LEASE_LOCK_TABLE)
FULL_BACKFILL_LOCK_PATH = abfss_tables(ADMIN_LH_ID, FULL_BACKFILL_LOCK_TABLE)
BATCH_MANIFEST_PATH = abfss_tables(ADMIN_LH_ID, BATCH_MANIFEST_TABLE)
PARSE_QUARANTINE_PATH = abfss_tables(ADMIN_LH_ID, PARSE_QUARANTINE_TABLE)
JOIN_QUARANTINE_PATH = abfss_tables(ADMIN_LH_ID, JOIN_QUARANTINE_TABLE)
VALIDATION_QUARANTINE_PATH = abfss_tables(ADMIN_LH_ID, VALIDATION_QUARANTINE_TABLE)
ANOMALY_PATH = abfss_tables(ADMIN_LH_ID, ANOMALY_TABLE)
GOVERNANCE_PATH = abfss_tables(ADMIN_LH_ID, GOVERNANCE_TABLE)
CANARY_STUDY_PATH = abfss_tables(ADMIN_LH_ID, CANARY_STUDY_TABLE)
PREFLIGHT_PATH = abfss_tables(ADMIN_LH_ID, PREFLIGHT_TABLE)
ALERT_PATH = abfss_tables(ADMIN_LH_ID, ALERT_TABLE)
ORCHESTRATION_RUN_PATH = abfss_tables(ADMIN_LH_ID, ORCHESTRATION_RUN_TABLE)
TARGET_PATH = abfss_tables(SILVER_LH_ID, TARGET_TABLE)

run_values = {}


def _schema_type_sql(data_type):
    if isinstance(data_type, T.StringType):
        return "STRING"
    if isinstance(data_type, T.BooleanType):
        return "BOOLEAN"
    if isinstance(data_type, T.TimestampType):
        return "TIMESTAMP"
    if isinstance(data_type, T.LongType):
        return "BIGINT"
    if isinstance(data_type, T.DoubleType):
        return "DOUBLE"
    if isinstance(data_type, T.IntegerType):
        return "INT"
    if isinstance(data_type, T.DateType):
        return "DATE"
    raise TypeError(f"Unsupported ALTER TABLE type for {data_type.simpleString()}")


def ensure_delta_table(path: str, schema: T.StructType, partition_cols=None):
    if DeltaTable.isDeltaTable(spark, path):
        existing_cols = set(spark.read.format("delta").load(path).columns)
        missing_fields = [field for field in schema.fields if field.name not in existing_cols]
        if missing_fields:
            add_clause = ", ".join(f"{field.name} {_schema_type_sql(field.dataType)}" for field in missing_fields)
            spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMNS ({add_clause})")
        return
    writer = spark.createDataFrame([], schema).write.format("delta").mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)


def ensure_wide_target_table(path: str, schema: T.StructType):
    if not DeltaTable.isDeltaTable(spark, path):
        spark.createDataFrame([], schema).write.format("delta").mode("overwrite").partitionBy("sourceModifiedDate", "sourceSystemHashBucket").save(path)
        return
    existing_cols = set(spark.read.format("delta").load(path).columns)
    expected_cols = {field.name for field in schema.fields}
    long_form_markers = {"dicomTag", "dicomKeyword", "canonicalColumnName", "tagJson", "valueJson", "ordinal"}
    if long_form_markers & existing_cols:
        print(f"Replacing existing long-form {TARGET_TABLE} Delta table with wide-table schema at {path}.")
        spark.createDataFrame([], schema).write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("sourceModifiedDate", "sourceSystemHashBucket").save(path)
        return
    missing_fields = [field for field in schema.fields if field.name not in existing_cols]
    if missing_fields:
        add_clause = ", ".join(f"{field.name} {_schema_type_sql(field.dataType)}" for field in missing_fields)
        spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMNS ({add_clause})")
    unexpected_missing = expected_cols - set(spark.read.format("delta").load(path).columns)
    if unexpected_missing:
        raise ValueError(f"Unable to create expected wide target columns: {sorted(unexpected_missing)}")


def require_columns(df, table_name: str, columns):
    missing = [col for col in columns if col not in df.columns]
    if missing:
        raise ValueError(f"{table_name} is missing required columns: {', '.join(missing)}")


def current_utc_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def update_run_row(**updates):
    global run_values
    run_values.update(updates)
    row = {field.name: run_values.get(field.name) for field in RUN_SCHEMA.fields}
    run_df = spark.createDataFrame([row], RUN_SCHEMA)
    DeltaTable.forPath(spark, RUN_PATH).alias("t").merge(
        run_df.alias("s"),
        "t.extractRunId = s.extractRunId AND t.batchId = s.batchId",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


def write_metrics_rows(rows):
    if not rows:
        return
    metrics_df = spark.createDataFrame(rows, METRICS_SCHEMA)
    metrics_df.write.format("delta").mode("append").save(METRICS_PATH)


def append_metric(rows, scope, name, value, dicom_tag=None, dicom_keyword=None, metric_json=None):
    rows.append({
        "extractRunId": EXTRACT_RUN_ID,
        "batchId": BATCH_ID,
        "metricScope": scope,
        "metricName": name,
        "dicomTag": dicom_tag,
        "dicomKeyword": dicom_keyword,
        "metricValue": None if value is None else float(value),
        "metricJson": metric_json,
        "createdAt": current_utc_naive(),
    })


def acquire_run_lease():
    if DeltaTable.isDeltaTable(spark, FULL_BACKFILL_LOCK_PATH):
        backfill_locks = (
            spark.read.format("delta").load(FULL_BACKFILL_LOCK_PATH)
            .where(
                (F.col("pipelineName") == F.lit(PIPELINE_NAME))
                & (F.col("targetTable") == F.lit(TARGET_TABLE))
                & (F.col("status") == F.lit("Active"))
                & (F.col("expiresAt") > F.lit(current_utc_naive()).cast("timestamp"))
            )
            .limit(1)
            .collect()
        )
        if backfill_locks:
            owner = backfill_locks[0].asDict()
            if not ORCHESTRATION_RUN_ID or owner["ownerRunId"] != ORCHESTRATION_RUN_ID:
                raise RuntimeError(f"Active full-backfill lock blocks extraction: {owner}")
    now = current_utc_naive()
    expires_at = now + timedelta(minutes=LEASE_TIMEOUT_MINUTES)
    row = {
        "pipelineName": PIPELINE_NAME,
        "targetTable": TARGET_TABLE,
        "leaseId": LEASE_ID,
        "ownerRunId": EXTRACT_RUN_ID,
        "acquiredAt": now,
        "expiresAt": expires_at,
        "releasedAt": None,
        "status": "Active",
    }
    lease_df = spark.createDataFrame([row], LEASE_SCHEMA)
    lock = DeltaTable.forPath(spark, LEASE_LOCK_PATH)
    try:
        (
            lock.alias("t")
            .merge(
                lease_df.alias("s"),
                "t.pipelineName = s.pipelineName AND t.targetTable = s.targetTable",
            )
            .whenMatchedUpdate(
                condition="t.status <> 'Active' OR t.expiresAt <= s.acquiredAt",
                set={field.name: f"s.{field.name}" for field in LEASE_SCHEMA.fields},
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    except Exception as lease_merge_exc:
        print(f"Atomic lease merge encountered a concurrent update: {lease_merge_exc}")

    current_lock = (
        spark.read.format("delta").load(LEASE_LOCK_PATH)
        .where(
            (F.col("pipelineName") == F.lit(PIPELINE_NAME))
            & (F.col("targetTable") == F.lit(TARGET_TABLE))
        )
        .limit(1)
        .collect()
    )
    if not current_lock or current_lock[0]["leaseId"] != LEASE_ID or current_lock[0]["status"] != "Active":
        owner = current_lock[0].asDict() if current_lock else None
        raise RuntimeError(f"Unable to acquire exclusive extraction lease for {TARGET_TABLE}; current owner: {owner}")
    try:
        lease_df.write.format("delta").mode("append").save(LEASE_PATH)
    except Exception:
        lock.update(
            condition=f"leaseId = '{LEASE_ID}'",
            set={"status": F.lit("Failed"), "releasedAt": F.lit(current_utc_naive()).cast("timestamp")},
        )
        raise
    print(f"Acquired exclusive extraction lease {LEASE_ID} until {expires_at} UTC.")


def release_run_lease(status="Released"):
    released_at = current_utc_naive()
    errors = []
    for path in (LEASE_LOCK_PATH, LEASE_PATH):
        try:
            DeltaTable.forPath(spark, path).update(
                condition=f"leaseId = '{LEASE_ID}'",
                set={"status": F.lit(status), "releasedAt": F.lit(released_at).cast("timestamp")},
            )
        except Exception as lease_exc:
            errors.append(f"{path}: {lease_exc}")
    if errors:
        print("Failed to release one or more lease records: " + " | ".join(errors))
    else:
        print(f"Released extraction lease {LEASE_ID} with status {status}.")


def batch_mode_name():
    if FILTER_STUDY_INSTANCE_UIDS:
        return "studyFilter"
    if SAMPLE_STUDY_COUNT is not None:
        return "studySample"
    if EXPLICIT_BATCH_MODE:
        return "sourceModifiedWindow"
    if last_high_watermark is not None:
        return "incremental"
    return "initialLoad"


def upsert_manifest(status, source_rows_expected=None, source_rows_processed=None, error_message=None, completed=False):
    now = current_utc_naive()
    row = {
        "manifestBatchId": MANIFEST_BATCH_ID,
        "orchestrationRunId": ORCHESTRATION_RUN_ID,
        "pipelineName": PIPELINE_NAME,
        "targetTable": TARGET_TABLE,
        "batchMode": batch_mode_name(),
        "sourceSystem": FILTER_SOURCE_SYSTEM or "ALL",
        "hashBucket": FILTER_SOURCE_SYSTEM_HASH_BUCKET,
        "sourceModifiedStart": BATCH_START_SOURCE_MODIFIED_AT,
        "sourceModifiedEnd": BATCH_END_SOURCE_MODIFIED_AT,
        "filterStudyInstanceUids": ",".join(FILTER_STUDY_INSTANCE_UIDS) if FILTER_STUDY_INSTANCE_UIDS else None,
        "status": status,
        "sourceRowsExpected": source_rows_expected,
        "sourceRowsProcessed": source_rows_processed,
        "startedAt": STARTED_AT,
        "completedAt": now if completed else None,
        "errorMessage": None if error_message is None else str(error_message)[:3500],
        "updatedAt": now,
    }
    df = spark.createDataFrame([row], BATCH_MANIFEST_SCHEMA)
    DeltaTable.forPath(spark, BATCH_MANIFEST_PATH).alias("t").merge(
        df.alias("s"),
        "t.manifestBatchId = s.manifestBatchId",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


def write_join_quarantine(joined_source):
    missing = joined_source.where(F.col("imagingStudyId").isNull())
    rows = missing.select(
        F.lit(EXTRACT_RUN_ID).alias("extractRunId"),
        F.lit(BATCH_ID).alias("batchId"),
        F.col("id").cast("string").alias("imagingMetastoreId"),
        F.col("msftSourceSystem").cast("string").alias("msftSourceSystem"),
        F.col("studyInstanceUid").cast("string").alias("studyInstanceUid"),
        F.lit("MissingImagingStudy").alias("reason"),
        F.lit(current_utc_naive()).cast("timestamp").alias("createdAt"),
    )
    if rows.take(1):
        rows.write.format("delta").mode("append").save(JOIN_QUARANTINE_PATH)


def write_parse_quarantine(staged_wide, enabled_tags):
    value_cols = [wide_column_name(row["canonicalColumnName"]) for row in enabled_tags]
    failed = staged_wide.where(F.coalesce(*[F.col(col) for col in value_cols]).isNull())
    rows = failed.select(
        F.lit(EXTRACT_RUN_ID).alias("extractRunId"),
        F.lit(BATCH_ID).alias("batchId"),
        F.col("imagingMetastoreId").cast("string").alias("imagingMetastoreId"),
        F.col("studyInstanceUid").cast("string").alias("studyInstanceUid"),
        F.lit(None).cast("string").alias("dicomTag"),
        F.lit(None).cast("string").alias("dicomKeyword"),
        F.lit(None).cast("string").alias("rawValueSnippet"),
        F.lit("NoEnabledTagValuesParsed").alias("errorClass"),
        F.lit("No enabled DICOM tag values parsed for this source row.").alias("errorMessage"),
        F.lit(current_utc_naive()).cast("timestamp").alias("createdAt"),
    )
    if rows.take(1):
        rows.write.format("delta").mode("append").save(PARSE_QUARANTINE_PATH)


def write_validation_quarantine(staged, validation_name, expected_col, actual_col):
    rows = staged.where(
        F.col(expected_col).isNotNull() & F.col(actual_col).isNotNull() & (F.col(expected_col) != F.col(actual_col))
    ).select(
        F.lit(EXTRACT_RUN_ID).alias("extractRunId"),
        F.lit(BATCH_ID).alias("batchId"),
        F.col("imagingMetastoreId").cast("string").alias("imagingMetastoreId"),
        F.lit(validation_name).alias("validationName"),
        F.col(expected_col).cast("string").alias("expectedValue"),
        F.col(actual_col).cast("string").alias("actualValue"),
        F.lit(f"{expected_col} does not match {actual_col}").alias("errorMessage"),
        F.lit(current_utc_naive()).cast("timestamp").alias("createdAt"),
    )
    if rows.take(1):
        rows.write.format("delta").mode("append").save(VALIDATION_QUARANTINE_PATH)


def seed_governance_metadata():
    now = current_utc_naive()
    rows = []
    for tag, keyword, canonical, _vr, _scope, _well_known, is_phi, phi_category, _enabled in SEED_TAGS:
        rows.append({
            "targetTable": TARGET_TABLE,
            "columnName": wide_column_name(canonical),
            "dicomTag": tag,
            "dicomKeyword": keyword,
            "isPhi": bool(is_phi),
            "phiCategory": phi_category,
            "recommendedSensitivityLabel": "Confidential - PHI" if is_phi else "General",
            "createdAt": now,
        })
    df = spark.createDataFrame(rows, GOVERNANCE_SCHEMA)
    DeltaTable.forPath(spark, GOVERNANCE_PATH).alias("t").merge(
        df.alias("s"),
        "t.targetTable = s.targetTable AND t.columnName = s.columnName",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


def write_anomaly_checks(source_rows_scanned, source_rows_active, metadata_rows_parse_failed, source_rows_missing_imaging_study, extension_rows_staged):
    now = current_utc_naive()
    rows = []
    def add(name, value, baseline, severity, message):
        rows.append({
            "extractRunId": EXTRACT_RUN_ID,
            "batchId": BATCH_ID,
            "metricName": name,
            "metricValue": None if value is None else float(value),
            "baselineValue": None if baseline is None else float(baseline),
            "severity": severity,
            "message": message,
            "isCurrent": True,
            "resolvedAt": None,
            "createdAt": now,
        })
    if source_rows_scanned and source_rows_active == 0:
        add("sourceRowsActive", source_rows_active, None, "Amber", "Source rows scanned but all were removed by deterministic dedupe.")
    if source_rows_active and extension_rows_staged == 0:
        add("extensionRowsStaged", extension_rows_staged, source_rows_active, "Red", "Active source rows produced zero extension rows.")
    if metadata_rows_parse_failed:
        add("metadataRowsParseFailed", metadata_rows_parse_failed, 0, "Red", "One or more active source rows failed metadata parsing.")
    if source_rows_missing_imaging_study:
        add("sourceRowsMissingImagingStudy", source_rows_missing_imaging_study, 0, "Red", "One or more active source rows did not join to ImagingStudy.")
    is_baseline_eligible = (
        last_high_watermark is not None
        and not FILTER_STUDY_INSTANCE_UIDS
        and SAMPLE_STUDY_COUNT is None
        and not EXPLICIT_BATCH_MODE
        and not DRY_RUN_ONLY
        and not ORCHESTRATION_RUN_ID
    )
    history = spark.read.format("delta").load(RUN_PATH).where(
        (F.col("status").isin("Succeeded", "SucceededWithCleanupWarning"))
        & F.col("sourceRowsActive").isNotNull()
        & (F.col("extractRunId") != F.lit(EXTRACT_RUN_ID))
        & (F.coalesce(F.col("dryRunOnly"), F.lit(False)) == F.lit(False))
        & F.col("batchStartSourceModifiedAt").isNull()
        & F.col("batchEndSourceModifiedAt").isNull()
        & F.col("orchestrationRunId").isNull()
    ).orderBy(F.col("startedAt").desc()).limit(20)
    if is_baseline_eligible and history.take(1):
        baseline = history.agg(F.avg("sourceRowsActive").alias("avgSourceRowsActive")).collect()[0]["avgSourceRowsActive"]
        if baseline and source_rows_active is not None and (source_rows_active > baseline * 10 or source_rows_active < baseline / 10):
            add("sourceRowsActive", source_rows_active, baseline, "Amber", "Active source row count differs by more than 10x from recent unfiltered production incremental runs.")
    if rows:
        try:
            DeltaTable.forPath(spark, ANOMALY_PATH).update(
                condition="isCurrent = true",
                set={"isCurrent": F.lit(False), "resolvedAt": F.lit(now).cast("timestamp")},
            )
        except Exception as anomaly_state_exc:
            print(f"Could not resolve prior anomaly rows: {anomaly_state_exc}")
        spark.createDataFrame(rows, ANOMALY_SCHEMA).write.format("delta").mode("append").save(ANOMALY_PATH)


def default_sla_config_row(updated_by="seeded-by-extract-notebook"):
    return {
        "pipelineName": PIPELINE_NAME,
        "targetTable": TARGET_TABLE,
        "staleWarningHours": STALE_WARNING_HOURS_DEFAULT,
        "staleErrorHours": STALE_ERROR_HOURS_DEFAULT,
        "maxParseFailureRows": 0,
        "maxMissingImagingStudyRows": 0,
        "maxUidMismatchRows": 0,
        "maxDeduplicationRate": None,
        "displayTimeZoneLabel": DISPLAY_TIME_ZONE_LABEL_DEFAULT,
        "displayUtcOffsetHours": DISPLAY_UTC_OFFSET_HOURS_DEFAULT,
        "updatedAt": current_utc_naive(),
        "updatedBy": updated_by,
    }


def seed_and_read_sla_config():
    ensure_delta_table(SLA_CONFIG_PATH, SLA_CONFIG_SCHEMA)
    config = spark.read.format("delta").load(SLA_CONFIG_PATH)
    rows = (
        config
        .where((F.col("pipelineName") == F.lit(PIPELINE_NAME)) & (F.col("targetTable") == F.lit(TARGET_TABLE)))
        .orderBy(F.col("updatedAt").desc())
        .limit(1)
        .collect()
    )
    if not rows:
        default_row = default_sla_config_row()
        spark.createDataFrame([default_row], SLA_CONFIG_SCHEMA).write.format("delta").mode("append").save(SLA_CONFIG_PATH)
        return default_row
    row = rows[0].asDict()
    changed = False
    default_row = default_sla_config_row("backfilled-by-extract-notebook")
    for key in ("staleWarningHours", "staleErrorHours", "maxParseFailureRows", "maxMissingImagingStudyRows", "maxUidMismatchRows", "displayTimeZoneLabel", "displayUtcOffsetHours"):
        if row.get(key) is None:
            row[key] = default_row[key]
            changed = True
    if changed:
        row["updatedAt"] = current_utc_naive()
        row["updatedBy"] = "backfilled-by-extract-notebook"
        spark.createDataFrame([row], SLA_CONFIG_SCHEMA).write.format("delta").mode("append").save(SLA_CONFIG_PATH)
    return row


def seed_time_zone_options():
    ensure_delta_table(TIME_ZONE_OPTION_PATH, TIME_ZONE_OPTION_SCHEMA)
    existing = spark.read.format("delta").load(TIME_ZONE_OPTION_PATH)
    existing_labels = {row["displayTimeZoneLabel"] for row in existing.select("displayTimeZoneLabel").distinct().collect()}
    now = current_utc_naive()
    seed_rows = [
        {
            "displayTimeZoneLabel": "UTC",
            "displayUtcOffsetHours": 0.0,
            "sortOrder": 1,
            "isDefault": False,
            "updatedAt": now,
            "updatedBy": "seeded-by-extract-notebook",
        },
        {
            "displayTimeZoneLabel": DISPLAY_TIME_ZONE_LABEL_DEFAULT,
            "displayUtcOffsetHours": DISPLAY_UTC_OFFSET_HOURS_DEFAULT,
            "sortOrder": 2,
            "isDefault": True,
            "updatedAt": now,
            "updatedBy": "seeded-by-extract-notebook",
        },
    ]
    missing_rows = [row for row in seed_rows if row["displayTimeZoneLabel"] not in existing_labels]
    if missing_rows:
        spark.createDataFrame(missing_rows, TIME_ZONE_OPTION_SCHEMA).write.format("delta").mode("append").save(TIME_ZONE_OPTION_PATH)
        print(f"Seeded {len(missing_rows)} time zone option rows.")


def apply_display_utc_offset(value, offset_hours):
    if value is None:
        return None
    return value + timedelta(hours=float(offset_hours))


def target_health_counts():
    if not DeltaTable.isDeltaTable(spark, TARGET_PATH):
        return {
            "totalExtensionRows": 0,
            "distinctStudyInstanceUids": 0,
            "distinctSeriesInstanceUids": 0,
            "distinctSopInstanceUids": 0,
        }
    target = spark.read.format("delta").load(TARGET_PATH).where(F.col("isActive") == F.lit(True))
    row = target.agg(
        F.count(F.lit(1)).cast("long").alias("totalExtensionRows"),
        F.countDistinct("studyInstanceUid").cast("long").alias("distinctStudyInstanceUids"),
        F.countDistinct("seriesInstanceUid").cast("long").alias("distinctSeriesInstanceUids"),
        F.countDistinct("sopInstanceUid").cast("long").alias("distinctSopInstanceUids"),
    ).collect()[0]
    return row.asDict()


def determine_health_status(status, hours_since_last_success, source_rows_missing, parse_failed, study_mismatch, series_mismatch, sop_mismatch, source_rows_deduplicated, source_rows_scanned, sla_config):
    if status not in ("Succeeded", "SucceededWithCleanupWarning"):
        return "Red", 3, "Last extraction run did not succeed."
    if hours_since_last_success is not None and hours_since_last_success >= sla_config["staleErrorHours"]:
        return "Red", 3, f"Last successful run is stale by {hours_since_last_success:.1f} hours."
    mismatch_total = (study_mismatch or 0) + (series_mismatch or 0) + (sop_mismatch or 0)
    if parse_failed is not None and parse_failed > sla_config["maxParseFailureRows"]:
        return "Red", 3, f"{parse_failed} metadata parse failures exceeded SLA threshold {sla_config['maxParseFailureRows']}."
    if source_rows_missing is not None and source_rows_missing > sla_config["maxMissingImagingStudyRows"]:
        return "Red", 3, f"{source_rows_missing} rows missing ImagingStudy exceeded SLA threshold {sla_config['maxMissingImagingStudyRows']}."
    if mismatch_total > sla_config["maxUidMismatchRows"]:
        return "Red", 3, f"{mismatch_total} UID mismatches exceeded SLA threshold {sla_config['maxUidMismatchRows']}."
    warning_reasons = []
    if status == "SucceededWithCleanupWarning":
        warning_reasons.append("staging cleanup warning")
    if hours_since_last_success is not None and hours_since_last_success >= sla_config["staleWarningHours"]:
        warning_reasons.append(f"last success is {hours_since_last_success:.1f} hours old")
    max_dedupe_rate = sla_config.get("maxDeduplicationRate")
    if max_dedupe_rate is not None and source_rows_scanned:
        dedupe_rate = (source_rows_deduplicated or 0) / source_rows_scanned
        if dedupe_rate > max_dedupe_rate:
            warning_reasons.append(f"dedupe rate {dedupe_rate:.1%} exceeded threshold {max_dedupe_rate:.1%}")
    if warning_reasons:
        return "Amber", 2, "; ".join(warning_reasons)
    return "Green", 1, "Last extraction run succeeded within SLA."



def write_health_snapshot(status, source_rows_deduplicated=None, study_uid_mismatches=None, series_uid_mismatches=None, sop_uid_mismatches=None):
    ensure_delta_table(HEALTH_PATH, HEALTH_SCHEMA)
    sla_config = seed_and_read_sla_config()
    now = current_utc_naive()
    completed_at = run_values.get("completedAt") or now
    hours_since_last_success = None
    if status in ("Succeeded", "SucceededWithCleanupWarning"):
        hours_since_last_success = 0.0
    elif last_high_watermark is not None:
        try:
            hours_since_last_success = (now - last_high_watermark).total_seconds() / 3600.0
        except Exception:
            hours_since_last_success = None
    health_status, health_rank, status_message = determine_health_status(
        status,
        hours_since_last_success,
        run_values.get("sourceRowsMissingImagingStudy"),
        run_values.get("metadataRowsParseFailed"),
        study_uid_mismatches,
        series_uid_mismatches,
        sop_uid_mismatches,
        source_rows_deduplicated,
        run_values.get("sourceRowsScanned"),
        sla_config,
    )
    counts = target_health_counts()
    display_offset = sla_config["displayUtcOffsetHours"]
    display_label = sla_config["displayTimeZoneLabel"]
    started_at = run_values.get("startedAt")
    now_display = apply_display_utc_offset(now, display_offset)
    started_at_display = apply_display_utc_offset(started_at, display_offset)
    completed_at_display = apply_display_utc_offset(completed_at, display_offset)
    row = {
        "snapshotId": str(uuid4()),
        "snapshotAt": now,
        "snapshotAtDisplay": now_display,
        "displayTimeZoneLabel": display_label,
        "displayUtcOffsetHours": display_offset,
        "extractRunId": EXTRACT_RUN_ID,
        "batchId": BATCH_ID,
        "pipelineName": PIPELINE_NAME,
        "targetTable": TARGET_TABLE,
        "status": status,
        "healthStatus": health_status,
        "healthStatusRank": health_rank,
        "statusMessage": status_message,
        "startedAt": started_at,
        "completedAt": completed_at,
        "startedAtDisplay": started_at_display,
        "completedAtDisplay": completed_at_display,
        "durationSeconds": run_values.get("durationSeconds"),
        "totalExtensionRows": counts.get("totalExtensionRows"),
        "distinctStudyInstanceUids": counts.get("distinctStudyInstanceUids"),
        "distinctSeriesInstanceUids": counts.get("distinctSeriesInstanceUids"),
        "distinctSopInstanceUids": counts.get("distinctSopInstanceUids"),
        "sourceRowsScanned": run_values.get("sourceRowsScanned"),
        "sourceRowsActive": run_values.get("sourceRowsActive"),
        "sourceRowsDeduplicated": source_rows_deduplicated,
        "sourceRowsJoinedToImagingStudy": run_values.get("sourceRowsJoinedToImagingStudy"),
        "sourceRowsMissingImagingStudy": run_values.get("sourceRowsMissingImagingStudy"),
        "metadataRowsParsed": run_values.get("metadataRowsParsed"),
        "metadataRowsParseFailed": run_values.get("metadataRowsParseFailed"),
        "extensionRowsStaged": run_values.get("extensionRowsStaged"),
        "extensionRowsInserted": run_values.get("extensionRowsInserted"),
        "extensionRowsUpdated": run_values.get("extensionRowsUpdated"),
        "studyUidMismatchRows": study_uid_mismatches,
        "seriesUidMismatchRows": series_uid_mismatches,
        "sopUidMismatchRows": sop_uid_mismatches,
        "hoursSinceLastSuccess": hours_since_last_success,
        "staleWarningHours": sla_config["staleWarningHours"],
        "staleErrorHours": sla_config["staleErrorHours"],
        "maxParseFailureRows": sla_config["maxParseFailureRows"],
        "maxMissingImagingStudyRows": sla_config["maxMissingImagingStudyRows"],
        "maxUidMismatchRows": sla_config["maxUidMismatchRows"],
        "maxDeduplicationRate": sla_config.get("maxDeduplicationRate"),
        "createdAt": now,
        "createdAtDisplay": now_display,
    }
    spark.createDataFrame([row], HEALTH_SCHEMA).write.format("delta").mode("append").save(HEALTH_PATH)


def parse_configured_paths(row):
    raw_paths = row["jsonPaths"]
    if not raw_paths:
        raise ValueError(f"DICOM tag {row['dicomTag']} is missing jsonPaths configuration.")
    try:
        paths = json.loads(raw_paths)
    except json.JSONDecodeError as exc:
        raise ValueError(f"DICOM tag {row['dicomTag']} has invalid jsonPaths JSON: {exc}") from exc
    if not isinstance(paths, list) or not paths:
        raise ValueError(f"DICOM tag {row['dicomTag']} jsonPaths must be a non-empty JSON array.")
    normalized = []
    for path in paths:
        if not isinstance(path, str) or not TAG_PATH_PATTERN.fullmatch(path):
            raise ValueError(f"DICOM tag {row['dicomTag']} has unsupported configured path: {path!r}")
        path_tags = re.findall(r"([0-9A-F]{8})\.Value\[\*\]", path)
        if not path_tags or path_tags[-1] != row["dicomTag"]:
            raise ValueError(f"Configured path {path!r} must terminate at DICOM tag {row['dicomTag']}.")
        normalized.append(path)
    return normalized


def validate_configured_tag(row):
    tag = row["dicomTag"]
    canonical = row["canonicalColumnName"]
    if not re.fullmatch(r"[0-9A-F]{8}", tag or ""):
        raise ValueError(f"Invalid configured dicomTag: {tag!r}")
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", canonical or ""):
        raise ValueError(f"DICOM tag {tag} has invalid canonicalColumnName: {canonical!r}")
    if row["targetDataType"] not in (None, "string"):
        raise ValueError(f"DICOM tag {tag} targetDataType must be 'string'.")
    if row["valueMode"] not in SUPPORTED_VALUE_MODES:
        raise ValueError(f"DICOM tag {tag} has unsupported valueMode: {row['valueMode']!r}")
    if row["pathPrecedence"] not in (None, "LISTED_ORDER"):
        raise ValueError(f"DICOM tag {tag} pathPrecedence must be LISTED_ORDER.")
    parse_configured_paths(row)


def seed_tag_dictionary():
    ensure_delta_table(TAG_DICTIONARY_PATH, TAG_DICTIONARY_SCHEMA)
    now = current_utc_naive()
    seed_rows = []
    for seed in SEED_TAGS:
        config = default_tag_configuration(*seed)
        seed_rows.append({
            **config,
            "validatedAt": now,
            "createdAt": now,
            "updatedAt": now,
        })
    seed_df = spark.createDataFrame(seed_rows, TAG_DICTIONARY_SCHEMA)
    target = DeltaTable.forPath(spark, TAG_DICTIONARY_PATH)
    fill_if_missing = {
        field: f"coalesce(t.{field}, s.{field})"
        for field in (
            "jsonPaths",
            "valueMode",
            "pathPrecedence",
            "targetDataType",
            "status",
            "approvalReference",
            "configurationHash",
            "validatedAt",
        )
    }
    (
        target.alias("t")
        .merge(seed_df.alias("s"), "t.dicomTag = s.dicomTag")
        .whenMatchedUpdate(set=fill_if_missing)
        .whenNotMatchedInsertAll()
        .execute()
    )

    dictionary = spark.read.format("delta").load(TAG_DICTIONARY_PATH)
    enabled_condition = (
        (F.col("enabled") == F.lit(True))
        & (F.coalesce(F.col("status"), F.lit("Enabled")) == F.lit("Enabled"))
    )
    if VALIDATION_TAG:
        validation_condition = (
            (F.col("dicomTag") == F.lit(VALIDATION_TAG))
            & (F.col("status") == F.lit("Validating"))
        )
        active_condition = enabled_condition | validation_condition
    else:
        active_condition = enabled_condition

    active_dictionary = dictionary.where(active_condition)
    duplicate_rows = active_dictionary.groupBy("dicomTag").count().where(F.col("count") > 1).collect()
    if duplicate_rows:
        duplicate_tags = ", ".join(row["dicomTag"] for row in duplicate_rows[:20])
        raise ValueError(f"DicomTagDictionary contains duplicate active dicomTag values: {duplicate_tags}")
    active_tags = active_dictionary.orderBy("dicomTag").collect()
    if len(active_tags) > MAX_ENABLED_TAGS:
        raise ValueError(f"Active tag count {len(active_tags)} exceeds MAX_ENABLED_TAGS={MAX_ENABLED_TAGS}.")
    if len(active_tags) == 0:
        raise ValueError("DicomTagDictionary has no active tags.")
    for row in active_tags:
        validate_configured_tag(row)
    if VALIDATION_TAG and not any(row["dicomTag"] == VALIDATION_TAG for row in active_tags):
        raise ValueError(f"VALIDATION_TAG {VALIDATION_TAG} is not present with status='Validating'.")
    print(f"Active DICOM tags: {len(active_tags)}")
    return active_tags


def read_last_successful_high_watermark():
    control = spark.read.format("delta").load(CONTROL_PATH)
    scoped = control.where(
        (F.col("pipelineName") == F.lit(PIPELINE_NAME))
        & (F.col("targetTable") == F.lit(TARGET_TABLE))
        & (F.coalesce(F.col("sourceSystem"), F.lit("ALL")) == F.lit(CONTROL_SOURCE_SYSTEM))
        & (F.coalesce(F.col("hashBucket"), F.lit(-1)) == F.lit(CONTROL_HASH_BUCKET))
    )
    rows = scoped.orderBy(F.col("updatedAt").desc()).limit(1).collect()
    if not rows:
        return None, None
    return rows[0]["lastSuccessfulHighWatermark"], rows[0]["lastSuccessfulRunId"]


def update_control_row(new_high_watermark):
    row = {
        "pipelineName": PIPELINE_NAME,
        "targetTable": TARGET_TABLE,
        "sourceSystem": CONTROL_SOURCE_SYSTEM,
        "hashBucket": CONTROL_HASH_BUCKET,
        "lastSuccessfulHighWatermark": new_high_watermark,
        "lastSuccessfulRunId": EXTRACT_RUN_ID,
        "updatedAt": current_utc_naive(),
    }
    control_df = spark.createDataFrame([row], CONTROL_SCHEMA)
    DeltaTable.forPath(spark, CONTROL_PATH).alias("t").merge(
        control_df.alias("s"),
        "t.pipelineName = s.pipelineName AND t.targetTable = s.targetTable AND coalesce(t.sourceSystem, 'ALL') = s.sourceSystem AND coalesce(t.hashBucket, -1) = s.hashBucket",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


def select_metadata_json(df):
    if "metadata" in df.columns:
        metadata_type = df.schema["metadata"].dataType
        if isinstance(metadata_type, (T.MapType, T.StructType)):
            return F.to_json(F.col("metadata"))
        if isinstance(metadata_type, T.StringType):
            return F.col("metadata")
    if "metadata_string" in df.columns:
        return F.col("metadata_string")
    raise ValueError("ImagingMetastore requires either metadata or metadata_string for DICOM metadata extraction.")


def configured_path_tag_chain(path: str):
    return re.findall(r"([0-9A-F]{8})\.Value\[\*\]", path)


def configured_string_path_schema(tag_chain):
    current_tag = tag_chain[-1]
    current_schema = T.StructType([
        T.StructField("vr", T.StringType(), True),
        T.StructField("Value", T.ArrayType(T.StringType()), True),
    ])
    for parent_tag in reversed(tag_chain[:-1]):
        item_schema = T.StructType([
            T.StructField(current_tag, current_schema, True),
        ])
        current_tag = parent_tag
        current_schema = T.StructType([
            T.StructField("vr", T.StringType(), True),
            T.StructField("Value", T.ArrayType(item_schema), True),
        ])
    return T.StructType([T.StructField(current_tag, current_schema, True)])


def configured_string_values_expr(path: str):
    tag_chain = configured_path_tag_chain(path)
    parsed = F.from_json(F.col("metadata_json"), configured_string_path_schema(tag_chain))
    node = parsed.getField(tag_chain[0])
    values = node.getField("Value")
    for child_tag in tag_chain[1:]:
        child_arrays = F.transform(
            values,
            lambda item: item.getField(child_tag).getField("Value"),
        )
        values = F.flatten(F.filter(child_arrays, lambda child_values: child_values.isNotNull()))
    return F.coalesce(values, F.array().cast("array<string>"))


def tag_value_expr(row):
    tag = row["dicomTag"]
    paths = parse_configured_paths(row)
    value_mode = row["valueMode"]
    if value_mode in ("FIRST_SCALAR", "PERSON_NAME_ALPHABETIC"):
        tag_chain = configured_path_tag_chain(paths[0])
        if len(paths) != 1 or len(tag_chain) != 1:
            raise ValueError(f"DICOM tag {tag} valueMode {value_mode} requires one top-level path.")
        tag_path = f"$.{tag}"
        first_value = F.get_json_object(F.col("metadata_json"), f"{tag_path}.Value[0]")
        if value_mode == "FIRST_SCALAR":
            return first_value
        alphabetic_struct = F.from_json(
            first_value,
            T.StructType([T.StructField("Alphabetic", T.StringType(), True)]),
        )
        return F.coalesce(
            F.get_json_object(F.col("metadata_json"), f"{tag_path}.Value[0].Alphabetic"),
            alphabetic_struct.getField("Alphabetic"),
        )

    path_values = [configured_string_values_expr(path) for path in paths]
    combined = F.concat(*path_values)
    non_empty = F.filter(
        combined,
        lambda value: value.isNotNull() & (F.length(F.trim(value)) > 0),
    )
    if value_mode == "ALL_DISTINCT_STRINGS_JSON":
        empty_strings = F.array().cast("array<string>")
        non_empty = F.aggregate(
            non_empty,
            empty_strings,
            lambda accumulated, value: F.when(
                F.array_contains(accumulated, value),
                accumulated,
            ).otherwise(F.concat(accumulated, F.array(value))),
        )
    return F.when(
        F.size(non_empty) == 0,
        F.lit(None).cast("string"),
    ).otherwise(F.to_json(non_empty))


def tag_present_expr(row):
    return tag_value_expr(row).isNotNull()


def dedupe_candidate_source(candidate_source):
    metadata_json_expr = select_metadata_json(candidate_source)
    ranked = candidate_source.withColumn("__metadata_json_for_dedupe", metadata_json_expr).withColumn("__metadata_length_for_dedupe", F.length(F.coalesce(F.col("__metadata_json_for_dedupe"), F.lit(""))))
    dedupe_window = Window.partitionBy("msftSourceSystem", "studyInstanceUid", "seriesInstanceUid", "sopInstanceUid", "filePath").orderBy(
        F.col("sourceModifiedAt").desc_nulls_last(), F.col("__metadata_length_for_dedupe").desc(), F.col("id").asc_nulls_last()
    )
    ranked = ranked.withColumn("__source_dedupe_rank", F.row_number().over(dedupe_window))
    source_rows_deduplicated = ranked.where(F.col("__source_dedupe_rank") > 1).count()
    deduped = ranked.where(F.col("__source_dedupe_rank") == 1).drop("__metadata_json_for_dedupe", "__metadata_length_for_dedupe", "__source_dedupe_rank")
    return deduped, source_rows_deduplicated


def build_study_reference(imaging_study, candidate_source):
    identifier_schema = T.ArrayType(T.StructType([
        T.StructField("system", T.StringType(), True),
        T.StructField("value", T.StringType(), True),
    ]))
    candidate_keys = candidate_source.select("msftSourceSystem", "studyInstanceUid").where(F.col("studyInstanceUid").isNotNull()).distinct()
    key_count = candidate_keys.count()
    if key_count == 0:
        return spark.createDataFrame([], T.StructType([
            T.StructField("msftSourceSystem", T.StringType(), True),
            T.StructField("studyInstanceUid", T.StringType(), True),
            T.StructField("imagingStudyId", T.StringType(), True),
        ]))

    source_systems = [row["msftSourceSystem"] for row in candidate_keys.select("msftSourceSystem").distinct().collect()]
    needed_studies = imaging_study.where(F.col("msftSourceSystem").isin(source_systems))

    if key_count <= 10000:
        uid_values = [row["studyInstanceUid"] for row in candidate_keys.select("studyInstanceUid").distinct().collect() if row["studyInstanceUid"]]
        if uid_values:
            uid_pattern = "|".join(re.escape(uid) for uid in uid_values)
            needed_studies = needed_studies.where(F.col("identifier_string").rlike(uid_pattern))

    parsed = needed_studies.withColumn("identifier_array", F.from_json(F.col("identifier_string"), identifier_schema))
    parse_failed = parsed.where(F.col("identifier_string").isNotNull() & F.col("identifier_array").isNull()).count()
    if parse_failed:
        raise ValueError(f"ImagingStudy.identifier_string parse failed for {parse_failed} needed rows; refusing regex fallback.")
    study_identifiers = (
        parsed
        .withColumn("identifier", F.explode_outer("identifier_array"))
        .where(F.col("identifier.system") == F.lit("urn:dicom:uid"))
        .select(
            F.col("id").alias("imagingStudyId"),
            F.col("msftSourceSystem"),
            F.col("identifier.value").alias("studyInstanceUidRaw"),
            F.regexp_replace(F.col("identifier.value"), r"^urn:oid:", "").alias("studyInstanceUid"),
            F.col("extension"),
            F.col("meta_lastUpdated"),
        )
        .join(candidate_keys, ["msftSourceSystem", "studyInstanceUid"], "inner")
    )
    window = Window.partitionBy("msftSourceSystem", "studyInstanceUid").orderBy(
        F.when(F.coalesce(F.col("extension").cast("string"), F.lit("")).contains("soft-deleted"), F.lit(1)).otherwise(F.lit(0)).asc(),
        F.when(F.col("studyInstanceUidRaw").startswith("urn:oid:"), F.lit(1)).otherwise(F.lit(0)).asc(),
        F.col("meta_lastUpdated").desc_nulls_last(),
        F.col("imagingStudyId").asc(),
    )
    return study_identifiers.withColumn("rn", F.row_number().over(window)).where(F.col("rn") == 1).drop("rn", "extension", "meta_lastUpdated", "studyInstanceUidRaw")


def build_wide_rows(joined_source, enabled_tags):
    extracted_at = current_utc_naive()
    with_metadata = joined_source.withColumn("metadata_json", select_metadata_json(joined_source))
    select_exprs = [
        F.col("id").alias("id"),
        F.col("id").alias("imagingMetastoreId"),
        F.sha2(F.concat_ws("|", F.coalesce(F.col("msftSourceSystem"), F.lit("")), F.coalesce(F.col("studyInstanceUid"), F.lit("")), F.coalesce(F.col("seriesInstanceUid"), F.lit("")), F.coalesce(F.col("sopInstanceUid"), F.lit("")), F.coalesce(F.col("filePath"), F.lit(""))), 256).alias("sourceRecordKey"),
        F.col("imagingStudyId"),
        F.col("msftSourceSystem"),
        F.col("studyInstanceUid"),
        F.col("seriesInstanceUid"),
        F.col("sopInstanceUid"),
        F.col("filePath"),
        F.col("sourceModifiedAt"),
        F.col("sourceModifiedDate"),
        F.col("sourceSystemHashBucket"),
    ]
    value_columns = []
    for row in enabled_tags:
        col_name = wide_column_name(row["canonicalColumnName"])
        value_columns.append(col_name)
        select_exprs.append(tag_value_expr(row).alias(col_name))
    staged = with_metadata.select(*select_exprs)
    hash_columns = [F.coalesce(F.col(col_name), F.lit("")) for col_name in value_columns]
    schema = target_schema(enabled_tags)
    staged = staged.select(
        *[F.col(field.name) for field in schema.fields if field.name in staged.columns],
        F.lit(True).cast("boolean").alias("isActive"),
        F.sha2(F.concat_ws("|", F.col("sourceRecordKey"), F.coalesce(F.col("sourceModifiedAt").cast("string"), F.lit("")), *hash_columns), 256).alias("sourceRowHash"),
        F.lit(extracted_at).cast("timestamp").alias("extractedAt"),
        F.lit(EXTRACT_RUN_ID).alias("extractRunId"),
    )
    return spark.createDataFrame(staged.select([field.name for field in schema.fields]).rdd, schema)


def merge_wide_table(staged, active_tags):
    schema = target_schema(active_tags)
    ensure_wide_target_table(TARGET_PATH, schema)
    target = DeltaTable.forPath(spark, TARGET_PATH)
    version_before_merge = target.history(1).select("version").collect()[0]["version"]
    metadata_key = "spark.databricks.delta.commitInfo.userMetadata"
    commit_metadata = f"extractRunId={EXTRACT_RUN_ID};batchId={BATCH_ID}"
    try:
        previous_metadata = spark.conf.get(metadata_key)
    except Exception:
        previous_metadata = None
    spark.conf.set(metadata_key, commit_metadata)
    try:
        (
            target.alias("t")
            .merge(staged.alias("s"), "t.sourceRecordKey = s.sourceRecordKey")
            .whenMatchedUpdateAll(condition="t.sourceRowHash <> s.sourceRowHash")
            .whenNotMatchedInsertAll()
            .execute()
        )
    finally:
        if previous_metadata is None:
            spark.conf.unset(metadata_key)
        else:
            spark.conf.set(metadata_key, previous_metadata)
    matching_commits = (
        target.history()
        .where(
            (F.col("version") > F.lit(version_before_merge))
            & (F.col("operation") == F.lit("MERGE"))
            & (F.col("userMetadata") == F.lit(commit_metadata))
        )
        .select("version", "operationMetrics")
        .collect()
    )
    if len(matching_commits) != 1:
        raise RuntimeError(
            f"Expected one tagged MERGE commit for {commit_metadata}; found {len(matching_commits)}."
        )
    operation_metrics = matching_commits[0]["operationMetrics"] or {}
    extension_rows_inserted = int(operation_metrics.get("numTargetRowsInserted", 0))
    extension_rows_updated = int(operation_metrics.get("numTargetRowsUpdated", 0))
    return extension_rows_inserted, extension_rows_updated


def validate_uid_columns(staged):
    study_mismatches = staged.where(F.col("studyInstanceUid_tag").isNotNull() & F.col("studyInstanceUid").isNotNull() & (F.col("studyInstanceUid") != F.col("studyInstanceUid_tag"))).count()
    series_mismatches = staged.where(F.col("seriesInstanceUid_tag").isNotNull() & F.col("seriesInstanceUid").isNotNull() & (F.col("seriesInstanceUid") != F.col("seriesInstanceUid_tag"))).count()
    sop_mismatches = staged.where(F.col("sopInstanceUid_tag").isNotNull() & F.col("sopInstanceUid").isNotNull() & (F.col("sopInstanceUid") != F.col("sopInstanceUid_tag"))).count()
    if study_mismatches or series_mismatches or sop_mismatches:
        write_validation_quarantine(staged, "StudyInstanceUID", "studyInstanceUid", "studyInstanceUid_tag")
        write_validation_quarantine(staged, "SeriesInstanceUID", "seriesInstanceUid", "seriesInstanceUid_tag")
        write_validation_quarantine(staged, "SOPInstanceUID", "sopInstanceUid", "sopInstanceUid_tag")
        raise ValueError(
            "UID validation failed: "
            f"studyUidMismatchRows={study_mismatches}, "
            f"seriesUidMismatchRows={series_mismatches}, "
            f"sopUidMismatchRows={sop_mismatches}"
        )
    return study_mismatches, series_mismatches, sop_mismatches
def cleanup_staging():
    notebookutils.fs.rm(STAGING_ROOT_PATH, True)


ensure_delta_table(TAG_DICTIONARY_PATH, TAG_DICTIONARY_SCHEMA)
ensure_delta_table(CONTROL_PATH, CONTROL_SCHEMA)
ensure_delta_table(RUN_PATH, RUN_SCHEMA)
ensure_delta_table(METRICS_PATH, METRICS_SCHEMA)
ensure_delta_table(SLA_CONFIG_PATH, SLA_CONFIG_SCHEMA)
ensure_delta_table(TIME_ZONE_OPTION_PATH, TIME_ZONE_OPTION_SCHEMA)
ensure_delta_table(LEASE_PATH, LEASE_SCHEMA)
ensure_delta_table(LEASE_LOCK_PATH, LEASE_SCHEMA)
ensure_delta_table(BATCH_MANIFEST_PATH, BATCH_MANIFEST_SCHEMA)
ensure_delta_table(PARSE_QUARANTINE_PATH, PARSE_QUARANTINE_SCHEMA)
ensure_delta_table(JOIN_QUARANTINE_PATH, JOIN_QUARANTINE_SCHEMA)
ensure_delta_table(VALIDATION_QUARANTINE_PATH, VALIDATION_QUARANTINE_SCHEMA)
ensure_delta_table(ANOMALY_PATH, ANOMALY_SCHEMA)
ensure_delta_table(GOVERNANCE_PATH, GOVERNANCE_SCHEMA)
ensure_delta_table(HEALTH_PATH, HEALTH_SCHEMA)
enabled_tags = seed_tag_dictionary()
seed_time_zone_options()
seed_governance_metadata()
acquire_run_lease()
if CLEAR_TARGET_BEFORE_RUN == "DELETE_ALL_EXTENSION_ROWS":
    ensure_wide_target_table(TARGET_PATH, target_schema(enabled_tags))
    DeltaTable.forPath(spark, TARGET_PATH).delete()
    print(f"Cleared all rows from {TARGET_TABLE} because CLEAR_TARGET_BEFORE_RUN was explicitly set.")
elif CLEAR_TARGET_BEFORE_RUN:
    raise ValueError("CLEAR_TARGET_BEFORE_RUN must be empty or exactly DELETE_ALL_EXTENSION_ROWS.")

last_high_watermark, last_successful_run_id = read_last_successful_high_watermark()

run_values = {
    "extractRunId": EXTRACT_RUN_ID,
    "batchId": BATCH_ID,
    "workspaceId": WORKSPACE_ID,
    "silverLakehouseId": SILVER_LH_ID,
    "adminLakehouseId": ADMIN_LH_ID,
    "pipelineName": PIPELINE_NAME,
    "orchestrationRunId": ORCHESTRATION_RUN_ID,
    "manifestBatchId": MANIFEST_BATCH_ID,
    "controlSourceSystem": CONTROL_SOURCE_SYSTEM,
    "controlHashBucket": CONTROL_HASH_BUCKET,
    "dryRunOnly": DRY_RUN_ONLY,
    "validationTag": VALIDATION_TAG or None,
    "targetTable": TARGET_TABLE,
    "startedAt": STARTED_AT,
    "completedAt": None,
    "status": "Running",
    "batchStartSourceModifiedAt": BATCH_START_SOURCE_MODIFIED_AT,
    "batchEndSourceModifiedAt": BATCH_END_SOURCE_MODIFIED_AT,
    "lastSuccessfulHighWatermarkBeforeRun": last_high_watermark,
    "newHighWatermarkAfterRun": None,
    "sourceRowsScanned": None,
    "sourceRowsActive": None,
    "sourceRowsJoinedToImagingStudy": None,
    "sourceRowsMissingImagingStudy": None,
    "metadataRowsParsed": None,
    "metadataRowsParseFailed": None,
    "extensionRowsStaged": None,
    "extensionRowsInserted": None,
    "extensionRowsUpdated": None,
    "extensionRowsSoftDeleted": 0,
    "extensionRowsPhysicallyDeleted": 0,
    "durationSeconds": None,
    "errorClass": None,
    "errorMessage": None,
    "notebookRunId": NOTEBOOK_RUN_ID,
}
update_run_row()
print(f"Started {PIPELINE_NAME}: extractRunId={EXTRACT_RUN_ID}, batchId={BATCH_ID}")
validation_target_existed = False
validation_target_version = None
validation_merge_started = False
if VALIDATION_TAG:
    validation_target_existed = DeltaTable.isDeltaTable(spark, TARGET_PATH)
    if validation_target_existed:
        validation_target_version = (
            DeltaTable.forPath(spark, TARGET_PATH).history(1).select("version").collect()[0]["version"]
        )

try:
    metastore = spark.read.format("delta").load(abfss_tables(SILVER_LH_ID, "ImagingMetastore"))
    imaging_study = spark.read.format("delta").load(abfss_tables(SILVER_LH_ID, "ImagingStudy"))
    require_columns(
        metastore,
        "ImagingMetastore",
        ["id", "msftSourceSystem", "studyInstanceUid", "seriesInstanceUid", "sopInstanceUid", "filePath", "sourceModifiedAt", "msftIsDeleted"],
    )
    require_columns(imaging_study, "ImagingStudy", ["id", "msftSourceSystem", "identifier_string", "extension", "meta_lastUpdated"])

    active_metastore = metastore.where(F.coalesce(F.col("msftIsDeleted"), F.lit(False)) == F.lit(False))
    if FILTER_STUDY_INSTANCE_UIDS:
        candidate_source = active_metastore.where(F.col("studyInstanceUid").isin(FILTER_STUDY_INSTANCE_UIDS))
        print(f"Batch mode: explicit FILTER_STUDY_INSTANCE_UIDS ({len(FILTER_STUDY_INSTANCE_UIDS)} studies); watermark control will not advance.")
    elif SAMPLE_STUDY_COUNT is not None:
        sample_studies = (
            active_metastore
            .where(F.col("studyInstanceUid").isNotNull())
            .select("studyInstanceUid", "sourceModifiedAt")
            .groupBy("studyInstanceUid")
            .agg(F.max("sourceModifiedAt").alias("latestSourceModifiedAt"))
            .orderBy(F.col("latestSourceModifiedAt").desc_nulls_last(), F.col("studyInstanceUid").asc())
            .limit(SAMPLE_STUDY_COUNT)
            .select("studyInstanceUid")
        )
        candidate_source = active_metastore.join(sample_studies, "studyInstanceUid", "inner")
        print(f"Batch mode: deterministic SAMPLE_STUDY_COUNT={SAMPLE_STUDY_COUNT}; watermark control will not advance.")
    elif EXPLICIT_BATCH_MODE:
        candidate_source = active_metastore.where(
            (F.col("sourceModifiedAt") >= F.lit(BATCH_START_SOURCE_MODIFIED_AT).cast("timestamp"))
            & (F.col("sourceModifiedAt") < F.lit(BATCH_END_SOURCE_MODIFIED_AT).cast("timestamp"))
        )
        print(f"Batch mode: explicit {BATCH_START_SOURCE_MODIFIED_AT} <= sourceModifiedAt < {BATCH_END_SOURCE_MODIFIED_AT}")
    elif last_high_watermark is not None:
        batch_start = last_high_watermark - timedelta(hours=WATERMARK_OVERLAP_HOURS)
        candidate_source = active_metastore.where(F.col("sourceModifiedAt") >= F.lit(batch_start).cast("timestamp"))
        print(f"Batch mode: incremental sourceModifiedAt >= {batch_start} (overlap {WATERMARK_OVERLAP_HOURS}h)")
    else:
        candidate_source = active_metastore
        print("Batch mode: initial load; no sourceModifiedAt filter.")

    candidate_source = candidate_source.withColumn(
        "sourceModifiedDate",
        F.coalesce(F.to_date(F.col("sourceModifiedAt")), F.to_date(F.lit("1900-01-01"))),
    ).withColumn(
        "sourceSystemHashBucket",
        # Distribute on a stable composite (source system + study) so a single large PACS
        # spreads across all HASH_BUCKET_COUNT buckets instead of collapsing into ~7 buckets
        # (one per source system). Study-level granularity keeps a study's instances co-located.
        F.pmod(
            F.xxhash64(
                F.coalesce(F.col("msftSourceSystem"), F.lit("")),
                F.coalesce(F.col("studyInstanceUid"), F.lit("")),
            ),
            F.lit(HASH_BUCKET_COUNT),
        ).cast("int"),
    )
    if FILTER_SOURCE_SYSTEM:
        candidate_source = candidate_source.where(F.col("msftSourceSystem") == F.lit(FILTER_SOURCE_SYSTEM))
        print(f"Applied source-system filter: {FILTER_SOURCE_SYSTEM}")
    if FILTER_SOURCE_SYSTEM_HASH_BUCKET is not None:
        candidate_source = candidate_source.where(F.col("sourceSystemHashBucket") == F.lit(FILTER_SOURCE_SYSTEM_HASH_BUCKET))
        print(f"Applied source-system hash-bucket filter: {FILTER_SOURCE_SYSTEM_HASH_BUCKET}")

    source_rows_scanned = candidate_source.count()
    source_modified_at_null_rows = candidate_source.where(F.col("sourceModifiedAt").isNull()).count()
    source_batch_high_watermark = candidate_source.agg(F.max("sourceModifiedAt").alias("maxSourceModifiedAt")).collect()[0]["maxSourceModifiedAt"]
    upsert_manifest("Running", source_rows_expected=source_rows_scanned)
    if DRY_RUN_ONLY:
        update_run_row(
            completedAt=current_utc_naive(),
            status="DryRunSucceeded",
            durationSeconds=(current_utc_naive() - STARTED_AT).total_seconds(),
            sourceRowsScanned=source_rows_scanned,
            sourceRowsActive=source_rows_scanned,
            newHighWatermarkAfterRun=source_batch_high_watermark,
        )
        upsert_manifest("DryRunSucceeded", source_rows_expected=source_rows_scanned, source_rows_processed=0, completed=True)
        release_run_lease("Released")
        print(f"DRY_RUN_ONLY complete: candidate source rows={source_rows_scanned}")
    else:
        if source_rows_scanned > MAX_SOURCE_ROWS_PER_BATCH:
            raise ValueError(
                f"Candidate source row count {source_rows_scanned} exceeds MAX_SOURCE_ROWS_PER_BATCH={MAX_SOURCE_ROWS_PER_BATCH}. "
                "Rerun with narrower BATCH_START_SOURCE_MODIFIED_AT/BATCH_END_SOURCE_MODIFIED_AT parameters."
            )
        if source_rows_scanned and (source_modified_at_null_rows / source_rows_scanned) > 0.0001:
            raise ValueError(f"sourceModifiedAt null rows {source_modified_at_null_rows}/{source_rows_scanned} exceed 0.01% threshold.")
    
        candidate_source, source_rows_deduplicated = dedupe_candidate_source(candidate_source)
        source_rows_active = source_rows_scanned - source_rows_deduplicated
        if source_rows_deduplicated:
            print(f"Deduplicated {source_rows_deduplicated} active ImagingMetastore source rows by id using deterministic ordering.")
    
        candidate_source.write.format("delta").mode("overwrite").save(SOURCE_STAGING_PATH)
        candidate_source = spark.read.format("delta").load(SOURCE_STAGING_PATH)
        print(f"Source scanned/deduplicated/active: {source_rows_scanned}/{source_rows_deduplicated}/{source_rows_active}; staged at {SOURCE_STAGING_PATH}")
    
        study_reference = build_study_reference(imaging_study, candidate_source)
        joined_source = candidate_source.join(study_reference, ["msftSourceSystem", "studyInstanceUid"], "left")
        source_rows_missing_imaging_study = joined_source.where(F.col("imagingStudyId").isNull()).count()
        source_rows_joined_to_imaging_study = source_rows_active - source_rows_missing_imaging_study
        if source_rows_missing_imaging_study:
            write_join_quarantine(joined_source)
            raise ValueError(f"{source_rows_missing_imaging_study} active ImagingMetastore rows did not join to ImagingStudy.")
    
        staged_wide = build_wide_rows(joined_source, enabled_tags)
        metadata_rows_parsed = staged_wide.where(
            F.coalesce(*[F.col(wide_column_name(row["canonicalColumnName"])) for row in enabled_tags]).isNotNull()
        ).count()
        metadata_rows_parse_failed = source_rows_active - metadata_rows_parsed
        if metadata_rows_parse_failed:
            write_parse_quarantine(staged_wide, enabled_tags)
        if VALIDATION_TAG and metadata_rows_parse_failed:
            raise ValueError(f"Validation tag run requires zero metadata parse failures; found {metadata_rows_parse_failed}/{source_rows_active}.")
        if source_rows_active and (metadata_rows_parse_failed / source_rows_active) > 0.01:
            raise ValueError(f"metadataRowsParseFailed {metadata_rows_parse_failed}/{source_rows_active} exceeds 1% threshold.")
    
        duplicate_key_rows = staged_wide.groupBy("sourceRecordKey").count().where(F.col("count") > 1).count()
        if duplicate_key_rows:
            raise ValueError(f"Staged wide rows contain {duplicate_key_rows} duplicate sourceRecordKey keys after source-row dedupe.")
    
        staged_wide.write.format("delta").mode("overwrite").save(WIDE_STAGING_PATH)
        staged_wide = spark.read.format("delta").load(WIDE_STAGING_PATH)
        extension_rows_staged = staged_wide.count()
        study_uid_mismatches, series_uid_mismatches, sop_uid_mismatches = validate_uid_columns(staged_wide)
        validation_merge_started = bool(VALIDATION_TAG)
        extension_rows_inserted, extension_rows_updated = merge_wide_table(staged_wide, enabled_tags)
    
        print(
            "Extraction counts: "
            f"joined={source_rows_joined_to_imaging_study}, missingStudy={source_rows_missing_imaging_study}, "
            f"metadataParsed={metadata_rows_parsed}, metadataParseFailed={metadata_rows_parse_failed}, "
            f"wideRowsStaged={extension_rows_staged}, inserted={extension_rows_inserted}, updated={extension_rows_updated}"
        )
    
        metric_rows = []
        append_metric(metric_rows, "source", "sourceRowsScanned", source_rows_scanned)
        append_metric(metric_rows, "source", "sourceRowsActive", source_rows_active)
        append_metric(metric_rows, "source", "sourceRowsDeduplicated", source_rows_deduplicated)
        append_metric(metric_rows, "source", "sourceRowsJoinedToImagingStudy", source_rows_joined_to_imaging_study)
        append_metric(metric_rows, "source", "sourceRowsMissingImagingStudy", source_rows_missing_imaging_study)
        append_metric(metric_rows, "source", "sourceModifiedAtNullRows", source_modified_at_null_rows)
        append_metric(metric_rows, "parse", "metadataRowsParsed", metadata_rows_parsed)
        append_metric(metric_rows, "parse", "metadataRowsParseFailed", metadata_rows_parse_failed)
        append_metric(metric_rows, "extension", "extensionRowsStaged", extension_rows_staged)
        append_metric(metric_rows, "extension", "extensionRowsInserted", extension_rows_inserted)
        append_metric(metric_rows, "extension", "extensionRowsUpdated", extension_rows_updated)
    
        coverage_aggs = []
        for row in enabled_tags:
            col_name = wide_column_name(row["canonicalColumnName"])
            coverage_aggs.append(F.sum(F.when(F.col(col_name).isNotNull(), F.lit(1)).otherwise(F.lit(0))).cast("double").alias(col_name))
        coverage_row = staged_wide.agg(*coverage_aggs).collect()[0].asDict() if coverage_aggs else {}
        for row in enabled_tags:
            col_name = wide_column_name(row["canonicalColumnName"])
            append_metric(metric_rows, "tagCoverage", "nonNullValueRows", coverage_row.get(col_name, 0), row["dicomTag"], row["dicomKeyword"], metric_json=col_name)
        append_metric(metric_rows, "uidValidation", "studyUidMismatchRows", study_uid_mismatches)
        append_metric(metric_rows, "uidValidation", "seriesUidMismatchRows", series_uid_mismatches)
        append_metric(metric_rows, "uidValidation", "sopUidMismatchRows", sop_uid_mismatches)
    
        write_anomaly_checks(source_rows_scanned, source_rows_active, metadata_rows_parse_failed, source_rows_missing_imaging_study, extension_rows_staged)
        try:
            write_metrics_rows(metric_rows)
        except Exception as metrics_exc:
            update_run_row(
                completedAt=current_utc_naive(),
                status="FailedMetricsWrite",
                errorClass=metrics_exc.__class__.__name__,
                errorMessage=f"Metrics write failed after target merge; staging retained at {STAGING_ROOT_PATH}: {str(metrics_exc)[:3500]}",
                durationSeconds=(current_utc_naive() - STARTED_AT).total_seconds(),
                sourceRowsScanned=source_rows_scanned,
                sourceRowsActive=source_rows_active,
                sourceRowsJoinedToImagingStudy=source_rows_joined_to_imaging_study,
                sourceRowsMissingImagingStudy=source_rows_missing_imaging_study,
                metadataRowsParsed=metadata_rows_parsed,
                metadataRowsParseFailed=metadata_rows_parse_failed,
                extensionRowsStaged=extension_rows_staged,
                extensionRowsInserted=extension_rows_inserted,
                extensionRowsUpdated=extension_rows_updated,
                newHighWatermarkAfterRun=source_batch_high_watermark,
            )
            write_health_snapshot("FailedMetricsWrite", locals().get("source_rows_deduplicated"), locals().get("study_uid_mismatches"), locals().get("series_uid_mismatches"), locals().get("sop_uid_mismatches"))
            upsert_manifest("FailedMetricsWrite", source_rows_expected=source_rows_scanned, source_rows_processed=extension_rows_staged, error_message=metrics_exc, completed=True)
            raise
    
        update_run_row(
            completedAt=current_utc_naive(),
            status="Succeeded",
            errorClass=None,
            errorMessage=None,
            durationSeconds=(current_utc_naive() - STARTED_AT).total_seconds(),
            sourceRowsScanned=source_rows_scanned,
            sourceRowsActive=source_rows_active,
            sourceRowsJoinedToImagingStudy=source_rows_joined_to_imaging_study,
            sourceRowsMissingImagingStudy=source_rows_missing_imaging_study,
            metadataRowsParsed=metadata_rows_parsed,
            metadataRowsParseFailed=metadata_rows_parse_failed,
            extensionRowsStaged=extension_rows_staged,
            extensionRowsInserted=extension_rows_inserted,
            extensionRowsUpdated=extension_rows_updated,
            newHighWatermarkAfterRun=source_batch_high_watermark,
        )
        if FILTER_STUDY_INSTANCE_UIDS or SAMPLE_STUDY_COUNT is not None or VALIDATION_TAG:
            print("Skipped control watermark update for explicit study-filter/sample/validation run.")
        else:
            update_control_row(source_batch_high_watermark)
        upsert_manifest("Succeeded", source_rows_expected=source_rows_scanned, source_rows_processed=source_rows_active, completed=True)
    
        try:
            cleanup_staging()
        except Exception as cleanup_exc:
            cleanup_metric = []
            append_metric(cleanup_metric, "cleanup", "stagingCleanupFailed", 1, metric_json=STAGING_ROOT_PATH)
            write_metrics_rows(cleanup_metric)
            update_run_row(
                status="SucceededWithCleanupWarning",
                errorClass=cleanup_exc.__class__.__name__,
                errorMessage=f"Staging cleanup failed for {STAGING_ROOT_PATH}: {str(cleanup_exc)[:3500]}",
            )
            write_health_snapshot("SucceededWithCleanupWarning", source_rows_deduplicated, study_uid_mismatches, series_uid_mismatches, sop_uid_mismatches)
            print(f"Succeeded with staging cleanup warning. Staging retained at {STAGING_ROOT_PATH}: {cleanup_exc}")
        else:
            print(f"Staging cleanup complete: {STAGING_ROOT_PATH}")
            write_health_snapshot("Succeeded", source_rows_deduplicated, study_uid_mismatches, series_uid_mismatches, sop_uid_mismatches)
        release_run_lease("Released")
    
        print(f"{PIPELINE_NAME} completed successfully: extractRunId={EXTRACT_RUN_ID}")
        print(
            "Deferred reconciliation marker: delete/patch reconciliation is intentionally handled by future notebook "
            f"{FUTURE_RECONCILIATION_NOTEBOOK}. Scope: include it in the existing imaging patch pipeline, read HDS patch/delete outputs, "
            "apply isActive=false/sourceIsDeleted=true/deletedDetectedAt updates to ImagingMetastoreExtension, and write separate Admin run/metrics rows."
        )
except Exception as exc:
    rollback_error = None
    if VALIDATION_TAG and validation_merge_started:
        try:
            if validation_target_existed:
                spark.sql(f"RESTORE TABLE delta.`{TARGET_PATH}` TO VERSION AS OF {validation_target_version}")
                print(f"Restored {TARGET_TABLE} to version {validation_target_version} after failed validation run.")
            else:
                notebookutils.fs.rm(TARGET_PATH, True)
                print(f"Removed newly created {TARGET_TABLE} after failed validation run.")
        except Exception as validation_rollback_exc:
            rollback_error = validation_rollback_exc
            print(f"CRITICAL: failed to roll back validation target changes: {validation_rollback_exc}")
    try:
        update_run_row(
            completedAt=current_utc_naive(),
            status="Failed",
            errorClass=exc.__class__.__name__,
            errorMessage=f"{str(exc)[:3500]} | staging retained at {STAGING_ROOT_PATH}",
            durationSeconds=(current_utc_naive() - STARTED_AT).total_seconds(),
        )
        write_health_snapshot("Failed", locals().get("source_rows_deduplicated"), locals().get("study_uid_mismatches"), locals().get("series_uid_mismatches"), locals().get("sop_uid_mismatches"))
        upsert_manifest("Failed", source_rows_expected=locals().get("source_rows_scanned"), source_rows_processed=locals().get("source_rows_active"), error_message=exc, completed=True)
        release_run_lease("Failed")
    except Exception as audit_exc:
        print(f"Failed to update run audit after extraction failure: {audit_exc}")
    if rollback_error is not None:
        raise RuntimeError(
            f"Validation failed and target rollback also failed: {rollback_error}"
        ) from exc
    raise
