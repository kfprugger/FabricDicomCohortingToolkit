# Fabric Notebook: Materialize Reporting Tables
# Reads from Silver + Gold OMOP lakehouses, computes derived columns,
# and writes flat Delta tables to healthcare1_reporting_gold lakehouse.
#
# Strategy:
#   - ImagingStudy.subject_string contains the patient identifier in
#     "identifier.value". This ID comes from the DICOM loader's re-tagging
#     (ds.PatientID = FHIR Patient.id from the FHIR server).
#   - Silver Patient table has id (HDS hash) and idOrig (original FHIR id).
#   - The HDS imaging pipeline may use a different identifier system than
#     the FHIR patient ingestion, so we handle both:
#       a) Try to join imaging patient UUIDs to Patient.idOrig.
#       b) For unmatched imaging patients, create stub patient rows.
#       c) Also include all Silver Patient rows.
#
# NOTE: All lakehouse IDs are resolved dynamically via notebookutils.
#       No hardcoded GUIDs required. Attach this notebook to any workspace
#       containing HDS lakehouses and a 'healthcare1_reporting_gold' lakehouse.

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, StringType
from datetime import date
import json

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# Dynamic ID resolution via notebookutils
# ---------------------------------------------------------------------------
import notebookutils

# Get current workspace ID (try modern API, fall back to runtime context)
try:
    WORKSPACE_ID = notebookutils.fabric.resolve_workspace_id()
except AttributeError:
    WORKSPACE_ID = notebookutils.runtime.context.get("currentWorkspaceId",
                   notebookutils.runtime.context.get("workspaceId"))

# Resolve lakehouse IDs by display name using Fabric REST API
def resolve_lakehouse_id(name):
    token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
    import requests
    resp = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/lakehouses",
        headers={"Authorization": f"Bearer {token}"}
    )
    resp.raise_for_status()
    for lh in resp.json().get("value", []):
        if lh["displayName"] == name:
            return lh["id"]
    raise ValueError(f"Lakehouse '{name}' not found in workspace {WORKSPACE_ID}")

# Lakehouse names (standard HDS naming convention)
SILVER_LH_NAME = "healthcare1_msft_silver"
GOLD_OMOP_LH_NAME = "healthcare1_msft_gold_omop"
REPORTING_LH_NAME = "healthcare1_reporting_gold"

print("Resolving lakehouse IDs...")
SILVER_LH_ID = resolve_lakehouse_id(SILVER_LH_NAME)
GOLD_OMOP_LH_ID = resolve_lakehouse_id(GOLD_OMOP_LH_NAME)
REPORTING_LH_ID = resolve_lakehouse_id(REPORTING_LH_NAME)

print(f"  Workspace:    {WORKSPACE_ID}")
print(f"  Silver LH:    {SILVER_LH_ID} ({SILVER_LH_NAME})")
print(f"  Gold OMOP LH: {GOLD_OMOP_LH_ID} ({GOLD_OMOP_LH_NAME})")
print(f"  Reporting LH: {REPORTING_LH_ID} ({REPORTING_LH_NAME})")

# Resolve OHIF Viewer URL from deployment state (or use default)
OHIF_VIEWER_BASE_URL = "https://example.azurestaticapps.net/viewer?StudyInstanceUIDs="
try:
    # Try to read from the dicom-viewer deployment state if accessible
    import os
    state_path = os.environ.get("OHIF_VIEWER_BASE_URL")
    if state_path:
        OHIF_VIEWER_BASE_URL = state_path
        print(f"  OHIF Viewer:  {OHIF_VIEWER_BASE_URL} (from env)")
    else:
        print(f"  OHIF Viewer:  {OHIF_VIEWER_BASE_URL} (default — update OHIF_VIEWER_BASE_URL env var)")
except Exception:
    print(f"  OHIF Viewer:  {OHIF_VIEWER_BASE_URL} (default)")

def abfss(lh_id, path=""):
    return f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{lh_id}/Tables/{path}"

# ---------------------------------------------------------------------------
# 1. ImagingStudyReporting (build first — we need patient UUIDs)
# ---------------------------------------------------------------------------
print("=== ImagingStudyReporting ===")
imaging_rpt = None
try:
    imaging_df = spark.read.format("delta").load(abfss(SILVER_LH_ID, "ImagingStudy"))

    imaging_rpt = imaging_df.select(
        F.col("id").alias("StudyId"),
        F.col("started").alias("StudyDate"),
        F.col("numberOfSeries").alias("NumberOfSeries"),
        F.col("numberOfInstances").alias("NumberOfInstances"),
        F.col("identifier_string"),
        F.col("subject_string"),
        F.col("series_string"),
    )

    # StudyInstanceUid from identifier_string JSON  {"value":"..."}
    imaging_rpt = imaging_rpt.withColumn(
        "StudyInstanceUid",
        F.regexp_extract(F.col("identifier_string"), r'"value"\s*:\s*"([^"]*)"', 1)
    ).withColumn(
        "StudyInstanceUid",
        F.when(F.col("StudyInstanceUid") == "", None).otherwise(F.col("StudyInstanceUid"))
    )

    # PatientUUID from subject_string — nested at identifier.value
    # JSON: {"identifier":{"value":"<uuid>",...},...}
    imaging_rpt = imaging_rpt.withColumn(
        "PatientUUID",
        F.regexp_extract(F.col("subject_string"),
                         r'"identifier"\s*:\s*\{[^}]*"value"\s*:\s*"([^"]*)"', 1)
    ).withColumn(
        "PatientUUID",
        F.when(F.col("PatientUUID") == "", None).otherwise(F.col("PatientUUID"))
    )

    # Modality from series_string JSON  {"code":"..."}
    imaging_rpt = imaging_rpt.withColumn(
        "Modality",
        F.regexp_extract(F.col("series_string"), r'"code"\s*:\s*"([^"]*)"', 1)
    ).withColumn(
        "Modality",
        F.when(F.col("Modality") == "", None).otherwise(F.col("Modality"))
    )

    # ModalityName
    modality_map = {
        "CT": "Computed Tomography", "MR": "Magnetic Resonance (MRI)",
        "MG": "Mammography", "CR": "Computed Radiography",
        "NM": "Nuclear Medicine", "PT": "Positron Emission Tomography (PET)",
        "OP": "Ophthalmic Photography", "OPT": "Ophthalmic Tomography (OCT)",
        "XC": "External-camera Photography", "US": "Ultrasound",
        "XA": "X-Ray Angiography", "DX": "Digital Radiography",
        "SC": "Secondary Capture", "RF": "Radiofluoroscopy",
        "IO": "Intra-Oral Radiography",
    }
    mapping_expr = F.coalesce(
        *[F.when(F.col("Modality") == code, F.lit(name)) for code, name in modality_map.items()],
        F.col("Modality")
    )
    imaging_rpt = imaging_rpt.withColumn("ModalityName", mapping_expr)

    # StudyYear
    imaging_rpt = imaging_rpt.withColumn(
        "StudyYear",
        F.when(F.col("StudyDate").isNull(), None).otherwise(F.year(F.col("StudyDate")).cast(IntegerType()))
    )

    # ViewerUrl — link to OHIF viewer for this study
    imaging_rpt = imaging_rpt.withColumn(
        "ViewerUrl",
        F.when(F.col("StudyInstanceUid").isNotNull(),
               F.concat(F.lit(OHIF_VIEWER_BASE_URL), F.col("StudyInstanceUid")))
        .otherwise(None)
    )

    # Drop helper columns
    imaging_rpt = imaging_rpt.drop("identifier_string", "subject_string", "series_string")

    imaging_rpt.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .save(abfss(REPORTING_LH_ID, "ImagingStudyReporting"))
    print(f"  Written {imaging_rpt.count()} rows")

    # Collect imaging patient UUIDs for PatientReporting
    imaging_patient_uuids = imaging_rpt.select("PatientUUID").distinct().filter("PatientUUID IS NOT NULL")
    imaging_patient_count = imaging_patient_uuids.count()
    print(f"  Distinct imaging patients: {imaging_patient_count}")
except Exception as e:
    import traceback
    print(f"  IMAGING ERROR: {e}")
    traceback.print_exc()
    imaging_patient_uuids = None

# ---------------------------------------------------------------------------
# 2. PatientReporting — build from Silver Patient, then add imaging-only patients
# ---------------------------------------------------------------------------
print("=== PatientReporting ===")
try:
    patient_df = spark.read.format("delta").load(abfss(SILVER_LH_ID, "Patient"))
    today = date.today()

    # Build from Silver Patient table
    patient_rpt = patient_df.select(
        F.col("id").alias("PatientId"),
        F.col("idOrig").alias("PatientUUID"),
        F.col("gender").alias("Gender"),
        F.col("birthDate").alias("BirthDate"),
        F.col("name_string"),
    )

    # Extract names
    patient_rpt = patient_rpt.withColumn(
        "FirstName",
        F.regexp_extract(F.col("name_string"), r'"given"\s*:\s*\[\s*"([^"]*)"', 1)
    ).withColumn("FirstName", F.when(F.col("FirstName") == "", None).otherwise(F.col("FirstName")))

    patient_rpt = patient_rpt.withColumn(
        "LastName",
        F.regexp_extract(F.col("name_string"), r'"family"\s*:\s*"([^"]*)"', 1)
    ).withColumn("LastName", F.when(F.col("LastName") == "", None).otherwise(F.col("LastName")))

    patient_rpt = patient_rpt.withColumn(
        "FullName",
        F.when(F.col("FirstName").isNull() & F.col("LastName").isNull(),
               F.concat(F.lit("Patient "), F.substring(F.col("PatientId"), 1, 12)))
        .otherwise(F.trim(F.concat_ws(" ", F.col("FirstName"), F.col("LastName"))))
    )

    patient_rpt = patient_rpt.withColumn(
        "Age",
        F.when(F.col("BirthDate").isNull(), None)
        .otherwise(F.floor(F.datediff(F.lit(today), F.col("BirthDate")) / 365.25).cast(IntegerType()))
    )

    patient_rpt = patient_rpt.withColumn(
        "AgeRange",
        F.when(F.col("Age").isNull(), "Unknown")
         .when(F.col("Age") < 18, "0-17").when(F.col("Age") < 30, "18-29")
         .when(F.col("Age") < 40, "30-39").when(F.col("Age") < 50, "40-49")
         .when(F.col("Age") < 60, "50-59").when(F.col("Age") < 70, "60-69")
         .when(F.col("Age") < 80, "70-79").otherwise("80+")
    )

    patient_rpt = patient_rpt.drop("name_string")
    silver_patient_count = patient_rpt.count()
    print(f"  Silver patients: {silver_patient_count}")

    # Now add imaging-only patients from DICOM metadata
    # Read imaging patient UUIDs and extract demographics from ImagingMetastore
    try:
        imaging_study_df = spark.read.format("delta").load(abfss(REPORTING_LH_ID, "ImagingStudyReporting"))
        imaging_uuids_list = [row.PatientUUID for row in
                              imaging_study_df.select("PatientUUID").distinct()
                              .filter("PatientUUID IS NOT NULL").collect()]
        existing_uuids_set = set(row.PatientUUID for row in patient_rpt.select("PatientUUID").collect())
        new_uuids = [u for u in imaging_uuids_list if u not in existing_uuids_set]
        print(f"  Imaging UUIDs: {len(imaging_uuids_list)}, existing: {len(existing_uuids_set)}, new: {len(new_uuids)}")

        if new_uuids:
            # Extract patient demographics from DICOM metadata tags
            # Tag 00100010 = PatientName (PN), 00100020 = PatientID (LO)
            # Tag 00100030 = BirthDate (DA), 00100040 = Sex (CS)
            metastore_df = spark.read.format("delta").load(abfss(SILVER_LH_ID, "ImagingMetastore"))

            # Extract PatientID (00100020) to match with our UUIDs
            dicom_patients = metastore_df.select(
                F.regexp_extract(F.col("metadata_string"),
                    r'"00100020":\s*\{"vr":\s*"LO",\s*"Value":\s*\["([^"]*)"', 1).alias("PatientUUID"),
                F.regexp_extract(F.col("metadata_string"),
                    r'"00100010":\s*\{"vr":\s*"PN",\s*"Value":\s*\[\{"Alphabetic":\s*"([^"]*)"', 1).alias("dicom_name"),
                F.regexp_extract(F.col("metadata_string"),
                    r'"00100030":\s*\{"vr":\s*"DA",\s*"Value":\s*\["(\d{8})"', 1).alias("dicom_birthdate"),
                F.regexp_extract(F.col("metadata_string"),
                    r'"00100040":\s*\{"vr":\s*"CS",\s*"Value":\s*\["([^"]*)"', 1).alias("dicom_sex"),
            ).filter(F.col("PatientUUID") != "").dropDuplicates(["PatientUUID"])

            dicom_patients_collected = {row.PatientUUID: row for row in dicom_patients.collect()}
            print(f"  DICOM patients found: {len(dicom_patients_collected)}")

            from datetime import datetime
            stub_rows = []
            for uuid in new_uuids:
                dcm = dicom_patients_collected.get(uuid)
                if dcm and dcm.dicom_name:
                    # Parse DICOM PN format: Family^Given
                    parts = dcm.dicom_name.split("^")
                    last_name = parts[0] if len(parts) > 0 else None
                    first_name = parts[1] if len(parts) > 1 else None
                    full_name = f"{first_name} {last_name}" if first_name and last_name else dcm.dicom_name
                else:
                    first_name = None
                    last_name = None
                    full_name = f"Patient {uuid[:8]}"

                # Parse birth date
                birth_date = None
                age = None
                if dcm and dcm.dicom_birthdate and len(dcm.dicom_birthdate) == 8:
                    try:
                        birth_date = datetime.strptime(dcm.dicom_birthdate, "%Y%m%d")
                        age = int((today - birth_date.date()).days / 365.25)
                    except ValueError:
                        pass

                # Parse sex
                gender = None
                if dcm and dcm.dicom_sex:
                    gender = {"M": "male", "F": "female"}.get(dcm.dicom_sex.upper(), dcm.dicom_sex.lower())

                # Age range
                if age is None:
                    age_range = "Unknown"
                elif age < 18: age_range = "0-17"
                elif age < 30: age_range = "18-29"
                elif age < 40: age_range = "30-39"
                elif age < 50: age_range = "40-49"
                elif age < 60: age_range = "50-59"
                elif age < 70: age_range = "60-69"
                elif age < 80: age_range = "70-79"
                else: age_range = "80+"

                stub_rows.append({
                    "PatientId": uuid,
                    "PatientUUID": uuid,
                    "FirstName": first_name,
                    "LastName": last_name,
                    "Gender": gender,
                    "BirthDate": birth_date,
                    "FullName": full_name,
                    "Age": age,
                    "AgeRange": age_range,
                })
            stub_df = spark.createDataFrame(stub_rows, schema=patient_rpt.schema)
            patient_rpt = patient_rpt.union(stub_df)
            print(f"  Total after DICOM enrichment: {patient_rpt.count()}")
    except Exception as e2:
        print(f"  DICOM enrichment error: {e2}")
        import traceback
        traceback.print_exc()

    patient_rpt.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .save(abfss(REPORTING_LH_ID, "PatientReporting"))
    final_count = spark.read.format("delta").load(abfss(REPORTING_LH_ID, "PatientReporting")).count()
    print(f"  Written and verified: {final_count} rows")
except Exception as e:
    import traceback
    print(f"  ERROR: {e}")
    traceback.print_exc()

# ---------------------------------------------------------------------------
# 3. DicomFileReporting
# ---------------------------------------------------------------------------
print("=== DicomFileReporting ===")
try:
    dicom_df = spark.read.format("delta").load(abfss(SILVER_LH_ID, "ImagingMetastore"))

    dicom_rpt = dicom_df.select(
        F.col("id").alias("FileId"),
        F.col("studyInstanceUid").alias("StudyInstanceUid"),
        F.col("seriesInstanceUid").alias("SeriesInstanceUid"),
        F.col("sopInstanceUid").alias("SopInstanceUid"),
        F.col("filePath").alias("FilePath"),
        F.col("msftSourceSystem").alias("SourceSystem"),
        F.col("sourceModifiedAt").alias("SourceModifiedAt"),
    )

    dicom_rpt.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .save(abfss(REPORTING_LH_ID, "DicomFileReporting"))
    print(f"  Written {dicom_rpt.count()} rows")
except Exception as e:
    print(f"  SKIPPED: {e}")

# ---------------------------------------------------------------------------
# 4. PersonDemographicsReporting
# ---------------------------------------------------------------------------
print("=== PersonDemographicsReporting ===")
try:
    person_df = spark.read.format("delta").load(abfss(GOLD_OMOP_LH_ID, "person"))
    concept_df = spark.read.format("delta").load(abfss(GOLD_OMOP_LH_ID, "concept"))

    race_concept = concept_df.select(
        F.col("concept_id").alias("race_cid"), F.col("concept_name").alias("Race"))
    eth_concept = concept_df.select(
        F.col("concept_id").alias("eth_cid"), F.col("concept_name").alias("Ethnicity"))

    demo_rpt = person_df.select(
        F.col("person_source_value").alias("PatientId"),
        F.col("race_concept_id"), F.col("ethnicity_concept_id"))

    demo_rpt = demo_rpt.join(race_concept, demo_rpt["race_concept_id"] == race_concept["race_cid"], "left") \
        .drop("race_cid", "race_concept_id")
    demo_rpt = demo_rpt.join(eth_concept, demo_rpt["ethnicity_concept_id"] == eth_concept["eth_cid"], "left") \
        .drop("eth_cid", "ethnicity_concept_id")
    demo_rpt = demo_rpt.fillna({"Race": "Unknown", "Ethnicity": "Unknown"})

    demo_rpt.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .save(abfss(REPORTING_LH_ID, "PersonDemographicsReporting"))
    print(f"  Written {demo_rpt.count()} rows")
except Exception as e:
    print(f"  SKIPPED: {e}")

print("\n=== Materialization complete ===")
