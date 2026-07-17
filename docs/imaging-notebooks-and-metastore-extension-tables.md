# Imaging notebooks and metastore extension tables

This document describes the Fabric notebooks owned by this toolkit and the lakehouse tables they create or consume. It also sketches where the new imaging metastore extension extraction should be inserted into the existing HDS imaging pipelines.

## Notebook inventory

| Notebook | Source/deploy script | Primary purpose | Source lakehouses | Target lakehouses |
|---|---|---|---|---|
| `materialize_reporting_tables` | `materialize_reporting.py`, deployed by `deploy-notebook.ps1` | Build report-ready flat tables for the Direct Lake Power BI imaging report. | Silver `healthcare1_msft_silver`; Gold OMOP `healthcare1_msft_gold_omop` | Reporting `healthcare1_reporting_gold` |
| `extract_imaging_metastore_extension` | `extract_imaging_metastore_extension.py`, deployed by `deploy-imaging-metastore-extension-notebook.ps1` | Incrementally extract the 29 configured DICOM metadata tags from Silver `ImagingMetastore` into one wide table named `ImagingMetastoreExtension`. | Silver `healthcare1_msft_silver`; Admin `healthcare1_msft_admin` | Silver `healthcare1_msft_silver`; Admin `healthcare1_msft_admin` |
| `reconcile_imaging_metastore_extension_patch_operations` | Not implemented yet | Future patch/delete reconciliation notebook for extension rows. | HDS patch/delete operation outputs; Silver extension tables | Silver extension tables; Admin run/metrics tables |

## `materialize_reporting_tables`

### What it does

This notebook prepares the existing imaging report model. It is batch/materialized reporting logic, not the source-of-truth DICOM metadata extension.

High-level steps:

1. Resolve workspace and lakehouse IDs dynamically with `notebookutils` and Fabric REST.
2. Resolve the OHIF viewer URL supplied by deployment.
3. Build `ImagingStudyReporting` from Silver `ImagingStudy`:
   - extracts canonical `StudyInstanceUid`;
   - extracts patient UUID from `subject_string`;
   - extracts modality from `series_string`;
   - deduplicates duplicated logical studies;
   - creates `ViewerUrl`.
4. Build `PatientReporting` from Silver `Patient`:
   - extracts first/last/full name from FHIR JSON;
   - computes age and age range;
   - adds imaging-only patients from selected DICOM patient tags when they are not present in Silver `Patient`.
5. Build `DicomFileReporting` from Silver `ImagingMetastore`:
   - projects file, study, series, SOP, path, source system, and source modified fields.
6. Build `PersonDemographicsReporting` from Gold OMOP `person` and `concept`:
   - resolves race and ethnicity names.

### Tables created in `healthcare1_reporting_gold`

| Table | Grain | Contents |
|---|---|---|
| `ImagingStudyReporting` | One row per canonical study | `StudyId`, `StudyDate`, `NumberOfSeries`, `NumberOfInstances`, `StudyInstanceUid`, `PatientUUID`, `Modality`, `ModalityName`, `StudyYear`, `ViewerUrl` |
| `PatientReporting` | One row per patient UUID | `PatientId`, `PatientUUID`, `FirstName`, `LastName`, `Gender`, `BirthDate`, `FullName`, `Age`, `AgeRange` |
| `DicomFileReporting` | One row per metastore file/instance row | `FileId`, `StudyInstanceUid`, `SeriesInstanceUid`, `SopInstanceUid`, `FilePath`, `SourceSystem`, `SourceModifiedAt` |
| `PersonDemographicsReporting` | One row per OMOP person source value | `PatientId`, `Race`, `Ethnicity` |

## `extract_imaging_metastore_extension`

### What it does

This notebook creates the customer-facing DICOM metadata extension product as one wide table named `ImagingMetastoreExtension`: one row per deduplicated `ImagingMetastore.id`, with source references and the 29 well-known DICOM attributes as columns.

High-level steps:

1. Read runtime parameters.
   - Supports explicit source-modified windows and default incremental mode.
   - Uses a parameter cell so Fabric `RunNotebook` can override defaults.
2. Resolve workspace, Silver lakehouse, and Admin lakehouse IDs dynamically.
3. Create or evolve Admin operational tables.
4. Seed and maintain `DicomTagDictionary` with the 29 HDS well-known DICOM tags.
5. Create an Admin run audit row before extraction.
6. Read active Silver `ImagingMetastore` rows for the explicit or incremental batch.
7. Guard batch size and null `sourceModifiedAt` rate.
8. Deduplicate source rows by `ImagingMetastore.id` with deterministic ordering before parsing tags. The chosen row prefers the latest `sourceModifiedAt`, then the row with the largest metadata payload, then stable UID/path/source-system ordering. Removed duplicates are reported as `sourceRowsDeduplicated` in Admin metrics.
9. Persist deduplicated candidate source rows to Admin run-scoped staging.
10. Build an `ImagingStudy` reference from structured JSON in `identifier_string` and join each metastore row to `ImagingStudy.id`.
11. Parse configured DICOM tags from structured metadata JSON directly into wide columns.
12. Persist staged wide rows to Admin run-scoped staging.
13. Create or replace/merge the Silver `ImagingMetastoreExtension` wide Delta table.
14. Validate source UID columns against extracted UID tag values.
15. Write Admin metrics, mark the run as succeeded, advance the control watermark, and clean staging.
16. Leave delete/patch reconciliation for the future `reconcile_imaging_metastore_extension_patch_operations` notebook.

### Runtime parameters

| Parameter | Meaning | Default |
|---|---|---|
| `BATCH_START_SOURCE_MODIFIED_AT` | Inclusive explicit batch start timestamp. Empty means automatic incremental mode. | empty |
| `BATCH_END_SOURCE_MODIFIED_AT` | Exclusive explicit batch end timestamp. Required when start is supplied. | empty |
| `MAX_SOURCE_ROWS_PER_BATCH` | Safety guard for source row count. | `50000000` |
| `WATERMARK_OVERLAP_HOURS` | Overlap applied to the last successful watermark in incremental mode. | `24` |
| `HASH_BUCKET_COUNT` | Bucket count for `sourceSystemHashBucket` partitioning. | `128` |
| `MAX_ENABLED_TAGS` | Maximum dictionary-enabled tag count allowed in one run. | `500` |

### Tables created in `healthcare1_msft_admin`

| Table | Grain | Contents |
|---|---|---|
| `DicomTagDictionary` | One row per configured DICOM tag | `dicomTag`, `dicomKeyword`, `canonicalColumnName`, expected VR, tag scope, well-known flag, PHI flag/category, enabled flag, timestamps. Seeded with 29 well-known tags and extensible for additional tags. |
| `ImagingMetastoreExtensionControl` | One row per pipeline/target | Last successful high watermark, last successful run ID, update timestamp. Controls incremental extraction. |
| `ImagingMetastoreExtensionRun` | One row per extraction run | Run IDs, workspace/lakehouse IDs, batch window, status, pre/post watermark, source/join/parse/merge counts, duration, and error details. |
| `ImagingMetastoreExtensionMetrics` | One row per metric | Source counts, deduplication counts, parse counts, extension merge counts, tag coverage metrics, UID validation metrics, and cleanup warnings. |
| `Files/_staging/extract_imaging_metastore_extension/{extractRunId}` | Run-scoped files | Delta staging for filtered source rows and staged wide rows. Removed after successful control watermark advance; retained on failure for debugging. |

### Tables created in `healthcare1_msft_silver`

| Table | Grain | Contents |
|---|---|---|
| `ImagingMetastoreExtension` | One row per deduplicated `ImagingMetastore.id` | Deterministic row ID, source references, joined `ImagingStudy.id`, the 29 well-known tag columns, `isActive`, `sourceRowHash`, extraction timestamp, and run ID. UID tag columns are suffixed as `studyInstanceUid_tag`, `seriesInstanceUid_tag`, and `sopInstanceUid_tag` so they do not overwrite source reference columns. Partitioned by `sourceModifiedDate` and `sourceSystemHashBucket`. |

## 29 well-known tag columns in `ImagingMetastoreExtension`

| Column | DICOM tag | Keyword | PHI classification |
|---|---:|---|---|
| `studyInstanceUid_tag` | `0020000D` | `StudyInstanceUID` | None |
| `patientName` | `00100010` | `PatientName` | DirectIdentifier |
| `patientSex` | `00100040` | `PatientSex` | QuasiIdentifier |
| `patientId` | `00100020` | `PatientID` | DirectIdentifier |
| `patientBirthDate` | `00100030` | `PatientBirthDate` | QuasiIdentifier |
| `accessionNumber` | `00080050` | `AccessionNumber` | DirectIdentifier |
| `referringPhysicianName` | `00080090` | `ReferringPhysicianName` | DirectIdentifier |
| `studyDate` | `00080020` | `StudyDate` | None |
| `studyDescription` | `00081030` | `StudyDescription` | None |
| `seriesInstanceUid_tag` | `0020000E` | `SeriesInstanceUID` | None |
| `modality` | `00080060` | `Modality` | None |
| `modalitiesInStudy` | `00080061` | `ModalitiesInStudy` | None |
| `performedProcedureStepStartDate` | `00400244` | `PerformedProcedureStepStartDate` | None |
| `manufacturerModelName` | `00081090` | `ManufacturerModelName` | None |
| `sopInstanceUid_tag` | `00080018` | `SOPInstanceUID` | None |
| `studyTime` | `00080030` | `StudyTime` | None |
| `timezoneOffsetFromUtc` | `00080201` | `TimezoneOffsetFromUTC` | None |
| `numberOfStudyRelatedSeries` | `00201206` | `NumberOfStudyRelatedSeries` | None |
| `numberOfStudyRelatedInstances` | `00201208` | `NumberOfStudyRelatedInstances` | None |
| `seriesNumber` | `00200011` | `SeriesNumber` | None |
| `seriesDescription` | `0008103E` | `SeriesDescription` | None |
| `numberOfSeriesRelatedInstances` | `00201209` | `NumberOfSeriesRelatedInstances` | None |
| `bodyPartExamined` | `00180015` | `BodyPartExamined` | None |
| `laterality` | `00200060` | `Laterality` | None |
| `seriesDate` | `00080021` | `SeriesDate` | None |
| `seriesTime` | `00080031` | `SeriesTime` | None |
| `sopClassUid` | `00080016` | `SOPClassUID` | None |
| `instanceNumber` | `00200013` | `InstanceNumber` | None |
| `documentTitle` | `00420010` | `DocumentTitle` | None |

## Pipeline injection recommendation

### Active DICOM ingestion pipeline

Pipeline observed in workspace `FUJIV_Fabric_Test`:

```text
healthcare1_msft_imaging_with_clinical_foundation_ingestion
raw_process_movement
  -> imaging_dicom_extract_bronze_ingestion
  -> imaging_bronze_silver_metastore_transformation
  -> imaging_dicom_fhir_conversion
  -> fhir_ndjson_bronze_ingestion
  -> bronze_silver_flatten
```

Recommended injection point for `extract_imaging_metastore_extension`:

```text
bronze_silver_flatten (Succeeded)
  -> extract_imaging_metastore_extension
```

Reason: the extension notebook requires both Silver `ImagingMetastore` and Silver `ImagingStudy.id`. `ImagingMetastore` is available after `imaging_bronze_silver_metastore_transformation`; `ImagingStudy` is not reliably available until `imaging_dicom_fhir_conversion`, `fhir_ndjson_bronze_ingestion`, and `bronze_silver_flatten` complete.

### Active patch pipeline

Pipeline observed in workspace `FUJIV_Fabric_Test`:

```text
healthcare1_msft_imaging_patch_with_clinical_foundation_ingestion
raw_process_movement
  -> imaging_patch_bronze_ingestion
  -> imaging_bronze_silver_metastore_transformation
  -> imaging_dicom_fhir_conversion
  -> fhir_ndjson_bronze_ingestion
  -> bronze_silver_flatten
```

Recommended v1 injection point:

```text
bronze_silver_flatten (Succeeded)
  -> extract_imaging_metastore_extension
```

This keeps active incremental extraction current after patch-driven Silver/FHIR flattening, without attempting delete/patch reconciliation in this notebook.

### Future patch/delete reconciliation

Add `reconcile_imaging_metastore_extension_patch_operations` as a separate activity after the patch operation outputs are available and after Silver flattening has completed:

```text
bronze_silver_flatten (Succeeded)
  -> reconcile_imaging_metastore_extension_patch_operations
```

That future notebook should read HDS patch/delete operation outputs, set `isActive=false`, `sourceIsDeleted=true`, and `deletedDetectedAt` on affected extension rows, refresh affected pivot rows, and write separate Admin run/metrics records under pipeline name `reconcile_imaging_metastore_extension_patch_operations`.

## Validation notes from FUJIV_Fabric_Test

- `extract_imaging_metastore_extension` deployed successfully to Fabric notebook item `72170806-ee66-4fa2-987b-1974ed1ff79a`.
- A controlled one-row explicit batch ran successfully twice for `2026-06-25T17:43:50` to `2026-06-25T17:44:51`.
- Earlier validation of the prior design inserted long-form rows and a separate `ImagingMetastoreWellKnownTags` pivot. The current design intentionally abandons that shape: `dbo.ImagingMetastoreExtension` is now the single wide table and should expose all 29 expected attributes as columns.
- Validate the current design with `INFORMATION_SCHEMA.COLUMNS` and row-count/sample queries against `dbo.ImagingMetastoreExtension` only.
- A wider 15-minute test window exposed duplicate `ImagingMetastore.id` rows in the live source data. The notebook now deduplicates those rows deterministically before tag parsing and reports the removed-row count in metric `sourceRowsDeduplicated`.
