# Fabric DICOM Cohorting Toolkit

An end-to-end toolkit for patient cohorting and medical imaging on [Microsoft Fabric Healthcare Data Solutions (HDS)](https://learn.microsoft.com/en-us/industry/healthcare/healthcare-data-solutions/overview). It combines a natural-language data agent, a Power BI imaging report (Direct Lake mode), and a zero-dependency DICOM viewer — all wired together so clinicians and researchers can identify patient cohorts, explore imaging studies, and view DICOM images directly from Fabric.

**Key capabilities:**

- **Ask questions in plain English** — the Fabric Data Agent translates natural-language cohorting queries into SQL across FHIR R4 (silver) and OMOP CDM v5.4 (gold) lakehouses
- **Interactive imaging dashboard** — Power BI report deployed via script (no Power BI Desktop required), using **Direct Lake** mode for near-real-time data access without Import or OAuth credentials
- **Pre-materialized reporting tables** — PySpark notebook extracts patient demographics from DICOM metadata, parses FHIR JSON, and writes clean Delta tables to a dedicated reporting lakehouse
- **Just-in-time DICOM viewer** — OHIF Viewer backed by a lightweight DICOMweb proxy that fetches `.dcm.zip` files on-demand from OneLake
- **Fully scripted deployment** — `Deploy-ImagingReport.ps1` auto-discovers SQL endpoints, patches TMDL, and deploys the semantic model + report via Fabric REST API

## Overview

| Component | Purpose |
|-----------|---------|
| **Cohorting Data Agent** | Fabric Data Agent that answers natural-language questions about patient cohorts using SQL over FHIR R4 (silver) and OMOP CDM v5.4 (gold) databases |
| **Materialize Notebook** | PySpark notebook that reads from Silver + Gold OMOP lakehouses, extracts demographics from DICOM metadata, and writes flat reporting tables |
| **Imaging Report** (.pbip) | Power BI report (Direct Lake) with demographic slicers, patient tables, and clickable DICOM viewer links — deployed headlessly via REST API |
| **DICOM Viewer** | OHIF Viewer + DICOMweb proxy deployed to Azure, serving DICOM images just-in-time from OneLake |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Microsoft Fabric Workspace                │
│                                                              │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────┐ │
│  │ Silver LH    │   │ Gold OMOP LH │   │ Bronze LH        │ │
│  │ • Patient    │   │ • person     │   │ • DICOM files    │ │
│  │ • ImagingStudy│  │ • concept    │   │   (.dcm.zip)     │ │
│  │ • ImagingMeta│   │              │   │                  │ │
│  └──────┬───────┘   └──────┬───────┘   └────────┬─────────┘ │
│         │                  │                     │           │
│         └──────────┬───────┘                     │           │
│                    ▼                             │           │
│  ┌─────────────────────────────┐                 │           │
│  │ materialize_reporting.py    │                 │           │
│  │ (PySpark Notebook)          │                 │           │
│  │ • Parse FHIR JSON           │                 │           │
│  │ • Extract DICOM patient tags│                 │           │
│  │ • Pre-compute ViewerUrl     │                 │           │
│  └──────────┬──────────────────┘                 │           │
│             ▼                                    │           │
│  ┌─────────────────────────────┐                 │           │
│  │ healthcare1_reporting_gold  │                 │           │
│  │ (Reporting Lakehouse)       │                 │           │
│  │ • PatientReporting          │                 │           │
│  │ • ImagingStudyReporting     │                 │           │
│  │ • DicomFileReporting        │                 │           │
│  │ • PersonDemographicsReporting│                │           │
│  └──────────┬──────────────────┘                 │           │
│             ▼                                    │           │
│  ┌─────────────────────────────┐                 │           │
│  │ ImagingReport               │                 │           │
│  │ (Semantic Model + Report)   │                 │           │
│  │ Mode: Direct Lake (1604)    │                 │           │
│  │ No Import / No OAuth needed │                 │           │
│  └─────────────────────────────┘                 │           │
│                                                  │           │
└──────────────────────────────────────────────────┼───────────┘
                                                   │
                              ┌────────────────────┘
                              ▼
                ┌──────────────────────────┐
                │ Azure (DICOM Viewer)     │
                │ • Container App (proxy)  │
                │ • Static Web App (OHIF)  │
                │ • Reads from OneLake JIT │
                └──────────────────────────┘
```

## Prerequisites

### Fabric Healthcare Data Solutions

A deployed [Microsoft Fabric HDS](https://learn.microsoft.com/en-us/industry/healthcare/healthcare-data-solutions/overview) environment with:

- **Silver Lakehouse** — FHIR R4 resources: `Patient`, `ImagingStudy`, `ImagingMetastore`, `Condition`, etc.
- **Gold OMOP Lakehouse** — OMOP CDM v5.4: `person`, `concept`, `condition_occurrence`, etc.
- **Bronze Lakehouse** — Raw DICOM `.dcm.zip` files in OneLake (via shortcut from ADLS Gen2)

### Azure Subscription (for DICOM Viewer)

- Azure CLI (`az`) authenticated
- Contributor access to create: Container App, Static Web App, Container Registry
- Node.js 18+ and Yarn (for OHIF build)

## Project Structure

```
FabricDicomCohortingToolkit/
├── Deploy-ImagingReport.ps1            # Headless semantic model + report deployment
├── deploy-notebook.ps1                 # Deploy materialize notebook to Fabric
├── materialize_reporting.py            # PySpark: build reporting tables from Silver + DICOM metadata
├── data-agent-instructions.md          # Fabric Data Agent instruction set
├── fewshots-silver-fhir.json           # 20 few-shot examples for FHIR silver queries
├── fewshots-gold-omop.json             # 15 few-shot examples for OMOP gold queries
│
├── ImagingReport.pbip                  # Power BI Project root
├── ImagingReport.SemanticModel/        # Semantic model (TMDL — Direct Lake)
│   └── definition/
│       ├── model.tmdl                  # Model with expression refs
│       ├── database.tmdl              # compatibilityLevel: 1604 (Direct Lake)
│       ├── expressions.tmdl           # ReportingSource expression (Sql.Database)
│       ├── relationships.tmdl         # 3 relationships (source columns only)
│       ├── cultures/en-US.tmdl
│       └── tables/
│           ├── Patient.tmdl            # → PatientReporting entity (Direct Lake)
│           ├── ImagingStudy.tmdl       # → ImagingStudyReporting entity (Direct Lake)
│           ├── DicomFile.tmdl          # → DicomFileReporting entity (Direct Lake)
│           ├── PersonDemographics.tmdl # → PersonDemographicsReporting entity (Direct Lake)
│           └── _Measures.tmdl          # DAX measures (counts, averages)
│
├── ImagingReport.Report/               # Report definition (PBIR)
│   └── definition/
│       └── pages/
│           ├── imaging_overview_page01/  # Overview: slicers, KPIs, patient table, charts
│           └── patient_images_page02/    # Patient detail: study table, DICOM files, viewer links
│
└── dicom-viewer/                       # DICOM Viewer deployment
    ├── Deploy-DicomViewer.ps1          # One-command deployment (workspace-aware, idempotent)
    ├── build_index.py                  # Build study index from Fabric ImagingMetastore
    ├── infra/main.bicep                # ACR + Container App + Static Web App
    ├── proxy/
    │   ├── app.py                      # DICOMweb proxy (Flask) — JIT fetch from OneLake
    │   ├── Dockerfile
    │   └── dicom_index.json            # Study index (generated by build_index.py)
    └── ohif/
        ├── app-config.js               # OHIF configuration template
        └── staticwebapp.config.json
```

## Deployment

**Important:** Deploy in this order — the DICOM viewer must be deployed before the notebook so the viewer URL flows into the reporting data.

### Step 1: Create the Reporting Lakehouse

The reporting lakehouse (`healthcare1_reporting_gold`) holds pre-materialized Delta tables consumed by the Direct Lake semantic model. Create it via the Fabric portal or API.

### Step 2: Deploy the DICOM Viewer

```powershell
cd dicom-viewer
.\Deploy-DicomViewer.ps1 -ResourceGroup rg-hds-dicom-viewer -FabricWorkspaceName "med-device-rti-hds"
```

This deploys the OHIF Viewer (Static Web App) + DICOMweb proxy (Container App), rebuilds the DICOM index, and grants the proxy's managed identity workspace access. The viewer URL is needed by Step 3.

### Step 3: Run the Materialization Notebook

```powershell
.\deploy-notebook.ps1 -FabricWorkspaceName "med-device-rti-hds"
```

The deploy script:
1. **Auto-discovers** the OHIF viewer URL from Azure (or `.deployment-state.json`)
2. **Patches** the URL into the notebook code before uploading
3. Creates and runs the notebook in Fabric

The notebook itself:
- **No hardcoded IDs** — resolves workspace and lakehouse IDs dynamically via `notebookutils.fabric.resolve_workspace_id()` and the Fabric REST API
- Reads `Patient` from Silver — extracts names from FHIR `name_string` JSON
- Reads `ImagingStudy` from Silver — extracts `StudyInstanceUid`, `PatientUUID`, `Modality`
- Reads `ImagingMetastore` from Silver — extracts patient demographics from **DICOM metadata tags** (00100010 PatientName, 00100020 PatientID, 00100030 BirthDate, 00100040 Sex)
- Unions Silver FHIR patients with DICOM-only patients
- Reads `person` + `concept` from Gold OMOP — pre-joins race/ethnicity
- Pre-computes `ViewerUrl` using the discovered OHIF base URL
- Writes 4 flat Delta tables to `healthcare1_reporting_gold`:

| Reporting Table | Source | Key Columns |
|----------------|--------|-------------|
| `PatientReporting` | Silver Patient + DICOM metadata | PatientId, PatientUUID, FullName, Gender, Age, AgeRange |
| `ImagingStudyReporting` | Silver ImagingStudy | StudyId, StudyInstanceUid, PatientUUID, Modality, ModalityName, ViewerUrl |
| `DicomFileReporting` | Silver ImagingMetastore | FileId, StudyInstanceUid, SeriesInstanceUid, SopInstanceUid |
| `PersonDemographicsReporting` | Gold OMOP person + concept | PatientId, Race, Ethnicity |

### Step 4: Deploy the Power BI Report

```powershell
.\Deploy-ImagingReport.ps1 -FabricWorkspaceName "med-device-rti-hds"
```

This script:
1. Discovers the `healthcare1_reporting_gold` lakehouse SQL endpoint
2. Patches the `ReportingSource` expression with the real server/database
3. Uploads the TMDL definition as a Direct Lake semantic model
4. Uploads the PBIR report definition with `byConnection` binding
5. Takes ownership of the semantic model

**No Power BI Desktop required. No OAuth credentials needed** — Direct Lake uses workspace identity.

### Rebuilding the DICOM Index

If the ImagingMetastore data changes, rebuild the index:

```powershell
cd dicom-viewer
python build_index.py --output proxy/dicom_index.json \
    --server "<sql-endpoint>.datawarehouse.fabric.microsoft.com" \
    --database "healthcare1_msft_silver"
# Rebuild + push the container image
az acr build --registry <acr-name> --image hds-dicom-proxy:latest proxy/
# Restart the container app
az containerapp update --name hds-dicom-proxy --resource-group <rg> --image <acr>.azurecr.io/hds-dicom-proxy:latest
```

## Semantic Model Details

The semantic model uses **Direct Lake** mode (compatibility level 1604), which reads Delta tables directly from OneLake without importing data. This eliminates the need for:
- OAuth credentials for `Sql.Database()` connections
- Scheduled refresh
- Data import latency

**Key design decisions:**
- All tables point to `healthcare1_reporting_gold` via a single `ReportingSource` expression (Direct Lake requires all entity partitions to use the same data source)
- No DAX calculated columns (Direct Lake prohibits them) — all derived columns are pre-computed in the materialization notebook
- Relationships use only source columns (Direct Lake cannot reference calculated columns in relationships)
- The `_Measures` table contains DAX measures only (referenced via a dummy entity partition on `PatientReporting`)

### Tables

| Table | Entity | Source Columns |
|-------|--------|---------------|
| Patient | PatientReporting | PatientId, PatientUUID, FirstName, LastName, Gender, BirthDate, FullName, Age, AgeRange |
| ImagingStudy | ImagingStudyReporting | StudyId, StudyInstanceUid, PatientUUID, Modality, ModalityName, StudyDate, NumberOfSeries, NumberOfInstances, StudyYear, ViewerUrl |
| DicomFile | DicomFileReporting | FileId, StudyInstanceUid, SeriesInstanceUid, SopInstanceUid, FilePath, SourceSystem, SourceModifiedAt |
| PersonDemographics | PersonDemographicsReporting | PatientId, Race, Ethnicity |
| _Measures | PatientReporting (dummy) | DAX measures: Total Patients, Total Studies, Total DICOM Files, Avg Age, etc. |

### Relationships

| From | To | Type |
|------|----|------|
| ImagingStudy.PatientUUID | Patient.PatientUUID | Many-to-one, bidirectional |
| DicomFile.StudyInstanceUid | ImagingStudy.StudyInstanceUid | Many-to-one, bidirectional |
| PersonDemographics.PatientId | Patient.PatientId | One-to-one, bidirectional |

## Component Details

### 1. Cohorting Data Agent

A Fabric Data Agent that translates natural-language patient cohorting questions into SQL. Configured with:

- **Instruction set** (`data-agent-instructions.md`) — covering silver-first query architecture, FHIR JSON patterns, and PII rules
- **Few-shot examples** — separate JSON files for silver FHIR (20 examples) and gold OMOP (15 examples)

**Key rules:**
- Silver-first: Always query FHIR tables first; only use gold for race/ethnicity concepts
- No cross-database JOINs (Fabric limitation)
- Never return PII (names/addresses/SSNs); use SHA-256 hashed `Patient.id`

**Setup:** Upload the three files to your Fabric Data Agent configuration in the Fabric portal.

### 2. Materialization Notebook

`materialize_reporting.py` is the critical data preparation step. It solves several challenges:

- **FHIR JSON parsing** — Silver Lakehouse stores complex FHIR types as `_string` columns (e.g., `name_string`, `subject_string`). PySpark regex extracts individual fields.
- **DICOM patient extraction** — When DICOM patients don't exist in the Silver Patient table (separate identifier systems from HDS ingestion), the notebook extracts demographics from DICOM metadata tags in `ImagingMetastore.metadata_string`: PatientName (00100010), PatientID (00100020), BirthDate (00100030), Sex (00100040).
- **Cross-lakehouse joins** — Direct Lake requires all tables from the same data source. The notebook pre-joins person + concept from Gold OMOP so the semantic model only needs one expression source.
- **ViewerUrl computation** — Pre-computes `{OHIF_BASE_URL}?StudyInstanceUIDs={uid}` so the report doesn't need DAX calculated columns.

### 3. DICOM Viewer

Open-source DICOM viewing stack — no AHDS dependency, no pre-loading.

| Component | Technology | Azure Service |
|-----------|-----------|---------------|
| Viewer UI | [OHIF Viewer v3](https://ohif.org) (MIT) | Static Web Apps |
| DICOMweb Proxy | Python/Flask + pydicom | Container Apps |

**JIT flow:** ViewerUrl click in Power BI → OHIF opens → requests DICOMweb metadata/frames → proxy fetches `.dcm.zip` from OneLake on-demand → parses with pydicom → returns DICOM metadata and pixel data → OHIF renders.

**RBAC requirement:** The proxy Container App's managed identity needs **Contributor** role on the Fabric workspace to read files from OneLake (Bronze Lakehouse).

## License

See individual component licenses:
- OHIF Viewer: MIT
- pydicom: MIT
- Fabric Data Agent instructions and report: Provided as-is
