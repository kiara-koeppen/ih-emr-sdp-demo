# IH EMR SDP Demo Pipeline

Spark Declarative Pipelines (SDP) demo for Intermountain Health, showing how to migrate a jobs-based ETL workflow (AutoLoader + manual MERGE) to a fully declarative pipeline using mock Epic EMR CDC data.

## Purpose

Intermountain Health currently runs 150+ tables through individual AutoLoader jobs with manual MERGE (SCD Type 1) logic for deduplication. This demo shows the same pattern implemented as a single SDP pipeline with:

- **Declarative AutoLoader ingestion** — no manual checkpoint or schema management
- **Built-in data quality expectations** — tracked and visible in the pipeline UI, replacing ad-hoc `filter(isNotNull)` patterns
- **AUTO CDC (SCD Type 1)** — replaces manual MERGE notebooks entirely with a single `create_auto_cdc_flow()` call
- **Materialized views** — auto-refreshing reporting aggregations tied to the same pipeline DAG

## Architecture

```
Epic EMR (CTRACK CDC)          Spark Declarative Pipeline
─────────────────────          ──────────────────────────

Parquet files with         ┌─────────────────────────────────────────────┐
I/U/D operations           │                                             │
    │                      │  Bronze (Streaming Tables)                  │
    ▼                      │  ├── bronze_encounters                     │
┌────────────┐             │  ├── bronze_diagnoses                      │
│ Cloud      │──AutoLoader─│  └── bronze_procedures                     │
│ Storage    │             │           │                                 │
│ (Volumes)  │             │           ▼                                 │
└────────────┘             │  Silver (Streaming Tables + Expectations)   │
                           │  ├── silver_encounters  (@expect_or_drop)  │
                           │  ├── silver_diagnoses   (ICD-10 validation)│
                           │  └── silver_procedures  (status validation)│
                           │           │                                 │
                           │           ▼                                 │
                           │  Gold CDC (AUTO CDC — SCD Type 1)          │
                           │  ├── gold_encounters_current               │
                           │  ├── gold_diagnoses_current                │
                           │  └── gold_procedures_current               │
                           │           │                                 │
                           │           ▼                                 │
                           │  Gold Reporting (Materialized Views)       │
                           │  ├── gold_facility_summary                 │
                           │  ├── gold_diagnosis_distribution           │
                           │  └── gold_daily_admissions                 │
                           └─────────────────────────────────────────────┘
```

**Key transformation at each layer:**

| Layer | What it does | SDP feature used |
|-------|-------------|------------------|
| Bronze | Raw ingestion from parquet CDC files | `@dp.table` + `cloudFiles` (Auto Loader) |
| Silver | Validate, filter deletes, add computed columns | `@dp.expect_or_drop`, `@dp.expect` |
| Gold CDC | Deduplicate to current state per entity | `dp.create_auto_cdc_flow()` with SCD Type 1 |
| Gold Reporting | Aggregate for dashboards and analytics | `@dp.materialized_view()` |

## Prerequisites

- **Databricks CLI** (v0.200+) with a configured profile
- **Databricks workspace** with Unity Catalog enabled
- **Catalog and schema** — the dev target uses `hcsc_agents_demo.emr_sdp_demo`
- **A running cluster** — needed to execute the mock data generator notebook

## File Structure

```
ih-emr-sdp-demo/
├── README.md                                   # This file
├── DEMO_SCRIPT.md                              # 60-min demo talking points and agenda
├── databricks.yml                              # Databricks Asset Bundle config (dev/prod)
└── src/
    ├── setup/
    │   └── 00_generate_mock_emr_data.py        # Notebook: generates mock Epic EMR data
    └── transformations/
        ├── 01_bronze_ingestion.py              # Bronze: AutoLoader ingestion (3 tables)
        ├── 02_silver_validated.py              # Silver: validation + expectations (3 tables)
        ├── 03_gold_auto_cdc.py                 # Gold: AUTO CDC SCD Type 1 (3 tables)
        └── 04_gold_reporting.py                # Gold: materialized views (3 views)
```

## Configuration

The `databricks.yml` defines two targets with these variables:

| Variable | Dev | Prod |
|----------|-----|------|
| `catalog` | `hcsc_agents_demo` | `production` |
| `schema` | `emr_sdp_demo` | `emr_pipeline` |
| `source_volume_path` | `/Volumes/hcsc_agents_demo/emr_sdp_demo/raw_emr_data` | `/Volumes/production/emr_pipeline/raw_emr_data` |
| `schema_location_base` | `/Volumes/hcsc_agents_demo/emr_sdp_demo/pipeline_metadata/schemas` | `/Volumes/production/emr_pipeline/pipeline_metadata/schemas` |

To use a different catalog/schema, update the variables in `databricks.yml` under the appropriate target.

## How to Deploy and Run

### 1. Generate mock data

Upload and run the setup notebook on an existing cluster:

```bash
# Upload the notebook
databricks workspace import \
  /Users/<your-email>/IH_SDP_Demo/00_generate_mock_emr_data \
  --file src/setup/00_generate_mock_emr_data.py \
  --format SOURCE --language PYTHON --overwrite

# Run it (replace CLUSTER_ID with your cluster)
databricks jobs submit --json '{
  "run_name": "Generate Mock EMR Data",
  "tasks": [{
    "task_key": "generate_data",
    "existing_cluster_id": "YOUR_CLUSTER_ID",
    "notebook_task": {
      "notebook_path": "/Users/<your-email>/IH_SDP_Demo/00_generate_mock_emr_data"
    }
  }]
}'
```

This creates ~6,500 rows of mock data (2,000 encounters, 3,000 diagnoses, 1,500 procedures) as parquet files in the source volume.

### 2. Deploy the pipeline

```bash
databricks bundle validate
databricks bundle deploy
```

### 3. Run the pipeline

```bash
databricks bundle run emr_sdp_pipeline
```

The pipeline processes all 12 tables/views in dependency order (bronze → silver → gold CDC → gold reporting) in about 30 seconds of execution time.

### 4. Deploy to production

```bash
databricks bundle deploy --target prod
databricks bundle run emr_sdp_pipeline --target prod
```

## Mock Data

The generator creates realistic healthcare data mimicking Intermountain Health's CTRACK CDC format:

- **Encounters**: patient visits with facility codes (IHC-SLC, IHC-PROVO, etc.), encounter types (INPATIENT, EMERGENCY, etc.), ICD-10 diagnosis codes, and CDC operation columns (I=Insert, U=Update, D=Delete)
- **Diagnoses**: linked to encounters with ICD-10-CM/PCS codes, diagnosis rank, and active status
- **Procedures**: linked to encounters with CPT codes, performing provider, and procedure status
