# IH SDP Demo Script

**Date:** Friday, March 14, 2026 | 12:30 - 1:30 PM ET
**Presenter:** Kiara Koeppen (Databricks SE)
**Audience:** Mounika Sirasanagandla (IH, Data Engineer)
**Also on call:** Emanuele (Databricks SA), possibly Marcin (Databricks)

---

## Agenda (60 min)

| Time | Section | Duration |
|------|---------|----------|
| 12:30 | Context setting & Mounika's current state | 5 min |
| 12:35 | Bronze: AutoLoader ingestion | 10 min |
| 12:45 | Silver: Data quality expectations | 10 min |
| 12:55 | Gold: AUTO CDC (the big one) | 15 min |
| 13:10 | Gold reporting: materialized views | 5 min |
| 13:15 | Asset Bundles & config-driven architecture | 5 min |
| 13:20 | Next steps & discussion | 10 min |

---

## 1. Context Setting (5 min)

**Open the pipeline UI first** so it loads while you talk:
`https://adb-669602668219382.2.azuredatabricks.net/#joblist/pipelines/28b6f709-ace2-41bc-9811-152d15d2cd2b`

**Talking points:**
- "This builds on the SDP workshop from December and the example Emanuele shared last week"
- "We mocked up data that mirrors your Epic EMR CDC pattern -- encounters, diagnoses, procedures with I/U/D operations"
- "Every file has BEFORE/AFTER comments so you can see exactly what changes vs your current notebooks"

**Ask Mounika:**
> "Before we dive in -- has anything changed in your pipeline setup since we last talked? Still 150+ tables with the same MERGE pattern?"

---

## 2. Bronze: AutoLoader Ingestion (10 min)

**What to show:** Bronze notebook/file with `cloudFiles` source definition

**Talking points:**
- **BEFORE:** Individual AutoLoader jobs per table, manual checkpoint paths, manual schema management
- **AFTER:** Declarative `cloudFiles` in SDP -- checkpointing is automatic, schema evolution is built in
- "SDP manages the checkpoint location for you -- no more `/mnt/checkpoints/table_xyz` paths to track"
- "Schema evolution happens automatically -- if Epic adds a column to the parquet, it flows through"

**Ask Mounika:**
> "How are you handling schema changes today when Epic updates their extract format? Is that a manual process?"

> "Are your 150 tables all parquet from CTRACK, or do you have other formats too?"

---

## 3. Silver: Data Quality Expectations (10 min)

**What to show:** Silver layer with `@dp.expect_or_drop` and `@dp.expect` decorators

**Talking points:**
- **BEFORE:** Ad-hoc `filter(col("x").isNotNull())` scattered in notebooks, no visibility into how many rows fail
- **AFTER:** Declarative expectations that are tracked and visible in the pipeline UI
- `@dp.expect("valid_patient_id", "patient_id IS NOT NULL")` -- warns but keeps the row
- `@dp.expect_or_drop("valid_encounter", "encounter_id IS NOT NULL")` -- drops bad rows automatically
- **Show the pipeline UI** -- point out the data quality metrics panel: "You get row counts, pass/fail rates, all without writing any tracking code"

**Ask Mounika:**
> "What data quality issues do you see most often from the Epic extracts? Nulls, duplicates, invalid codes?"

> "Do you have any quality checks today that block downstream processing, or is it all best-effort?"

---

## 4. Gold: AUTO CDC -- The Main Event (15 min)

**This is the key demo point. Spend the most time here.**

**What to show:** Gold layer with `dp.create_auto_cdc_flow()` -- the AUTO CDC configuration

**Talking points:**
- **BEFORE (Mounika's current pattern):**
  - Manual MERGE INTO with match conditions
  - Separate handling for the DELETES path from CTRACK
  - Race conditions when multiple jobs hit the same target table
  - Custom logic to interpret `cdc_operation` column (I/U/D)
- **AFTER:**
  - `dp.create_auto_cdc_flow(target, source, keys, sequence_by, stored_as_scd_type="1")` -- one declaration
  - "SDP handles dedup, ordering, and late-arriving data automatically via `sequence_by`"
  - "No more separate DELETES path -- it all flows through a single stream"
  - "No race conditions because SDP manages the transaction ordering via the sequence column"
- **Emphasize:** "This single `create_auto_cdc_flow()` call replaces your entire manual MERGE notebook per table"

**Ask Mounika:**
> "Walk me through what happens today when you get a DELETE record from CTRACK -- is that a separate job?"

> "Have you hit merge conflicts or race conditions with concurrent jobs writing to the same table?"

> "For your SCD Type 1 -- are there any tables where you actually need Type 2 (history tracking)?"

---

## 5. Gold Reporting: Materialized Views (5 min)

**What to show:** Materialized views for facility summary, diagnosis distribution, daily admissions

**Talking points:**
- These are downstream aggregations that refresh automatically when upstream gold tables update
- No separate scheduling needed -- the pipeline DAG handles the dependency
- "If you have reporting tables or summary views today, they can become materialized views in the same pipeline"

**Ask Mounika:**
> "Do you have downstream reporting or analytics that depends on these gold tables today?"

---

## 6. Asset Bundles & Config-Driven Architecture (5 min)

**What to show:** `databricks.yml` with dev/prod targets (can show in IDE or just talk through)

**Talking points:**
- Everything deployed via Databricks Asset Bundles -- `databricks bundle deploy -t dev`
- Dev and prod targets with different catalogs, schemas, compute
- **Emanuele's config-driven plan:** A configuration table defines N pipelines x X tables -- add a row to onboard a new table, no code changes
- "For your 150+ tables, you wouldn't create 150 pipelines -- you'd have N concurrent pipelines each handling a batch of tables, all driven by config"

**Ask Mounika:**
> "How do you onboard a new table today? Is it copy-paste of an existing notebook?"

> "Are you using any CI/CD today, or is it manual deployment?"

---

## 7. Next Steps & Discussion (10 min)

**Proposed action items to discuss:**

1. **Mounika POC:** Pick 3-5 representative tables from the 150+ (ideally with different CDC patterns) and try SDP in a dev environment
2. **Config table design:** Emanuele to share the config-driven architecture pattern for scaling to 150+ tables
3. **Lakeflow Connect evaluation:** Could replace Azure Data Factory for the Azure Blob Storage ingestion layer (future discussion)
4. **Follow-up session:** Hands-on working session where Mounika converts one of her actual MERGE notebooks to SDP

**Ask Mounika:**
> "Which 3-5 tables would be good candidates for a POC? Ideally ones that are representative of the different patterns you have."

> "What would need to be true for you to feel confident replacing the manual MERGE with AUTO CDC?"

> "Is there anything in your current setup that you think might not map cleanly to SDP?"

---

## Quick Reference

- **Pipeline URL:** https://adb-669602668219382.2.azuredatabricks.net/#joblist/pipelines/28b6f709-ace2-41bc-9811-152d15d2cd2b
- **Workspace:** https://adb-669602668219382.2.azuredatabricks.net
- **Prior context:** December 2025 SDP workshop (CMS data, SQL syntax), March 6 Emanuele example
- **This demo's differentiator:** Python API + data that mimics Mounika's actual Epic EMR CDC pattern
