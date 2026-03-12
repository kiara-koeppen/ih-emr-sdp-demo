# --------------------------------------------------------------------------
# GOLD LAYER — Deduplicated Current-State Tables via AUTO CDC
# --------------------------------------------------------------------------
# Replaces: Manual MERGE (SCD Type 1) logic in notebooks
# SDP advantage: AUTO CDC handles dedup, ordering, and late data automatically
# --------------------------------------------------------------------------
#
# THIS IS THE KEY DEMO POINT FOR MOUNIKA:
#
# BEFORE (her current manual MERGE pattern):
#     df_updates = spark.table("silver.encounters")
#     df_target = spark.table("gold.encounters_current")
#     df_target.alias("t").merge(
#         df_updates.alias("s"),
#         "t.encounter_id = s.encounter_id"
#     ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
#
# AFTER (SDP AUTO CDC — this code):
#     Just declare keys + sequence column.
#     SDP handles the MERGE logic, dedup, ordering, and late data.
#     No manual merge code. No checkpoint management. No race conditions.
# --------------------------------------------------------------------------

from pyspark import pipelines as dp


# ── Encounters: SCD Type 1 (current state only) ──────────────────────────

dp.create_streaming_table(
    name="gold_encounters_current",
    comment="Current-state encounters — deduplicated via AUTO CDC (replaces manual MERGE)",
    cluster_by=["facility_code", "admit_date"],
)

dp.create_auto_cdc_flow(
    target="gold_encounters_current",
    source="silver_encounters",
    keys=["encounter_id"],
    sequence_by="cdc_timestamp",
    stored_as_scd_type="1",  # SCD Type 1: in-place update, current state only
)


# ── Diagnoses: SCD Type 1 ────────────────────────────────────────────────

dp.create_streaming_table(
    name="gold_diagnoses_current",
    comment="Current-state diagnoses — deduplicated via AUTO CDC",
    cluster_by=["diagnosis_code"],
)

dp.create_auto_cdc_flow(
    target="gold_diagnoses_current",
    source="silver_diagnoses",
    keys=["diagnosis_id"],
    sequence_by="cdc_timestamp",
    stored_as_scd_type="1",
)


# ── Procedures: SCD Type 1 ───────────────────────────────────────────────

dp.create_streaming_table(
    name="gold_procedures_current",
    comment="Current-state procedures — deduplicated via AUTO CDC",
    cluster_by=["procedure_code", "procedure_date"],
)

dp.create_auto_cdc_flow(
    target="gold_procedures_current",
    source="silver_procedures",
    keys=["procedure_id"],
    sequence_by="cdc_timestamp",
    stored_as_scd_type="1",
)
