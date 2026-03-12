# --------------------------------------------------------------------------
# SILVER LAYER — Validated & Cleaned EMR Data
# --------------------------------------------------------------------------
# Replaces: Manual notebook transformations with ad-hoc quality checks
# SDP advantage: Built-in expectations (data quality), automatic lineage
# --------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ── Encounters ────────────────────────────────────────────────────────────

@dp.table(
    name="silver_encounters",
    comment="Validated patient encounters — quality-checked and typed",
    cluster_by=["facility_code", "admit_date"],
)
@dp.expect_or_drop("valid_encounter_id", "encounter_id IS NOT NULL")
@dp.expect_or_drop("valid_patient_id", "patient_id IS NOT NULL")
@dp.expect_or_drop("valid_admit_date", "admit_date IS NOT NULL")
@dp.expect("valid_discharge_order", "discharge_date IS NULL OR discharge_date >= admit_date")
def silver_encounters():
    """
    BEFORE (manual notebook):
        df = spark.table("bronze.encounters")
        df = df.filter(col("encounter_id").isNotNull())
        df = df.filter(col("patient_id").isNotNull())
        # No systematic quality tracking — bad rows silently dropped

    AFTER (SDP expectations):
        Quality rules are DECLARATIVE. Failed expectations are tracked
        in the pipeline event log with counts and percentages.
        Visible in the pipeline UI — no custom logging needed.
    """
    return (
        spark.readStream.table("bronze_encounters")
        .filter(F.col("cdc_operation") != "D")  # Filter deletes for this layer
        .withColumn("length_of_stay_hours",
            F.when(F.col("discharge_date").isNotNull(),
                   F.round((F.col("discharge_date").cast("long") - F.col("admit_date").cast("long")) / 3600, 1))
        )
        .withColumn("is_emergency",
            F.col("encounter_type") == "EMERGENCY"
        )
        .drop("_rescued_data")
    )


# ── Diagnoses ─────────────────────────────────────────────────────────────

@dp.table(
    name="silver_diagnoses",
    comment="Validated diagnosis records with ICD-10 code validation",
    cluster_by=["diagnosis_code"],
)
@dp.expect_or_drop("valid_diagnosis_id", "diagnosis_id IS NOT NULL")
@dp.expect_or_drop("valid_encounter_id", "encounter_id IS NOT NULL")
@dp.expect("valid_icd10_format", "diagnosis_code RLIKE '^[A-Z][0-9]{2}(\\\\.[0-9A-Z]{1,4})?$'")
def silver_diagnoses():
    return (
        spark.readStream.table("bronze_diagnoses")
        .filter(F.col("cdc_operation") != "D")
        .filter(F.col("is_active") == True)
        .drop("_rescued_data")
    )


# ── Procedures ────────────────────────────────────────────────────────────

@dp.table(
    name="silver_procedures",
    comment="Validated procedure records",
    cluster_by=["procedure_code", "procedure_date"],
)
@dp.expect_or_drop("valid_procedure_id", "procedure_id IS NOT NULL")
@dp.expect_or_drop("valid_encounter_id", "encounter_id IS NOT NULL")
@dp.expect("valid_status", "procedure_status IN ('COMPLETED', 'SCHEDULED', 'CANCELLED')")
def silver_procedures():
    return (
        spark.readStream.table("bronze_procedures")
        .filter(F.col("cdc_operation") != "D")
        .drop("_rescued_data")
    )
