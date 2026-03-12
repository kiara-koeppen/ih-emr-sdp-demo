# --------------------------------------------------------------------------
# GOLD REPORTING LAYER — Materialized Views for Healthcare Analytics
# --------------------------------------------------------------------------
# Replaces: Manual notebook queries re-run on every dashboard refresh
# SDP advantage: Materialized views are auto-refreshed by the pipeline,
#   always consistent with upstream CDC tables, and optimized for reads.
# --------------------------------------------------------------------------
#
# BEFORE (manual reporting notebooks):
#     # Analyst runs ad-hoc aggregation in a notebook every morning
#     df = spark.sql("""
#         SELECT facility_code, encounter_type,
#                COUNT(*) AS total_encounters,
#                AVG(length_of_stay_hours) AS avg_los
#         FROM gold.encounters_current
#         GROUP BY facility_code, encounter_type
#     """)
#     df.write.mode("overwrite").saveAsTable("reporting.facility_summary")
#     # Problem: stale data, no lineage, manual scheduling, overwrites lose history
#
# AFTER (SDP materialized views — this code):
#     Declare the aggregation once. The pipeline keeps it fresh automatically.
#     Full lineage from bronze -> silver -> gold CDC -> reporting MVs.
#     No manual scheduling. No stale dashboards. No orphaned tables.
# --------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# -- Facility Summary -------------------------------------------------------
# Key metric view for hospital operations: volume and avg LOS by facility
# Feeds the executive operations dashboard

@dp.materialized_view(
    name="gold_facility_summary",
    comment="Encounter volume and avg length of stay by facility and encounter type",
    cluster_by=["facility_code"],
)
def gold_facility_summary():
    """
    BEFORE (manual notebook):
        result = spark.sql("SELECT ... GROUP BY ...").toPandas()
        # Re-run manually before each meeting; often stale by the time execs see it

    AFTER (SDP materialized view):
        Always current. Pipeline refreshes automatically when upstream data changes.
    """
    return (
        spark.read.table("gold_encounters_current")
        .groupBy("facility_code", "encounter_type")
        .agg(
            F.count("*").alias("total_encounters"),
            F.avg("length_of_stay_hours").alias("avg_length_of_stay_hours"),
            F.countDistinct("patient_id").alias("unique_patients"),
            F.sum(F.when(F.col("is_emergency"), 1).otherwise(0)).alias("emergency_count"),
            F.min("admit_date").alias("earliest_admit"),
            F.max("admit_date").alias("latest_admit"),
        )
    )


# -- Diagnosis Distribution -------------------------------------------------
# Top diagnosis codes by facility — critical for population health management
# and resource allocation planning

@dp.materialized_view(
    name="gold_diagnosis_distribution",
    comment="Diagnosis code frequency by facility — for population health reporting",
    cluster_by=["facility_code", "diagnosis_code"],
)
def gold_diagnosis_distribution():
    """
    BEFORE (manual notebook):
        # Analyst joins encounters + diagnoses, groups, filters top N manually
        # Different analysts use different logic — inconsistent reports

    AFTER (SDP materialized view):
        Single source of truth for diagnosis distribution.
        Joins are maintained by the pipeline — no drift between reports.
    """
    encounters = spark.read.table("gold_encounters_current")
    diagnoses = spark.read.table("gold_diagnoses_current")

    return (
        diagnoses
        .join(
            encounters.select("encounter_id", "facility_code", "encounter_type"),
            on="encounter_id",
            how="inner",
        )
        .groupBy("facility_code", "diagnosis_code", "code_system")
        .agg(
            F.count("*").alias("diagnosis_count"),
            F.countDistinct("encounter_id").alias("encounter_count"),
            F.countDistinct("patient_id").alias("patient_count"),
        )
    )


# -- Daily Admissions -------------------------------------------------------
# Daily admission trends by facility — capacity planning and surge detection
# Designed for time-series dashboards and alerting thresholds

@dp.materialized_view(
    name="gold_daily_admissions",
    comment="Daily admission counts by facility — for capacity planning and trend analysis",
    cluster_by=["facility_code", "admit_date_day"],
)
def gold_daily_admissions():
    """
    BEFORE (manual notebook):
        spark.sql("SELECT DATE(admit_date), facility_code, COUNT(*) ...")
        # Scheduled as a separate job, often out of sync with the main pipeline

    AFTER (SDP materialized view):
        Tied to the same pipeline as ingestion and CDC.
        Refreshed atomically — dashboard always matches the source data.
    """
    return (
        spark.read.table("gold_encounters_current")
        .withColumn("admit_date_day", F.to_date("admit_date"))
        .groupBy("facility_code", "admit_date_day")
        .agg(
            F.count("*").alias("total_admissions"),
            F.sum(F.when(F.col("is_emergency"), 1).otherwise(0)).alias("emergency_admissions"),
            F.sum(F.when(F.col("encounter_type") == "INPATIENT", 1).otherwise(0)).alias("inpatient_admissions"),
            F.sum(F.when(F.col("encounter_type") == "OUTPATIENT", 1).otherwise(0)).alias("outpatient_admissions"),
            F.avg("length_of_stay_hours").alias("avg_length_of_stay_hours"),
        )
    )
