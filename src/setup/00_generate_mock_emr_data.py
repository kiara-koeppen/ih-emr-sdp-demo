# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Mock Epic EMR Data for SDP Demo
# MAGIC
# MAGIC This notebook creates realistic mock data that mimics Intermountain Health's
# MAGIC Epic EMR CDC pipeline pattern: parquet files landing in cloud storage,
# MAGIC containing patient encounters, diagnoses, and procedures.
# MAGIC
# MAGIC **Run this once before running the SDP pipeline.**

# COMMAND ----------

CATALOG = "hcsc_agents_demo"
SCHEMA = "emr_sdp_demo"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_emr_data"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_emr_data
""")
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.pipeline_metadata
""")

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Volume: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Patient Encounters (CTRACK CDC pattern)
# MAGIC Simulates CDC files with operation type column, matching IH's CTRACK format.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

# --- Patient encounters (the core Epic EMR table) ---
num_patients = 500
num_encounters = 2000

encounter_schema = StructType([
    StructField("encounter_id", StringType()),
    StructField("patient_id", StringType()),
    StructField("encounter_type", StringType()),
    StructField("facility_code", StringType()),
    StructField("department", StringType()),
    StructField("admit_date", TimestampType()),
    StructField("discharge_date", TimestampType()),
    StructField("attending_provider_id", StringType()),
    StructField("primary_diagnosis_code", StringType()),
    StructField("encounter_status", StringType()),
    StructField("cdc_operation", StringType()),
    StructField("cdc_timestamp", TimestampType()),
])

encounter_types = ["INPATIENT", "OUTPATIENT", "EMERGENCY", "OBSERVATION", "TELEHEALTH"]
facilities = ["IHC-SLC", "IHC-PROVO", "IHC-OGDEN", "IHC-LOGAN", "IHC-STGEORGE", "IHC-MURRAY"]
departments = ["CARDIOLOGY", "ORTHOPEDICS", "ONCOLOGY", "PEDIATRICS", "NEUROLOGY", "INTERNAL_MED", "EMERGENCY"]
statuses = ["ACTIVE", "DISCHARGED", "TRANSFERRED", "CANCELLED"]
icd10_codes = [
    "I25.10", "J44.1", "E11.9", "M54.5", "I10", "J06.9", "K21.0",
    "N39.0", "R07.9", "Z23", "F32.9", "G43.909", "M79.3", "R51.9"
]

rows = []
base_time = datetime(2026, 3, 1, 6, 0, 0)

for i in range(num_encounters):
    enc_id = f"ENC-{100000 + i}"
    pat_id = f"PAT-{10000 + random.randint(0, num_patients - 1)}"
    admit = base_time + timedelta(hours=random.randint(0, 240), minutes=random.randint(0, 59))
    discharge = admit + timedelta(hours=random.randint(2, 168)) if random.random() > 0.3 else None
    cdc_ts = admit + timedelta(seconds=random.randint(0, 300))

    rows.append((
        enc_id, pat_id,
        random.choice(encounter_types),
        random.choice(facilities),
        random.choice(departments),
        admit, discharge,
        f"PROV-{random.randint(1000, 1050)}",
        random.choice(icd10_codes),
        random.choice(statuses),
        random.choice(["I", "I", "I", "U", "U"]),  # CDC ops: mostly inserts, some updates
        cdc_ts,
    ))

encounters_df = spark.createDataFrame(rows, schema=encounter_schema)

# Write as parquet files in batches (mimicking CDC file drops)
batch_size = 500
for batch_num, start in enumerate(range(0, num_encounters, batch_size)):
    batch_df = encounters_df.limit(batch_size).offset(start) if hasattr(encounters_df, 'offset') else (
        encounters_df.withColumn("_rn", F.monotonically_increasing_id())
        .filter((F.col("_rn") >= start) & (F.col("_rn") < start + batch_size))
        .drop("_rn")
    )
    path = f"{VOLUME_PATH}/encounters/batch_{batch_num:03d}"
    batch_df = encounters_df.withColumn("_rn", F.monotonically_increasing_id()) \
        .filter((F.col("_rn") >= start) & (F.col("_rn") < start + batch_size)) \
        .drop("_rn")
    batch_df.write.mode("overwrite").parquet(path)
    print(f"  Wrote batch {batch_num} -> {path} ({batch_df.count()} rows)")

print(f"\nEncounters written to {VOLUME_PATH}/encounters/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Diagnoses

# COMMAND ----------

diagnosis_rows = []
for i in range(3000):
    enc_id = f"ENC-{100000 + random.randint(0, num_encounters - 1)}"
    dx_ts = base_time + timedelta(hours=random.randint(0, 240))
    diagnosis_rows.append((
        f"DX-{200000 + i}",
        enc_id,
        f"PAT-{10000 + random.randint(0, num_patients - 1)}",
        random.choice(icd10_codes),
        random.choice(["ICD-10-CM", "ICD-10-PCS"]),
        random.choice(["PRIMARY", "SECONDARY", "ADMITTING"]),
        random.choice([True, True, True, False]),  # is_active
        random.choice(["I", "I", "U"]),
        dx_ts,
    ))

diagnosis_schema = StructType([
    StructField("diagnosis_id", StringType()),
    StructField("encounter_id", StringType()),
    StructField("patient_id", StringType()),
    StructField("diagnosis_code", StringType()),
    StructField("code_system", StringType()),
    StructField("diagnosis_rank", StringType()),
    StructField("is_active", BooleanType()),
    StructField("cdc_operation", StringType()),
    StructField("cdc_timestamp", TimestampType()),
])

diagnoses_df = spark.createDataFrame(diagnosis_rows, schema=diagnosis_schema)
diagnoses_df.write.mode("overwrite").parquet(f"{VOLUME_PATH}/diagnoses/")
print(f"Diagnoses: {diagnoses_df.count()} rows -> {VOLUME_PATH}/diagnoses/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Procedures

# COMMAND ----------

cpt_codes = ["99213", "99214", "99232", "99233", "36415", "71046", "93000", "80053", "85025", "43239"]

procedure_rows = []
for i in range(1500):
    enc_id = f"ENC-{100000 + random.randint(0, num_encounters - 1)}"
    proc_ts = base_time + timedelta(hours=random.randint(0, 240))
    procedure_rows.append((
        f"PROC-{300000 + i}",
        enc_id,
        f"PAT-{10000 + random.randint(0, num_patients - 1)}",
        random.choice(cpt_codes),
        "CPT",
        random.choice(facilities),
        f"PROV-{random.randint(1000, 1050)}",
        proc_ts,
        random.choice(["COMPLETED", "COMPLETED", "COMPLETED", "SCHEDULED", "CANCELLED"]),
        random.choice(["I", "I", "U"]),
        proc_ts,
    ))

procedure_schema = StructType([
    StructField("procedure_id", StringType()),
    StructField("encounter_id", StringType()),
    StructField("patient_id", StringType()),
    StructField("procedure_code", StringType()),
    StructField("code_system", StringType()),
    StructField("facility_code", StringType()),
    StructField("performing_provider_id", StringType()),
    StructField("procedure_date", TimestampType()),
    StructField("procedure_status", StringType()),
    StructField("cdc_operation", StringType()),
    StructField("cdc_timestamp", TimestampType()),
])

procedures_df = spark.createDataFrame(procedure_rows, schema=procedure_schema)
procedures_df.write.mode("overwrite").parquet(f"{VOLUME_PATH}/procedures/")
print(f"Procedures: {procedures_df.count()} rows -> {VOLUME_PATH}/procedures/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("Mock EMR Data Generation Complete")
print("=" * 60)
print(f"  Encounters:  {num_encounters} rows in {VOLUME_PATH}/encounters/")
print(f"  Diagnoses:   {len(diagnosis_rows)} rows in {VOLUME_PATH}/diagnoses/")
print(f"  Procedures:  {len(procedure_rows)} rows in {VOLUME_PATH}/procedures/")
print()
print("Next step: Run the SDP pipeline to process this data")
print("  databricks bundle deploy && databricks bundle run emr_sdp_pipeline")
