# --------------------------------------------------------------------------
# BRONZE LAYER — Raw EMR Data Ingestion
# --------------------------------------------------------------------------
# Replaces: AutoLoader notebooks with manual schema management
# SDP advantage: Declarative, auto-scaling, built-in checkpointing
# --------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

source_volume_path = spark.conf.get("source_volume_path")
schema_location_base = spark.conf.get("schema_location_base")


@dp.table(
    name="bronze_encounters",
    comment="Raw Epic encounter CDC data — ingested via Auto Loader from parquet files",
    cluster_by=["admit_date"],
)
def bronze_encounters():
    """
    Ingests CTRACK CDC encounter files as they land in storage.

    BEFORE (manual AutoLoader notebook):
        df = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "parquet")
              .option("cloudFiles.schemaLocation", "/some/path")
              .load("abfss://..."))
        df.writeStream.option("checkpointLocation", "/another/path")
          .trigger(availableNow=True).toTable("bronze.encounters")

    AFTER (SDP — this code):
        Just define the table. SDP handles checkpointing,
        schema tracking, and incremental processing automatically.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{schema_location_base}/bronze_encounters")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{source_volume_path}/encounters/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name="bronze_diagnoses",
    comment="Raw Epic diagnosis CDC data",
    cluster_by=["cdc_timestamp"],
)
def bronze_diagnoses():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{schema_location_base}/bronze_diagnoses")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{source_volume_path}/diagnoses/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name="bronze_procedures",
    comment="Raw Epic procedure CDC data",
    cluster_by=["procedure_date"],
)
def bronze_procedures():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{schema_location_base}/bronze_procedures")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{source_volume_path}/procedures/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
