# Databricks notebook source
# DBTITLE 1, Retrieve Parameters from Widgets
from pyspark.sql.functions import current_timestamp, input_file_name

# Define parameter widget
dbutils.widgets.text("storage_account", "datalakevamshi")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")

# Configuration using dynamic storage account parameter
RAW_BASE_PATH = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net/landing"
CHECKPOINT_BASE = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net/checkpoints/bronze"

sources = ["customers", "accounts", "transactions", "loans", "credit_cards"]

# COMMAND ----------

# DBTITLE 2, Bronze Layer - Auto Loader Ingestion Execution
def ingest_to_bronze(source_name: str):
    source_dir = f"{RAW_BASE_PATH}/{source_name}"
    checkpoint_dir = f"{CHECKPOINT_BASE}/{source_name}"
    target_table = f"banking_catalog.bronze.bronze_{source_name}"
    
    df_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", f"{checkpoint_dir}/schema")
        .load(source_dir)
    )
    
    query = (
        df_stream
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_dir)
        .outputMode("append")
        .trigger(availableNow=True) # Batch execution mode
        .toTable(target_table)
    )
    
    query.awaitTermination()
    print(f"Successfully ingested {source_name} into {target_table}")

# Process all landing entities
for source in sources:
    ingest_to_bronze(source)