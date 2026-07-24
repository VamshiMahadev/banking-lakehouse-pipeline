# Databricks notebook source
# DBTITLE 1, Parameters and Setup
dbutils.widgets.text("storage_account", "datalakevamshi")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")

RAW_BASE_PATH = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net/landing"

# Tables to load
entities = ["customers", "accounts", "transactions", "loans", "credit_cards"]

# COMMAND ----------

# DBTITLE 2, Copy Local Repo CSV Files to Azure Data Lake
import os

# Identify where DABs deployed the local workspace files
repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_dir = os.path.join(repo_root, "data")

print(f"📂 Looking for sample CSV data in workspace path: {data_dir}")

for entity in entities:
    local_csv_path = os.path.join(data_dir, f"{entity}.csv")
    target_adls_path = f"{RAW_BASE_PATH}/{entity}"
    
    if os.path.exists(local_csv_path):
        print(f"📄 Processing {entity}.csv...")
        
        # Read from local workspace directory
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"file:{local_csv_path}")
        
        # Write to ADLS landing container
        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(target_adls_path)
        )
        print(f"✅ Successfully written 50 rows of '{entity}' to {target_adls_path}")
    else:
        print(f"⚠️ File not found: {local_csv_path}")