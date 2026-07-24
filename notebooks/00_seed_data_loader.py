# Databricks notebook source
# DBTITLE 1, Parameters and Setup
import os

dbutils.widgets.text("storage_account", "datalakevamshi")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")

RAW_BASE_PATH = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net/landing"

# Entities to load
entities = ["customers", "accounts", "transactions", "loans", "credit_cards"]

# COMMAND ----------

# DBTITLE 2, Copy Local Repo CSV Files to Azure Data Lake
# Fetch the current notebook path dynamically
try:
    notebook_path = dbutils.notebook.getContext().notebookPath().get()
except Exception:
    notebook_path = spark.conf.get("spark.databricks.notebook.path", "")

# Compute candidate paths where the 'data' folder might reside
# Notebook is in: .../files/notebooks/00_seed_data_loader.py
# Data folder is in: .../files/data/
notebook_dir = os.path.dirname(notebook_path)
parent_dir = os.path.dirname(notebook_dir)

candidate_paths = [
    os.path.join("/Workspace", parent_dir.lstrip("/"), "data"),
    "/Workspace/Shared/.bundle/banking_lakehouse_bundle/prod/files/data",
    "/Workspace/data"
]

data_dir = None
for path in candidate_paths:
    if os.path.exists(path):
        data_dir = path
        break

if not data_dir:
    raise FileNotFoundError(f"❌ Could not find 'data' directory in any of these locations: {candidate_paths}")

print(f"📂 Found sample CSV data at path: {data_dir}")

for entity in entities:
    local_csv_path = os.path.join(data_dir, f"{entity}.csv")
    target_adls_path = f"{RAW_BASE_PATH}/{entity}"
    
    if os.path.exists(local_csv_path):
        print(f"📄 Copying {entity}.csv to {target_adls_path}...")
        
        # Read local workspace CSV file
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"file:{local_csv_path}")
        
        # Overwrite to ADLS landing directory
        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(target_adls_path)
        )
        print(f"✅ Successfully written '{entity}' records to {target_adls_path}")
    else:
        print(f"⚠️ File not found at path: {local_csv_path}")