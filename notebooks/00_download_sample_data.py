# Databricks notebook source
# DBTITLE 1, Setup Storage Account & Landing Paths
import urllib.request
import os

# Get storage account name from widget (defaults to datalakevamshi)
dbutils.widgets.text("storage_account", "datalakevamshi")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")

# Base landing path in ADLS Gen2
RAW_BASE_PATH = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net/landing"

# Define online sample CSV source URLs for 5 banking/financial tables
sample_datasets = {
    "customers": "https://raw.githubusercontent.com/datasets/investor-flow-of-funds/master/data/weekly.csv",
    "accounts": "https://raw.githubusercontent.com/datablist/sample-csv-files/main/files/organizations/organizations-100.csv",
    "transactions": "https://raw.githubusercontent.com/datablist/sample-csv-files/main/files/customers/customers-100.csv",
    "loans": "https://raw.githubusercontent.com/plotly/datasets/master/finance-charts-apple.csv",
    "credit_cards": "https://raw.githubusercontent.com/datasets/gdp/master/data/gdp.csv"
}

# COMMAND ----------

# DBTITLE 2, Download Datasets Online & Write to ADLS Landing Directory
for table_name, url in sample_datasets.items():
    print(f"📥 Downloading sample data for '{table_name}' from: {url}")
    
    # Download file content locally on driver node
    temp_local_path = f"/tmp/{table_name}.csv"
    urllib.request.urlretrieve(url, temp_local_path)
    
    # Read using PySpark
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"file:{temp_local_path}")
    
    # Write as CSV to ADLS Landing directory
    destination_path = f"{RAW_BASE_PATH}/{table_name}"
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(destination_path)
    )
    
    # Clean up local temporary file
    if os.path.exists(temp_local_path):
        os.remove(temp_local_path)
        
    print(f"✅ Successfully loaded sample records to: {destination_path}\n")