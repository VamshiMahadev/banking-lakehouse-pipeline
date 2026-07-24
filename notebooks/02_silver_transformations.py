# Databricks notebook source
# DBTITLE 1, Silver Layer - Data Cleaning & Delta Upserts
from pyspark.sql.functions import col, trim, lower, to_timestamp, cast, row_number
from pyspark.sql.window import Window

spark.sql("USE CATALOG banking_catalog;")
spark.sql("USE SCHEMA silver;")

# -------------------------------------------------------------
# 1. CUSTOMERS TABLE (MERGE Upsert & Deduplication)
# -------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS banking_catalog.silver.dim_customers (
    customer_id STRING NOT NULL,
    first_name STRING,
    last_name STRING,
    email STRING,
    kyc_status STRING,
    join_date DATE,
    updated_at TIMESTAMP,
    CONSTRAINT pk_customers PRIMARY KEY(customer_id)
) USING DELTA;
""")

df_cust_raw = spark.table("banking_catalog.bronze.bronze_customers")
window_spec = Window.partitionBy("customer_id").orderBy(col("_ingested_at").desc())

df_cust_clean = (
    df_cust_raw
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .select(
        trim(col("customer_id")).alias("customer_id"),
        trim(col("first_name")).alias("first_name"),
        trim(col("last_name")).alias("last_name"),
        lower(trim(col("email"))).alias("email"),
        trim(col("kyc_status")).alias("kyc_status"),
        col("join_date").cast("date").alias("join_date"),
        col("_ingested_at").alias("updated_at")
    )
)
df_cust_clean.createOrReplaceTempView("v_clean_customers")

spark.sql("""
MERGE INTO banking_catalog.silver.dim_customers AS target
USING v_clean_customers AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
""")

# -------------------------------------------------------------
# 2. ACCOUNTS TABLE
# -------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS banking_catalog.silver.dim_accounts (
    account_id STRING NOT NULL,
    customer_id STRING,
    account_type STRING,
    balance DECIMAL(18, 2),
    created_at TIMESTAMP,
    CONSTRAINT pk_accounts PRIMARY KEY(account_id),
    CONSTRAINT fk_accounts_customer FOREIGN KEY (customer_id) REFERENCES banking_catalog.silver.dim_customers(customer_id)
) USING DELTA;
""")

df_acc_clean = (
    spark.table("banking_catalog.bronze.bronze_accounts")
    .select(
        trim(col("account_id")).alias("account_id"),
        trim(col("customer_id")).alias("customer_id"),
        trim(col("account_type")).alias("account_type"),
        col("balance").cast("decimal(18,2)").alias("balance"),
        to_timestamp(col("created_at")).alias("created_at")
    )
)
df_acc_clean.createOrReplaceTempView("v_clean_accounts")

spark.sql("""
MERGE INTO banking_catalog.silver.dim_accounts AS target
USING v_clean_accounts AS source
ON target.account_id = source.account_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
""")

# -------------------------------------------------------------
# 3. TRANSACTIONS FACT TABLE
# -------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS banking_catalog.silver.fact_transactions (
    transaction_id STRING NOT NULL,
    account_id STRING,
    amount DECIMAL(18, 2),
    transaction_type STRING,
    txn_timestamp TIMESTAMP,
    CONSTRAINT pk_transactions PRIMARY KEY(transaction_id),
    CONSTRAINT fk_txn_account FOREIGN KEY (account_id) REFERENCES banking_catalog.silver.dim_accounts(account_id)
) USING DELTA
CLUSTER BY (account_id, txn_timestamp);
""")

df_txn_clean = (
    spark.table("banking_catalog.bronze.bronze_transactions")
    .select(
        trim(col("transaction_id")).alias("transaction_id"),
        trim(col("account_id")).alias("account_id"),
        col("amount").cast("decimal(18,2)").alias("amount"),
        trim(col("transaction_type")).alias("transaction_type"),
        to_timestamp(col("timestamp")).alias("txn_timestamp")
    )
)
df_txn_clean.createOrReplaceTempView("v_clean_transactions")

spark.sql("""
MERGE INTO banking_catalog.silver.fact_transactions AS target
USING v_clean_transactions AS source
ON target.transaction_id = source.transaction_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
""")