# Databricks notebook source
# DBTITLE 1, Setup Silver Layer Context
from pyspark.sql.functions import col, trim, lower, to_date, to_timestamp, row_number
from pyspark.sql.window import Window

spark.sql("USE CATALOG banking_catalog;")
spark.sql("USE SCHEMA silver;")

# COMMAND ----------

# DBTITLE 2, Create All Target Silver Tables First (Prevents Dependency Failures)
# 1. Customers Table
spark.sql("""
CREATE TABLE IF NOT EXISTS banking_catalog.silver.dim_customers (
    customer_id STRING NOT NULL,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone_number STRING,
    created_date DATE,
    updated_at TIMESTAMP,
    CONSTRAINT pk_customers PRIMARY KEY(customer_id)
) USING DELTA;
""")

# 2. Accounts Table (Notice: No FK constraint to avoid creation blocks)
spark.sql("""
CREATE TABLE IF NOT EXISTS banking_catalog.silver.dim_accounts (
    account_id STRING NOT NULL,
    customer_id STRING,
    account_type STRING,
    balance DECIMAL(18, 2),
    status STRING,
    opened_date DATE,
    CONSTRAINT pk_accounts PRIMARY KEY(account_id)
) USING DELTA;
""")

# 3. Transactions Fact Table
spark.sql("""
CREATE TABLE IF NOT EXISTS banking_catalog.silver.fact_transactions (
    transaction_id STRING NOT NULL,
    account_id STRING,
    txn_type STRING,
    amount DECIMAL(18, 2),
    txn_timestamp TIMESTAMP,
    CONSTRAINT pk_transactions PRIMARY KEY(transaction_id)
) USING DELTA
CLUSTER BY (account_id, txn_timestamp);
""")

# COMMAND ----------

# DBTITLE 3, Clean & Upsert Customers
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
        trim(col("phone_number")).alias("phone_number"),
        to_date(col("created_date")).alias("created_date"),
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

# COMMAND ----------

# DBTITLE 4, Clean & Upsert Accounts
df_acc_clean = (
    spark.table("banking_catalog.bronze.bronze_accounts")
    .select(
        trim(col("account_id")).alias("account_id"),
        trim(col("customer_id")).alias("customer_id"),
        trim(col("account_type")).alias("account_type"),
        col("balance").cast("decimal(18,2)").alias("balance"),
        trim(col("status")).alias("status"),
        to_date(col("opened_date")).alias("opened_date")
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

# COMMAND ----------

# DBTITLE 5, Clean & Upsert Transactions
df_txn_clean = (
    spark.table("banking_catalog.bronze.bronze_transactions")
    .select(
        trim(col("transaction_id")).alias("transaction_id"),
        trim(col("account_id")).alias("account_id"),
        trim(col("txn_type")).alias("txn_type"),
        col("amount").cast("decimal(18,2)").alias("amount"),
        to_timestamp(col("txn_timestamp")).alias("txn_timestamp")
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