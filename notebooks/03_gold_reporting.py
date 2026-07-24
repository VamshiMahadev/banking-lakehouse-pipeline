# Databricks notebook source
# DBTITLE 1, Gold Layer - Aggregated Views & Business Logic
# MAGIC %sql
# MAGIC USE CATALOG banking_catalog;
# MAGIC USE SCHEMA gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DBTITLE 1, Customer 360 & Financial Portfolio View
# MAGIC CREATE OR REPLACE VIEW banking_catalog.gold.v_customer_360 AS
# MAGIC SELECT 
# MAGIC     c.customer_id,
# MAGIC     CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
# MAGIC     c.email,
# MAGIC     c.kyc_status,
# MAGIC     COUNT(DISTINCT a.account_id) AS total_accounts,
# MAGIC     COALESCE(SUM(a.balance), 0) AS total_liquidity_balance
# MAGIC FROM banking_catalog.silver.dim_customers c
# MAGIC LEFT JOIN banking_catalog.silver.dim_accounts a ON c.customer_id = a.customer_id
# MAGIC GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.kyc_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DBTITLE 1, Daily Transaction Summary (Gold Table)
# MAGIC CREATE TABLE IF NOT EXISTS banking_catalog.gold.fact_daily_account_metrics
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT 
# MAGIC     account_id,
# MAGIC     DATE(txn_timestamp) AS txn_date,
# MAGIC     SUM(CASE WHEN transaction_type IN ('DEPOSIT', 'CREDIT') THEN amount ELSE 0 END) AS total_deposits,
# MAGIC     SUM(CASE WHEN transaction_type IN ('WITHDRAWAL', 'DEBIT') THEN amount ELSE 0 END) AS total_withdrawals,
# MAGIC     COUNT(transaction_id) AS transaction_count
# MAGIC FROM banking_catalog.silver.fact_transactions
# MAGIC GROUP BY account_id, DATE(txn_timestamp);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DBTITLE 1, Performance Optimization with Z-Order Optimization
# MAGIC OPTIMIZE banking_catalog.gold.fact_daily_account_metrics ZORDER BY (account_id, txn_date);