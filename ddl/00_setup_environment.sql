-- Databricks notebook source
-- DBTITLE 1, Create Parameter Widget
CREATE WIDGET TEXT storage_account DEFAULT 'datalakevamshi';

-- COMMAND ----------

-- DBTITLE 2, Dynamic Managed Storage Path
-- Construct the managed storage location URL for the catalog
SET spark.var.storage_account = :storage_account;

-- COMMAND ----------

-- DBTITLE 3, Create External Location & Catalog with Explicit Managed Location
-- First, ensure external location exists for Unity Catalog
CREATE EXTERNAL LOCATION IF NOT EXISTS ext_datalake_raw
URL concat('abfss://raw@', :storage_account, '.dfs.core.windows.net/')
WITH (STORAGE CREDENTIAL sp_cred);

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_datalake_curated
URL concat('abfss://curated@', :storage_account, '.dfs.core.windows.net/')
WITH (STORAGE CREDENTIAL sp_cred);

-- COMMAND ----------

-- DBTITLE 4, Create Catalog with Explicit Managed Location (FIXES THE ERROR)
-- Providing MANAGED LOCATION avoids relying on missing metastore default storage
CREATE CATALOG IF NOT EXISTS banking_catalog
MANAGED LOCATION concat('abfss://curated@', :storage_account, '.dfs.core.windows.net/banking_catalog');

USE CATALOG banking_catalog;

-- Create Schemas for Medallion Layers
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;