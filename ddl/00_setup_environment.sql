-- Databricks notebook source
-- DBTITLE 1, Create Parameter Widget
CREATE WIDGET TEXT storage_account DEFAULT 'datalakevamshi';

-- COMMAND ----------

-- DBTITLE 2, Create Storage Credentials & External Locations
-- Create Storage Credential if not already present
CREATE STORAGE CREDENTIAL IF NOT EXISTS sp_cred1
WITH IDENTITY = 'ManagedIdentity';

-- Create External Locations using widget parameter markers
CREATE EXTERNAL LOCATION IF NOT EXISTS ext_datalake_raw
URL 'abfss://raw@${storage_account}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sp_cred1);

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_datalake_curated
URL 'abfss://curated@${storage_account}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sp_cred1);

-- COMMAND ----------

-- DBTITLE 3, Create Catalog & Medallion Schemas
-- Note: Unity Catalog creation in pure SQL requires an existing external location
CREATE CATALOG IF NOT EXISTS banking_catalog
MANAGED LOCATION 'abfss://curated@${storage_account}.dfs.core.windows.net/banking_catalog';

USE CATALOG banking_catalog;

-- Create Schemas for Medallion Layers
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;