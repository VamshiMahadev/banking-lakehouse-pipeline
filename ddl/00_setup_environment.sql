-- Databricks notebook source
-- DBTITLE 1, Create Parameter Widget
-- Create a text widget parameter with a default value
CREATE WIDGET TEXT storage_account DEFAULT 'datalakename';

-- COMMAND ----------

-- DBTITLE 2, Environment & Schema Setup
CREATE CATALOG IF NOT EXISTS banking_catalog;
USE CATALOG banking_catalog;

-- Create Schemas for Medallion Layers
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- COMMAND ----------

-- DBTITLE 3, Dynamic External Locations Setup
-- Use SQL variable substitution using ${getArgument('widget_name')} or ${storage_account}
CREATE EXTERNAL LOCATION IF NOT EXISTS ext_datalake_raw
URL concat('abfss://raw@', :storage_account, '.dfs.core.windows.net/')
WITH (STORAGE CREDENTIAL sp_cred1);

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_datalake_curated
URL concat('abfss://curated@', :storage_account, '.dfs.core.windows.net/')
WITH (STORAGE CREDENTIAL sp_cred1);