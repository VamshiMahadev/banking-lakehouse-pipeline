-- Databricks notebook source
-- DBTITLE 1, Environment & Schema Setup
CREATE CATALOG IF NOT EXISTS banking_catalog;
USE CATALOG banking_catalog;

-- Create Schemas for Medallion Layers
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Create External Storage Locations (Unity Catalog)
CREATE EXTERNAL LOCATION IF NOT EXISTS ext_datalake_raw
URL 'abfss://raw@datalakevamshi.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sp_cred);

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_datalake_curated
URL 'abfss://curated@datalakevamshi.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sp_cred);