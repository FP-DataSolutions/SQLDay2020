-- Databricks notebook source
SHOW DATABASES

-- COMMAND ----------

DROP DATABASE IF EXISTS DeltaDWH CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS DeltaDWH LOCATION '/mnt/datalake/analytics_zone/DWHS/DeltaDWH/'

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

USE DeltaDWH

-- COMMAND ----------

CREATE TABLE dimDate USING DELTA AS SELECT DateId AS DateKey,Date,Year,Quarter,Month,Week FROM default.temp_dimdate

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * FROM dimDate WHERE Year=2020 AND Month=12

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls /mnt/datalake/analytics_zone/DWHS/DeltaDWH/
