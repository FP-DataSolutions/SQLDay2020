-- Databricks notebook source
SHOW DATABASES

-- COMMAND ----------

-- DBTITLE 1,Go To Synapse DemoDelta
USE DeltaDWH

-- COMMAND ----------

DESCRIBE EXTENDED dimUser

-- COMMAND ----------

DESCRIBE HISTORY dimUser

-- COMMAND ----------

-- DBTITLE 1,Show Delta table -Data Lake
-- MAGIC %fs
-- MAGIC ls dbfs:/mnt/datalake/analytics_zone/DWHS/DeltaDWH/dimuser

-- COMMAND ----------

-- DBTITLE 1,Vacuum
SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

VACUUM dimUser RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- DBTITLE 1,Symlink
SET spark.databricks.delta.symlinkFormatManifest.fileSystemCheck.enabled = false

-- COMMAND ----------

GENERATE symlink_format_manifest FOR TABLE delta.`dbfs:/mnt/datalake/analytics_zone/DWHS/DeltaDWH/dimdate`;
GENERATE symlink_format_manifest FOR TABLE delta.`dbfs:/mnt/datalake/analytics_zone/DWHS/DeltaDWH/dimuser`;
GENERATE symlink_format_manifest FOR TABLE delta.`dbfs:/mnt/datalake/analytics_zone/DWHS/DeltaDWH/dimtaskdetails`;
GENERATE symlink_format_manifest FOR TABLE delta.`dbfs:/mnt/datalake/analytics_zone/DWHS/DeltaDWH/facttask`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Go To Synapse 
