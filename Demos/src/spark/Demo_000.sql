-- Databricks notebook source
-- MAGIC %fs
-- MAGIC ls 'dbfs:/mnt/datalake/rawdata/'

-- COMMAND ----------

DROP TABLE IF EXISTS hubspot;
CREATE TABLE hubspot USING JSON OPTIONS (multiline 'true') LOCATION '/mnt/datalake/rawdata/HubSpot/engagements/yyyyMMdd=20201120/';

-- COMMAND ----------

DROP TABLE IF EXISTS users;
CREATE TABLE users USING PARQUET  LOCATION '/mnt/datalake/rawdata/UserDB/users/yyyyMMdd=20201120/';

-- COMMAND ----------

SELECT * FROM hubspot

-- COMMAND ----------

DESCRIBE FORMATTED  hubspot

-- COMMAND ----------

SELECT
    ds.engagement AS e,
    ds.associations AS a,
    ds.metadata as m
  FROM
    hubspot as r LATERAL VIEW explode(r.results) mv AS ds

-- COMMAND ----------

WITH hd AS
(
SELECT
    ds.engagement AS e,
    ds.associations AS a,
    ds.metadata as m
  FROM
    hubspot as r LATERAL VIEW explode(r.results) mv AS ds
)
SELECT from_unixtime(e.timestamp / 1000, 'yyyy-MM-dd hh:mm:ss') AS creationDate,
e.*,
e.OwnerId
FROM hd
WHERE e.type='TASK'

-- COMMAND ----------

WITH hd AS
(
SELECT
    ds.engagement AS e,
    ds.associations AS a,
    ds.metadata as m
  FROM
    hubspot as r LATERAL VIEW explode(r.results) mv AS ds
)
SELECT 
u.UserName,
COUNT(*) AS totalTasks
FROM hd
JOIN users AS u ON u.UserId = hd.e.OwnerId
WHERE e.type='TASK'
GROUP BY u.UserName
