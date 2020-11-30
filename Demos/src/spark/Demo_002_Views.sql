-- Databricks notebook source
CREATE
OR REPLACE VIEW vwDimUser AS
SELECT
  UserId,
  UserName,
  UserRole
FROM
  stage_users

-- COMMAND ----------

CREATE
OR REPLACE VIEW vwDimTaskDetails AS
SELECT
  ds.engagement.Id AS TaskId,
  ds.engagement.bodyPreview AS Details
FROM
  stage_hubspot as r LATERAL VIEW explode(r.results) mv AS ds
WHERE
  ds.engagement.type = 'TASK'

-- COMMAND ----------

CREATE OR REPLACE VIEW vwFactTask AS 
WITH CTE AS 
( 
SELECT 
    ds.engagement.Id,
    ds.engagement.ownerId, 
    from_unixtime(ds.engagement.timestamp / 1000, 'yyyyMMdd') AS creationDateId 
  FROM 
    stage_hubspot as r LATERAL VIEW explode(r.results) mv AS ds 
 WHERE ds.engagement.type='TASK' 
 ) 
 SELECT e.Id AS TaskId, 
        d.DateKey, 
        u.UserKey, 
        t.TaskDetailsKey
        FROM CTE AS e 
 JOIN dimDate AS d ON d.DateKey = creationDateId 
 JOIN dimUser AS u ON e.ownerId = u.UserId AND u.row_iscurrent = 1 
 JOIN dimTaskDetails AS t ON t.TaskId = e.Id AND t.row_iscurrent = 1
