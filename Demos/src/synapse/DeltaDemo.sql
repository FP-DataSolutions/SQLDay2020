CREATE DATABASE SqlDay2020Demo;
USE SqlDay2020Demo;

----
SELECT r.filename() AS DetlaFileName,
    *
FROM
    OPENROWSET(
        BULK 'abfss://datalake@datalake2demos.dfs.core.windows.net/analytics_zone/DWHS/DeltaDWH/dimuser/',
        FORMAT='PARQUET'
    ) AS [r]

WHERE UserId = 0 
---
--GO TO SPARK Demo003 -Show Delta https://github.com/FP-DataSolutions/SQLDay2020/blob/main/Demos/DeltaFormat.md



WITH dataFileNames AS
(
    SELECT REPLACE(f.C1,'abfss://datalake@datalake2demos.dfs.core.windows.net/analytics_zone/DWHS/DeltaDWH/dimuser/','') AS fileName FROM 
  OPENROWSET(
        BULK 'abfss://datalake@datalake2demos.dfs.core.windows.net/analytics_zone/DWHS/DeltaDWH/dimuser/_symlink_format_manifest/manifest',
        FORMAT='CSV',
        PARSER_VERSION = '2.0',
        firstrow = 0
  ) AS f
)
SELECT
    UserKey, UserId, UserRole,row_iscurrent AS IsCurrent,  row_startdate AS StartDate,row_enddate AS EndDate
FROM
    OPENROWSET(
        BULK 'abfss://datalake@datalake2demos.dfs.core.windows.net/analytics_zone/DWHS/DeltaDWH/dimuser/',
        FORMAT='PARQUET'
    ) AS [r]
WHERE r.filename() IN 
    (SELECT f.fileName FROM dataFileNames AS f )
AND UserId = 0 

--DROP VIEW IF EXISTS dbo.vmDimDate ;
-----------------------------------------------------------------CREATE VIEWS
DROP VIEW IF EXISTS dbo.vwDimUser ;
GO
CREATE  VIEW dbo.vwDimUser AS 
WITH dataFileNames AS
(
    SELECT REPLACE(f.C1,'abfss://datalake@datalake2demos.dfs.core.windows.net/analytics_zone/DWHS/DeltaDWH/dimuser/','') AS fileName FROM 
  OPENROWSET(
        BULK 'https://datalake2demos.dfs.core.windows.net/datalake/analytics_zone/DWHS/DeltaDWH/dimuser/_symlink_format_manifest/manifest',
        FORMAT='CSV',
        PARSER_VERSION = '2.0',
        firstrow = 0
  ) AS f
)
SELECT
     UserKey, UserId, UserName, UserRole,row_iscurrent AS IsCurrent,  row_startdate AS StartDate,row_enddate AS EndDate
FROM
    OPENROWSET(
        BULK 'https://datalake2demos.dfs.core.windows.net/datalake/analytics_zone/DWHS/DeltaDWH/dimuser/',
        FORMAT='PARQUET'
    ) AS [r]
WHERE r.filename() IN 
    (SELECT f.fileName FROM dataFileNames AS f )

GO
DROP VIEW IF EXISTS dbo.vwDimDate ;
GO
CREATE  VIEW dbo.vwDimDate AS 
WITH dataFileNames AS
(
    SELECT REPLACE(f.C1,'abfss://datalake@datalake2demos.dfs.core.windows.net/analytics_zone/DWHS/DeltaDWH/dimdate/','') AS fileName FROM 
  OPENROWSET(
        BULK 'https://datalake2demos.dfs.core.windows.net/datalake/analytics_zone/DWHS/DeltaDWH/dimdate/_symlink_format_manifest/manifest',
        FORMAT='CSV',
        PARSER_VERSION = '2.0',
        firstrow = 0
  ) AS f
)
SELECT
     *
FROM
    OPENROWSET(
        BULK 'https://datalake2demos.dfs.core.windows.net/datalake/analytics_zone/DWHS/DeltaDWH/dimdate/',
        FORMAT='PARQUET'
    ) AS [r]
WHERE r.filename() IN 
    (SELECT f.fileName FROM dataFileNames AS f )

GO

DROP VIEW IF EXISTS dbo.vwDimTaskDetails ;
GO
CREATE  VIEW dbo.vwDimTaskDetails AS 
WITH dataFileNames AS
(
    SELECT REPLACE(f.C1,'abfss://datalake@datalake2demos.dfs.core.windows.net/analytics_zone/DWHS/DeltaDWH/dimtaskdetails/','') AS fileName FROM 
  OPENROWSET(
        BULK 'https://datalake2demos.dfs.core.windows.net/datalake/analytics_zone/DWHS/DeltaDWH/dimtaskdetails/_symlink_format_manifest/manifest',
        FORMAT='CSV',
        PARSER_VERSION = '2.0',
        firstrow = 0
  ) AS f
)
SELECT
     TaskDetailsKey, TaskId, Details,row_iscurrent AS IsCurrent,  row_startdate AS StartDate,row_enddate AS EndDate
FROM
    OPENROWSET(
        BULK 'https://datalake2demos.dfs.core.windows.net/datalake/analytics_zone/DWHS/DeltaDWH/dimtaskdetails/',
        FORMAT='PARQUET'
    ) AS [r]
WHERE r.filename() IN 
    (SELECT f.fileName FROM dataFileNames AS f )


DROP VIEW IF EXISTS dbo.vwFactTask ;
GO
CREATE  VIEW dbo.vwFactTask AS 
WITH dataFileNames AS
(
    SELECT REPLACE(f.C1,'abfss://datalake@datalake2demos.dfs.core.windows.net/analytics_zone/DWHS/DeltaDWH/facttask/','') AS fileName FROM 
  OPENROWSET(
        BULK 'https://datalake2demos.dfs.core.windows.net/datalake/analytics_zone/DWHS/DeltaDWH/facttask/_symlink_format_manifest/',
        FORMAT='CSV',
        PARSER_VERSION = '2.0',
        firstrow = 0
  ) AS f
)
SELECT
     TaskKey, DateKey,UserKey,TaskDetailsKey
FROM
    OPENROWSET(
        BULK 'https://datalake2demos.dfs.core.windows.net/datalake/analytics_zone/DWHS/DeltaDWH/facttask/',
        FORMAT='PARQUET'
    ) AS [r]
WHERE r.filename() IN 
    (SELECT f.fileName FROM dataFileNames AS f )













