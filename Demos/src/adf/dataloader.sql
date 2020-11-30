
SELECT ds.[Name] AS DataSourceType, 
       s.[Name] AS DataSinkType, 
       p.[Name] AS PartitionType, 
       c.[Name], 
       c.[Description], 
       c.[SqlIngestQuery], 
       c.[SqlMaxIdQuery], 
       c.[SourceServer], 
       c.[SourceDatabaseName], 
       c.[SinkDir], 
       c.[SinkFileNamePrefix], 
       c.[SinkFileNameExt], 
       c.[IsActive]
FROM [dbo].[SqlLoaderConfig] AS c
     JOIN dbo.DataSources AS ds ON ds.Id = c.DataSourceId
     JOIN dbo.SinkTypes AS s ON s.Id = c.SinkTypeId
     JOIN dbo.PartitionTypes AS p ON p.Id = c.PartitionTypeId;


  EXEC [dbo].[usp_GetDataLoaderConfigDateRange] 1,1;
  EXEC [dbo].[usp_GetDataLoaderConfigKeysRange] 1,1;

TRUNCATE TABLE [dbo].[DataLoadsHistory]; --Run ADF , Show LinkedService,Show Data Lake

  SELECT * FROM [dbo].[DataLoadsHistory]

  EXEC [dbo].[usp_GetDataLoaderConfigDateRange] 1,1;
  EXEC [dbo].[usp_GetDataLoaderConfigKeysRange] 1,1;
