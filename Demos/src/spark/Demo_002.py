# Databricks notebook source
# DBTITLE 1,Set Spark Configuration
spark.conf.set("spark.sql.shuffle.partitions",16)
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite",True)
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact",True)

# COMMAND ----------

# DBTITLE 1,Import deltaWarehouse library 
from etl.etl_process import ETLProcess
from utils.config import Config

# COMMAND ----------

# DBTITLE 1,Set Library Configuration (https://github.com/FP-DataSolutions/DeltaWarehouse)
Config.IsSparkDataBrick=True
Config.DATABASE_HOSTNAME = 'sqldemos.database.windows.net'
Config.DATABASE_PORT = 1433
Config.DATABASE_NAME = 'dwdemo'
Config.DATABASE_USERNAME = 'demoadmin'
Config.DATABASE_PASSWORD = dbutils.secrets.get(scope = "keyvault", key= "sinkdwhpassword")
Config.DATABASE_ENCRYPT = 'true'

# COMMAND ----------

# DBTITLE 1,Set ETL Configuration (datasources config and config -show & explain)
base_path='/mnt/datalake'
dw_name='DeltaDWH'
dw_path='analytics_zone/DWHS/DeltaDWH/'
data_source_configurations='analytics_zone/ETLUtils/datasourcesconfig.csv'
etl_configurations='analytics_zone/ETLUtils/config.csv'

# COMMAND ----------

# DBTITLE 1,Run ETL Process (Show Views -explain)
etl_proc = ETLProcess(base_path,
                      data_source_configurations,
                      etl_configurations,
                      dw_path,
                      dw_name)
etl_proc.init()
etl_proc.register_data_sources()
##create views -DIMS
etl_proc.get_dw().run_sql("CREATE OR REPLACE VIEW vwDimUser AS SELECT UserId,UserName, UserRole  FROM stage_users ")
etl_proc.get_dw().run_sql("CREATE OR REPLACE VIEW vwDimTaskDetails AS SELECT \
    ds.engagement.Id AS TaskId, \
    ds.engagement.bodyPreview AS Details \
  FROM \
    stage_hubspot as r LATERAL VIEW explode(r.results) mv AS ds \
 WHERE ds.engagement.type='TASK' ")
etl_proc.run_dimensions()
##create views -FACTS
etl_proc.get_dw().run_sql("CREATE OR REPLACE VIEW vwFactTask AS \
WITH CTE AS \
( \
SELECT \
    ds.engagement.Id,\
    ds.engagement.ownerId, \
    from_unixtime(ds.engagement.timestamp / 1000, 'yyyyMMdd') AS creationDateId \
  FROM \
    stage_hubspot as r LATERAL VIEW explode(r.results) mv AS ds \
 WHERE ds.engagement.type='TASK' \
 ) \
 SELECT e.Id AS TaskId, \
        d.DateKey, \
        u.UserKey, \
        t.TaskDetailsKey\
        FROM CTE AS e \
 JOIN dimDate AS d ON d.DateKey = creationDateId \
 JOIN dimUser AS u ON e.ownerId = u.UserId AND u.row_iscurrent = 1 \
 JOIN dimTaskDetails AS t ON t.TaskId = e.Id AND t.row_iscurrent = 1 ")
etl_proc.run_facts()

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC d.Year,u.UserName, COUNT(*) AS total
# MAGIC FROM factTask AS t
# MAGIC JOIN dimDate AS d ON d.DateKey = t.DateKey
# MAGIC JOIN dimUser AS u ON u.UserKey = t.UserKey
# MAGIC GROUP BY d.Year,u.UserName
# MAGIC ORDER BY d.Year DESC, total DESC

# COMMAND ----------

# DBTITLE 1,ETL -SCD2 
users_df = spark.read.parquet('/mnt/datalake/rawdata/UserDB/users/yyyyMMdd=20201121/')
users_df.show()
spark.sql("SELECT UserId,UserName,UserRole FROM dimUser").show()

# COMMAND ----------

# DBTITLE 1,Configure ETL
base_path='/mnt/datalake'
dw_name='DeltaDWH'
dw_path='analytics_zone/DWHS/DeltaDWH/'
data_source_configurations='analytics_zone/ETLUtils/datasourcesconfig1.csv'
etl_configurations='analytics_zone/ETLUtils/config.csv'

# COMMAND ----------

# DBTITLE 1,Run ETL
etl_proc = ETLProcess(base_path,
                      data_source_configurations,
                      etl_configurations,
                      dw_path,
                      dw_name)
etl_proc.init()
etl_proc.register_data_sources()
etl_proc.run_dimensions()

# COMMAND ----------

# DBTITLE 1,Show dimUser (SCD2)
# MAGIC %sql
# MAGIC SELECT UserKey,UserId,UserName,UserRole,row_iscurrent, row_startdate,row_enddate FROM dimUser WHERE UserId = 0 ORDER BY row_iscurrent DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM  dimUser

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM  dimUser_etl_hist

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/datalake/analytics_zone/DWHS/DeltaDWH/
