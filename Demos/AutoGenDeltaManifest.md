# Auto generate symlink delta manifest

```sql
DROP DATABASE IF EXISTS DeltaTest CASCADE
```

```sql
CREATE DATABASE IF NOT EXISTS DeltaTest LOCATION '/mnt/datalake/analytics_zone/DWHS/DeltaTest/'
```

```sql
USE DeltaTest
```

```sql
CREATE TABLE IF NOT EXISTS Test (Val INT) USING DELTA
TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)  
```

**TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)**

```sql
SET spark.databricks.delta.symlinkFormatManifest.fileSystemCheck.enabled = false --Azure Data Lake Gen2
```

```sql
INSERT INTO Test SELECT 1
```

```sql
%fs
head /mnt/datalake/analytics_zone/DWHS/DeltaTest/test/_symlink_format_manifest/manifest
```

```sql
UPDATE Test SET Val = 2
```

```sql
%fs
head /mnt/datalake/analytics_zone/DWHS/DeltaTest/test/_symlink_format_manifest/manifest
```

