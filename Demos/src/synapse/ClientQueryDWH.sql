SELECT 
d.Year,u.UserName, COUNT(*) AS total
FROM dbo.vwFactTask AS t
JOIN dbo.vwDimDate AS d ON d.DateKey = t.DateKey
JOIN dbo.vwDimUser AS u ON u.UserKey = t.UserKey
GROUP BY d.Year,u.UserName
ORDER BY d.Year DESC, total DESC
---

SELECT COUNT(*) FROM
    OPENROWSET(
        BULK 'https://azureopendatastorage.blob.core.windows.net/nyctlc/yellow/puYear=2019/puMonth=*/*.parquet',
        FORMAT='PARQUET'
    ) AS [nyc];