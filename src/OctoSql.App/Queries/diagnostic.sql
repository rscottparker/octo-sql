-- Created by GitHub Copilot in SSMS - review carefully before executing
-- =============================================
-- Data Integrity Diagnostic Script
-- Post-Clone Verification for AssocID Migration
-- =============================================

SET NOCOUNT ON;

PRINT '================================================================';
PRINT 'DATA INTEGRITY DIAGNOSTIC REPORT';
PRINT 'Generated: ' + CONVERT(VARCHAR(30), GETDATE(), 120);
PRINT '================================================================';
PRINT '';

-- =============================================
-- SECTION 1: Target AssocIDs Configuration
-- =============================================
DECLARE @BaseAssocID CHAR(3) = '012';

IF OBJECT_ID('tempdb..#TargetAssocIDs') IS NOT NULL
    DROP TABLE #TargetAssocIDs;

CREATE TABLE #TargetAssocIDs (
    AssocID CHAR(3) NOT NULL PRIMARY KEY
);

INSERT INTO #TargetAssocIDs (AssocID)
VALUES ('012'),('014'),('017'),('032'),('033'),('040'),('042'),('050'),
       ('057'),('076'),('078'),('081'),('085'),('086'),('092');

PRINT 'Target AssocIDs to verify: ' + CAST((SELECT COUNT(*) FROM #TargetAssocIDs) AS VARCHAR(10));
PRINT '';

-- =============================================
-- SECTION 2: Identify Tables with AssocID/CustAssocID
-- =============================================
PRINT '----------------------------------------------------------------';
PRINT 'SECTION 2: Tables with AssocID/CustAssocID Columns';
PRINT '----------------------------------------------------------------';

IF OBJECT_ID('tempdb..#TablesWithAssocID') IS NOT NULL
    DROP TABLE #TablesWithAssocID;

CREATE TABLE #TablesWithAssocID (
    SchemaName SYSNAME NOT NULL,
    TableName SYSNAME NOT NULL,
    ColumnName SYSNAME NOT NULL,
    HasIdentityColumn BIT NOT NULL,
    IdentityColumnName SYSNAME NULL,
    PRIMARY KEY (SchemaName, TableName, ColumnName)
);

INSERT INTO #TablesWithAssocID (SchemaName, TableName, ColumnName, HasIdentityColumn, IdentityColumnName)
SELECT DISTINCT 
    s.name AS SchemaName,
    t.name AS TableName,
    c.name AS ColumnName,
    CASE WHEN EXISTS (
        SELECT 1 FROM sys.columns ic 
        WHERE ic.object_id = t.object_id 
        AND ic.is_identity = 1
    ) THEN 1 ELSE 0 END AS HasIdentityColumn,
    (SELECT TOP 1 ic.name 
     FROM sys.columns ic 
     WHERE ic.object_id = t.object_id 
     AND ic.is_identity = 1) AS IdentityColumnName
FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    JOIN sys.columns c ON c.object_id = t.object_id
WHERE (c.name = N'AssocID' OR c.name = N'CustAssocID')
    AND t.is_ms_shipped = 0
    AND RIGHT(t.name, 2) <> '_A'
    AND t.name NOT LIKE 'Clone%'
ORDER BY s.name, t.name, c.name;

SELECT 
    CAST(SchemaName AS VARCHAR(20)) AS [Schema],
    CAST(TableName AS VARCHAR(50)) AS [Table],
    CAST(ColumnName AS VARCHAR(20)) AS [Column],
    CASE WHEN HasIdentityColumn = 1 
        THEN 'YES (' + ISNULL(IdentityColumnName, 'Unknown') + ')' 
        ELSE 'NO' 
    END AS [Has Identity Column]
FROM #TablesWithAssocID
ORDER BY SchemaName, TableName;

PRINT CHAR(10) + 'Total tables found: ' + CAST((SELECT COUNT(DISTINCT TableName) FROM #TablesWithAssocID) AS VARCHAR(10));
PRINT 'Tables with identity columns: ' + CAST((SELECT COUNT(DISTINCT TableName) FROM #TablesWithAssocID WHERE HasIdentityColumn = 1) AS VARCHAR(10));
PRINT '';

-- =============================================
-- SECTION 3: Row Count Validation by AssocID
-- =============================================
PRINT '----------------------------------------------------------------';
PRINT 'SECTION 3: Row Count Analysis by AssocID';
PRINT '----------------------------------------------------------------';

IF OBJECT_ID('tempdb..#RowCountResults') IS NOT NULL
    DROP TABLE #RowCountResults;

CREATE TABLE #RowCountResults (
    TableName SYSNAME NOT NULL,
    AssocID CHAR(3) NOT NULL,
    TotalRows INT NOT NULL,
    PRIMARY KEY (TableName, AssocID)
);

DECLARE @SQL NVARCHAR(MAX);
DECLARE @CurrentSchema SYSNAME;
DECLARE @CurrentTable SYSNAME;
DECLARE @CurrentColumn SYSNAME;

DECLARE table_cursor CURSOR FAST_FORWARD FOR
    SELECT DISTINCT SchemaName, TableName, ColumnName
    FROM #TablesWithAssocID;

OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @CurrentSchema, @CurrentTable, @CurrentColumn;

WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        SET @SQL = N'
            INSERT INTO #RowCountResults (TableName, AssocID, TotalRows)
            SELECT 
                ''' + @CurrentTable + ''' AS TableName,
                ' + QUOTENAME(@CurrentColumn) + ' AS AssocID,
                COUNT(*) AS TotalRows
            FROM ' + QUOTENAME(@CurrentSchema) + '.' + QUOTENAME(@CurrentTable) + '
            WHERE ' + QUOTENAME(@CurrentColumn) + ' IN (SELECT AssocID FROM #TargetAssocIDs)
            GROUP BY ' + QUOTENAME(@CurrentColumn) + ';';
        
        EXEC sp_executesql @SQL;
    END TRY
    BEGIN CATCH
        PRINT 'Error querying ' + @CurrentSchema + '.' + @CurrentTable + ': ' + ERROR_MESSAGE();
    END CATCH;

    FETCH NEXT FROM table_cursor INTO @CurrentSchema, @CurrentTable, @CurrentColumn;
END;

CLOSE table_cursor;
DEALLOCATE table_cursor;

-- Display row count summary
PRINT CHAR(10) + 'Row Count Summary (Top 20 tables by volume):';
SELECT TOP 20
    CAST(TableName AS VARCHAR(50)) AS [Table Name],
    CAST(AssocID AS VARCHAR(10)) AS [AssocID],
    TotalRows AS [Total Rows]
FROM #RowCountResults
ORDER BY TotalRows DESC, TableName, AssocID;

-- =============================================
-- SECTION 4: Missing Data Detection
-- =============================================
PRINT CHAR(10);
PRINT '----------------------------------------------------------------';
PRINT 'SECTION 4: Missing Data Detection';
PRINT '----------------------------------------------------------------';

-- Find tables where base AssocID has data but cloned AssocIDs don't
WITH BaseData AS (
    SELECT TableName, TotalRows AS BaseRows
    FROM #RowCountResults
    WHERE AssocID = @BaseAssocID AND TotalRows > 0
),
ClonedData AS (
    SELECT 
        r.TableName,
        COUNT(DISTINCT r.AssocID) AS AssocIDsWithData,
        SUM(r.TotalRows) AS TotalClonedRows
    FROM #RowCountResults r
    WHERE r.AssocID <> @BaseAssocID
    GROUP BY r.TableName
)
SELECT 
    CAST(b.TableName AS VARCHAR(50)) AS [Table Name],
    b.BaseRows AS [Base Rows (012)],
    ISNULL(c.AssocIDsWithData, 0) AS [Cloned AssocIDs],
    ISNULL(c.TotalClonedRows, 0) AS [Total Cloned Rows],
    CASE 
        WHEN ISNULL(c.AssocIDsWithData, 0) = 0 THEN '*** NO DATA CLONED ***'
        WHEN ISNULL(c.AssocIDsWithData, 0) < 14 THEN '*** PARTIAL CLONE ***'
        WHEN ISNULL(c.TotalClonedRows, 0) < (b.BaseRows * 14) THEN 'Row count mismatch'
        ELSE 'OK'
    END AS [Status]
FROM BaseData b
    LEFT JOIN ClonedData c ON b.TableName = c.TableName
ORDER BY 
    CASE 
        WHEN ISNULL(c.AssocIDsWithData, 0) = 0 THEN 1
        WHEN ISNULL(c.AssocIDsWithData, 0) < 14 THEN 2
        ELSE 3
    END,
    b.TableName;

-- =============================================
-- SECTION 5: Identity Column Issues
-- =============================================
PRINT CHAR(10);
PRINT '----------------------------------------------------------------';
PRINT 'SECTION 5: Tables with Identity Columns - Potential Issues';
PRINT '----------------------------------------------------------------';

SELECT 
    CAST(t.TableName AS VARCHAR(50)) AS [Table Name],
    CAST(t.IdentityColumnName AS VARCHAR(30)) AS [Identity Column],
    CASE 
        WHEN r.TableName IS NULL THEN '*** NO DATA FOUND ***'
        WHEN r.TotalRows = 0 THEN '*** ZERO ROWS ***'
        ELSE 'Has Data'
    END AS [Status],
    ISNULL(r.TotalRows, 0) AS [Rows for AssocID 012]
FROM #TablesWithAssocID t
    LEFT JOIN #RowCountResults r ON t.TableName = r.TableName AND r.AssocID = @BaseAssocID
WHERE t.HasIdentityColumn = 1
ORDER BY 
    CASE 
        WHEN r.TableName IS NULL THEN 1
        WHEN r.TotalRows = 0 THEN 2
        ELSE 3
    END,
    t.TableName;

-- =============================================
-- SECTION 6: Expected vs Actual Row Counts
-- =============================================
PRINT CHAR(10);
PRINT '----------------------------------------------------------------';
PRINT 'SECTION 6: Expected vs Actual Row Count Comparison';
PRINT '----------------------------------------------------------------';

WITH BaseRowCounts AS (
    SELECT TableName, TotalRows AS BaseRows
    FROM #RowCountResults
    WHERE AssocID = @BaseAssocID AND TotalRows > 0
),
ActualRowCounts AS (
    SELECT 
        TableName,
        COUNT(DISTINCT AssocID) AS UniqueAssocIDs,
        SUM(TotalRows) AS ActualTotalRows
    FROM #RowCountResults
    GROUP BY TableName
)
SELECT TOP 20
    CAST(b.TableName AS VARCHAR(50)) AS [Table Name],
    b.BaseRows AS [Base Rows],
    (b.BaseRows * 15) AS [Expected Total], -- 15 AssocIDs total
    ISNULL(a.ActualTotalRows, 0) AS [Actual Total],
    CAST(((ISNULL(a.ActualTotalRows, 0) * 100.0) / NULLIF((b.BaseRows * 15), 0)) AS DECIMAL(5,1)) AS [% Complete],
    CASE 
        WHEN ISNULL(a.ActualTotalRows, 0) = 0 THEN '*** FAILED ***'
        WHEN ISNULL(a.ActualTotalRows, 0) < (b.BaseRows * 15 * 0.9) THEN '*** INCOMPLETE ***'
        ELSE 'OK'
    END AS [Status]
FROM BaseRowCounts b
    LEFT JOIN ActualRowCounts a ON b.TableName = a.TableName
ORDER BY 
    [% Complete],
    b.BaseRows DESC;

-- =============================================
-- SECTION 7: Foreign Key Integrity Check
-- =============================================
PRINT CHAR(10);
PRINT '----------------------------------------------------------------';
PRINT 'SECTION 7: Foreign Key Integrity Status';
PRINT '----------------------------------------------------------------';

-- Check if FKs were dropped and need recreation
SELECT 
    CAST(OBJECT_SCHEMA_NAME(parent_object_id) AS VARCHAR(20)) AS [Parent Schema],
    CAST(OBJECT_NAME(parent_object_id) AS VARCHAR(40)) AS [Parent Table],
    CAST(name AS VARCHAR(50)) AS [FK Name],
    CAST(OBJECT_SCHEMA_NAME(referenced_object_id) AS VARCHAR(20)) AS [Referenced Schema],
    CAST(OBJECT_NAME(referenced_object_id) AS VARCHAR(40)) AS [Referenced Table],
    CASE WHEN is_disabled = 1 THEN 'DISABLED' ELSE 'ENABLED' END AS [Status],
    CASE WHEN is_not_trusted = 1 THEN 'NOT TRUSTED' ELSE 'TRUSTED' END AS [Trust Status]
FROM sys.foreign_keys
WHERE parent_object_id IN (
    SELECT t.object_id
    FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE EXISTS (
        SELECT 1 FROM #TablesWithAssocID ta
        WHERE ta.SchemaName = s.name AND ta.TableName = t.name
    )
)
ORDER BY [Parent Schema], [Parent Table], [FK Name];

-- =============================================
-- SECTION 8: Summary Report
-- =============================================
PRINT CHAR(10);
PRINT '================================================================';
PRINT 'SUMMARY REPORT';
PRINT '================================================================';

DECLARE @TotalTables INT, @TablesWithData INT, @TablesWithoutData INT, 
        @TablesWithPartialData INT, @TablesWithIdentity INT;

SELECT @TotalTables = COUNT(DISTINCT TableName) FROM #TablesWithAssocID;

SELECT @TablesWithData = COUNT(DISTINCT r.TableName)
FROM #RowCountResults r
WHERE r.AssocID <> @BaseAssocID AND r.TotalRows > 0;

SELECT @TablesWithoutData = COUNT(DISTINCT b.TableName)
FROM (SELECT DISTINCT TableName FROM #RowCountResults WHERE AssocID = @BaseAssocID AND TotalRows > 0) b
WHERE NOT EXISTS (
    SELECT 1 FROM #RowCountResults r 
    WHERE r.TableName = b.TableName AND r.AssocID <> @BaseAssocID AND r.TotalRows > 0
);

SELECT @TablesWithPartialData = COUNT(DISTINCT r.TableName)
FROM #RowCountResults r
WHERE r.AssocID <> @BaseAssocID
GROUP BY r.TableName
HAVING COUNT(DISTINCT r.AssocID) < 14;  -- Less than 14 cloned AssocIDs

SELECT @TablesWithIdentity = COUNT(DISTINCT TableName) 
FROM #TablesWithAssocID WHERE HasIdentityColumn = 1;

PRINT 'Total tables evaluated: ' + CAST(@TotalTables AS VARCHAR(10));
PRINT 'Tables with identity columns: ' + CAST(@TablesWithIdentity AS VARCHAR(10));
PRINT '';
PRINT 'Cloning Results:';
PRINT '  ✓ Tables successfully cloned: ' + CAST(@TablesWithData AS VARCHAR(10));
PRINT '  ✗ Tables with NO cloned data: ' + CAST(@TablesWithoutData AS VARCHAR(10));
PRINT '  ! Tables with PARTIAL data: ' + CAST(@TablesWithPartialData AS VARCHAR(10));
PRINT '';

IF @TablesWithoutData > 0 OR @TablesWithPartialData > 0
BEGIN
    PRINT '*** CRITICAL: Data cloning was INCOMPLETE ***';
    PRINT 'Action Required: Review SECTION 4 and SECTION 5 for details';
END
ELSE
BEGIN
    PRINT '✓ Data cloning appears COMPLETE for all tables';
END

PRINT '';
PRINT '================================================================';
PRINT 'END OF DIAGNOSTIC REPORT';
PRINT '================================================================';

-- Cleanup
DROP TABLE IF EXISTS #TargetAssocIDs;
DROP TABLE IF EXISTS #TablesWithAssocID;
DROP TABLE IF EXISTS #RowCountResults;