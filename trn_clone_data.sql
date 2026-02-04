--------------------------------------------------------------------
-- UDF: Generic ID recomputation for any ID column
--------------------------------------------------------------------
CREATE OR ALTER FUNCTION [dbo].[ufn_AssocDerivedID]
(
      @OriginalID      INT
    , @AssocID         CHAR(3)
    , @ShiftFactor     INT
)
RETURNS BIGINT
WITH SCHEMABINDING
AS
BEGIN
    RETURN
        CAST(@AssocID AS INT) * CAST(@ShiftFactor AS BIGINT)
        + CAST(@OriginalID AS BIGINT);
END;
GO
--------------------------------------------------------------------
-- Stored procedure: usp_Wip15_CloneAssocData
--------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[usp_Wip15_CloneAssocData]
    (
    @BaseAssocID CHAR(3) = '012'  -- Base AssocID to clone from
    ,
    @PackageID   INT
-- SSIS package identifier for logging
)
AS
BEGIN
    SET NOCOUNT ON;

    ----------------------------------------------------------------
    -- Isolation level
    -- If READ_COMMITTED_SNAPSHOT is ON at the database level,
    -- READ COMMITTED uses row versioning for all reads in this proc.
    ----------------------------------------------------------------
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

    ----------------------------------------------------------------
    -- Common variables
    ----------------------------------------------------------------
        DECLARE
                    @ProcName       SYSNAME        = OBJECT_NAME(@@PROCID)
                , @ResultMessage  NVARCHAR(MAX)
                , @SchemaName     SYSNAME
                , @TableName      SYSNAME
                , @CloneTableName SYSNAME
                , @CloneFullName  NVARCHAR(512)
                , @TableFullName  NVARCHAR(512)
                , @ObjName        NVARCHAR(512)
                , @SQL            NVARCHAR(MAX)
                , @ErrorMsg       NVARCHAR(MAX)
                -- shared error helper vars (declare once to reuse)
                , @ErrorTableFull NVARCHAR(512)
                , @DynCreate      NVARCHAR(MAX)
                , @DynInsertErr   NVARCHAR(MAX)
                , @DynSelectInto  NVARCHAR(MAX)
                , @ErrorTableFullC NVARCHAR(512)
                , @DynCreateC     NVARCHAR(MAX)
                , @DynInsertErrC  NVARCHAR(MAX)
                , @DynSelectIntoC NVARCHAR(MAX)
                , @FullNameC      NVARCHAR(512)
                , @CloneBaseFullC NVARCHAR(512);

    ----------------------------------------------------------------
    -- Log: procedure start
    ----------------------------------------------------------------
    BEGIN TRY
        SET @ResultMessage =
            N'START: ' + @ProcName
            + N'; BaseAssocID=' + CAST(@BaseAssocID AS NVARCHAR(10));

        INSERT INTO [dbo].[SSISLoad]
        (PackageID, Result, StatusDT)
    VALUES
        (@PackageID, @ResultMessage, GETDATE());
    END TRY
    BEGIN CATCH
        -- Do not fail core logic on logging issues
    END CATCH;

    /****************************************************************
     PHASE TRG0: Disable all table-level DML triggers on user tables
    ****************************************************************/
    IF OBJECT_ID('tempdb..#TargetTriggers') IS NOT NULL
        DROP TABLE #TargetTriggers;

    CREATE TABLE #TargetTriggers
    (
        TriggerName SYSNAME NOT NULL
        ,
        SchemaName SYSNAME NOT NULL
        ,
        TableName SYSNAME NOT NULL
        ,
        DisableScript NVARCHAR(MAX) NOT NULL
        ,
        EnableScript NVARCHAR(MAX) NOT NULL
        ,
        SortOrder INT IDENTITY(1,1) PRIMARY KEY
    );

    ;WITH
        TrgInfo
        AS
        (
            SELECT
                tr.name AS TriggerName
            , s.name  AS SchemaName
            , t.name  AS TableName
            FROM sys.triggers AS tr
                JOIN sys.tables  AS t ON tr.parent_id = t.object_id
                JOIN sys.schemas AS s ON t.schema_id  = s.schema_id
            WHERE
              tr.is_ms_shipped      = 0
                AND t.is_ms_shipped       = 0
                AND tr.parent_class_desc  = N'OBJECT_OR_COLUMN'
                AND tr.is_disabled        = 0
        )
    INSERT INTO #TargetTriggers
        (
        TriggerName
        , SchemaName
        , TableName
        , DisableScript
        , EnableScript
        )
    SELECT
        TriggerName
        , SchemaName
        , TableName
        , DisableScript =
            N'DISABLE TRIGGER ' + QUOTENAME(TriggerName)
            + N' ON ' + QUOTENAME(SchemaName) + N'.' + QUOTENAME(TableName) + N';'
        , EnableScript =
            N'ENABLE TRIGGER ' + QUOTENAME(TriggerName)
            + N' ON ' + QUOTENAME(SchemaName) + N'.' + QUOTENAME(TableName) + N';'
    FROM TrgInfo;

    DECLARE
          @DisableTrgSql  NVARCHAR(MAX)
        , @DisableTrgName SYSNAME;

    DECLARE DisableTrigger_Cursor CURSOR FAST_FORWARD FOR
        SELECT DisableScript, TriggerName
    FROM #TargetTriggers
    ORDER BY SortOrder;

    OPEN DisableTrigger_Cursor;

    FETCH NEXT FROM DisableTrigger_Cursor
        INTO @DisableTrgSql, @DisableTrgName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            EXEC sys.sp_executesql @DisableTrgSql;

            BEGIN TRY
                SET @ResultMessage =
                    N'DISABLE_TRIGGER: ' + @DisableTrgName;

                INSERT INTO [dbo].[SSISLoad]
            (PackageID, Result, StatusDT)
        VALUES
            (@PackageID, @ResultMessage, GETDATE());
            END TRY
            BEGIN CATCH
            END CATCH;
        END TRY
        BEGIN CATCH
            SET @ErrorMsg = N'ERROR disabling trigger ' + @DisableTrgName + N': ' + ERROR_MESSAGE();
            RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

            BEGIN TRY
                SET @ResultMessage =
                    N'DISABLE_TRIGGER_ERROR: ' + @DisableTrgName
                    + N'; Error=' + ERROR_MESSAGE();

                INSERT INTO [dbo].[SSISLoad]
            (PackageID, Result, StatusDT)
        VALUES
            (@PackageID, @ResultMessage, GETDATE());
            END TRY
            BEGIN CATCH
            END CATCH;
        END CATCH;

        FETCH NEXT FROM DisableTrigger_Cursor
            INTO @DisableTrgSql, @DisableTrgName;
    END;

    CLOSE DisableTrigger_Cursor;
    DEALLOCATE DisableTrigger_Cursor;

    /****************************************************************
     PHASE FK0: Drop all foreign key constraints on user tables
    ****************************************************************/
    IF OBJECT_ID('tempdb..#TargetFKs') IS NOT NULL
        DROP TABLE #TargetFKs;

    CREATE TABLE #TargetFKs
    (
        FKName SYSNAME NOT NULL
        ,
        ParentSchema SYSNAME NOT NULL
        ,
        ParentTable SYSNAME NOT NULL
        ,
        ReferencedSchema SYSNAME NOT NULL
        ,
        ReferencedTable SYSNAME NOT NULL
        ,
        CreateScript NVARCHAR(MAX) NOT NULL
        ,
        DropScript NVARCHAR(MAX) NOT NULL
        ,
        SortOrder INT IDENTITY(1,1) PRIMARY KEY
    );

    ;WITH
        FKInfo
        AS
        (
            SELECT
                fk.object_id AS FKObjectId
            , fk.name      AS FKName
            , ps.name      AS ParentSchema
            , pt.name      AS ParentTable
            , rs.name      AS ReferencedSchema
            , rt.name      AS ReferencedTable
            FROM sys.foreign_keys AS fk
                JOIN sys.tables      AS pt ON fk.parent_object_id    = pt.object_id
                JOIN sys.schemas     AS ps ON pt.schema_id           = ps.schema_id
                JOIN sys.tables      AS rt ON fk.referenced_object_id = rt.object_id
                JOIN sys.schemas     AS rs ON rt.schema_id           = rs.schema_id
            WHERE
              fk.is_ms_shipped = 0
                AND pt.is_ms_shipped = 0
        ),
        FKCols
        AS
        (
            SELECT
                fi.FKObjectId
            , ParentCols =
                STRING_AGG(QUOTENAME(pc.name), N', ')
                WITHIN GROUP (ORDER BY fkc.constraint_column_id)
            , ReferencedCols =
                STRING_AGG(QUOTENAME(rc.name), N', ')
                WITHIN GROUP (ORDER BY fkc.constraint_column_id)
            FROM FKInfo AS fi
                JOIN sys.foreign_key_columns AS fkc
                ON fi.FKObjectId = fkc.constraint_object_id
                JOIN sys.columns AS pc
                ON fkc.parent_object_id = pc.object_id
                    AND fkc.parent_column_id = pc.column_id
                JOIN sys.columns AS rc
                ON fkc.referenced_object_id = rc.object_id
                    AND fkc.referenced_column_id = rc.column_id
            GROUP BY fi.FKObjectId
        )
    INSERT INTO #TargetFKs
        (
        FKName
        , ParentSchema
        , ParentTable
        , ReferencedSchema
        , ReferencedTable
        , CreateScript
        , DropScript
        )
    SELECT
        fi.FKName
        , fi.ParentSchema
        , fi.ParentTable
        , fi.ReferencedSchema
        , fi.ReferencedTable
        , CreateScript =
            N'ALTER TABLE ' + QUOTENAME(fi.ParentSchema) + N'.' + QUOTENAME(fi.ParentTable)
            + N' WITH CHECK ADD CONSTRAINT ' + QUOTENAME(fi.FKName)
            + N' FOREIGN KEY (' + fkcols.ParentCols + N') REFERENCES '
            + QUOTENAME(fi.ReferencedSchema) + N'.' + QUOTENAME(fi.ReferencedTable)
            + N'(' + fkcols.ReferencedCols + N'); '
            + N'ALTER TABLE ' + QUOTENAME(fi.ParentSchema) + N'.' + QUOTENAME(fi.ParentTable)
            + N' CHECK CONSTRAINT ' + QUOTENAME(fi.FKName) + N';'
        , DropScript =
            N'ALTER TABLE ' + QUOTENAME(fi.ParentSchema) + N'.' + QUOTENAME(fi.ParentTable)
            + N' DROP CONSTRAINT ' + QUOTENAME(fi.FKName) + N';'
    FROM FKInfo AS fi
        JOIN FKCols AS fkcols
        ON fi.FKObjectId = fkcols.FKObjectId;

    DECLARE
          @DropFKSql NVARCHAR(MAX)
        , @FKName    SYSNAME;

    DECLARE DropFK_Cursor CURSOR FAST_FORWARD FOR
        SELECT DropScript, FKName
    FROM #TargetFKs
    ORDER BY SortOrder;

    OPEN DropFK_Cursor;

    FETCH NEXT FROM DropFK_Cursor
        INTO @DropFKSql, @FKName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            EXEC sys.sp_executesql @DropFKSql;

            BEGIN TRY
                SET @ResultMessage =
                    N'DROP_FK: ' + @FKName;

                INSERT INTO [dbo].[SSISLoad]
            (PackageID, Result, StatusDT)
        VALUES
            (@PackageID, @ResultMessage, GETDATE());
            END TRY
            BEGIN CATCH
            END CATCH;
        END TRY
        BEGIN CATCH
            SET @ErrorMsg = N'ERROR dropping FK ' + @FKName + N': ' + ERROR_MESSAGE();
            RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

            BEGIN TRY
                SET @ResultMessage =
                    N'DROP_FK_ERROR: ' + @FKName
                    + N'; Error=' + ERROR_MESSAGE();

                INSERT INTO [dbo].[SSISLoad]
            (PackageID, Result, StatusDT)
        VALUES
            (@PackageID, @ResultMessage, GETDATE());
            END TRY
            BEGIN CATCH
            END CATCH;
        END CATCH;

        FETCH NEXT FROM DropFK_Cursor
            INTO @DropFKSql, @FKName;
    END;

    CLOSE DropFK_Cursor;
    DEALLOCATE DropFK_Cursor;

    /****************************************************************
     STEP 0C: Cleanup specific temp/backup tables if they exist
    ****************************************************************/
    BEGIN TRY
        IF OBJECT_ID(N'[dbo].[temptable]', 'U') IS NOT NULL
        BEGIN
        DROP TABLE [dbo].[temptable];

        BEGIN TRY
                SET @ResultMessage = N'DROP_TABLE: [dbo].[temptable]';

                INSERT INTO [dbo].[SSISLoad]
            (PackageID, Result, StatusDT)
        VALUES
            (@PackageID, @ResultMessage, GETDATE());
            END TRY
            BEGIN CATCH
            END CATCH;
    END;

        IF OBJECT_ID(N'[dbo].[tblTxBookingAudit]', 'U') IS NOT NULL
        BEGIN
        DROP TABLE [dbo].[tblTxBookingAudit];

        BEGIN TRY
                SET @ResultMessage = N'DROP_TABLE: [dbo].[tblTxBookingAudit]';

                INSERT INTO [dbo].[SSISLoad]
            (PackageID, Result, StatusDT)
        VALUES
            (@PackageID, @ResultMessage, GETDATE());
            END TRY
            BEGIN CATCH
            END CATCH;
    END;

        IF OBJECT_ID(N'[dbo].[TxMasterLoanOrNote_BKP05052025]', 'U') IS NOT NULL
        BEGIN
        DROP TABLE [dbo].[TxMasterLoanOrNote_BKP05052025];

        BEGIN TRY
                SET @ResultMessage =
                    N'DROP_TABLE: [dbo].[TxMasterLoanOrNote_BKP05052025]';

                INSERT INTO [dbo].[SSISLoad]
            (PackageID, Result, StatusDT)
        VALUES
            (@PackageID, @ResultMessage, GETDATE());
            END TRY
            BEGIN CATCH
            END CATCH;
    END;

        IF OBJECT_ID(N'[dbo].[TxBooking_BKP_20250506]', 'U') IS NOT NULL
        BEGIN
        DROP TABLE [dbo].[TxBooking_BKP_20250506];

        BEGIN TRY
                SET @ResultMessage =
                    N'DROP_TABLE: [dbo].[TxBooking_BKP_20250506]';

                INSERT INTO [dbo].[SSISLoad]
            (PackageID, Result, StatusDT)
        VALUES
            (@PackageID, @ResultMessage, GETDATE());
            END TRY
            BEGIN CATCH
            END CATCH;
    END;

        IF OBJECT_ID(N'[dbo].[StpFileInfoBak]', 'U') IS NOT NULL
        BEGIN
        DROP TABLE [dbo].[StpFileInfoBak];

        BEGIN TRY
                SET @ResultMessage = N'DROP_TABLE: [dbo].[StpFileInfoBak]';

                INSERT INTO [dbo].[SSISLoad]
            (PackageID, Result, StatusDT)
        VALUES
            (@PackageID, @ResultMessage, GETDATE());
            END TRY
            BEGIN CATCH
            END CATCH;
    END;

        IF OBJECT_ID(N'[dbo].[TxScrAgScoreXML]', 'U') IS NOT NULL
        BEGIN
        DROP TABLE [dbo].[TxScrAgScoreXML];

        BEGIN TRY
                SET @ResultMessage = N'DROP_TABLE: [dbo].[TxScrAgScoreXML]';

                INSERT INTO [dbo].[SSISLoad]
            (PackageID, Result, StatusDT)
        VALUES
            (@PackageID, @ResultMessage, GETDATE());
            END TRY
            BEGIN CATCH
            END CATCH;
    END;
    END TRY
    BEGIN CATCH
        DECLARE @DropErrorMessage NVARCHAR(MAX) =
            N'DROP_SPECIFIC_TABLES_ERROR: ' + ERROR_MESSAGE();

        RAISERROR(@DropErrorMessage, 0, 1) WITH NOWAIT;

        BEGIN TRY
            SET @ResultMessage = @DropErrorMessage;

            INSERT INTO [dbo].[SSISLoad]
        (PackageID, Result, StatusDT)
    VALUES
        (@PackageID, @ResultMessage, GETDATE());
        END TRY
        BEGIN CATCH
        END CATCH;
    END CATCH;

    /****************************************************************
     STEP 0A: Rebuild all *_A tables via structure-only clone/drop/rename
    ****************************************************************/
    DECLARE TableA_Cursor CURSOR FAST_FORWARD FOR
        SELECT
        s.name AS SchemaName
            , t.name AS TableName
    FROM sys.tables  AS t
        JOIN sys.schemas AS s ON t.schema_id = s.schema_id
    WHERE
              t.is_ms_shipped = 0
        AND LEN(t.name) > 2
        AND RIGHT(t.name, 2) = '_A';

    OPEN TableA_Cursor;

    FETCH NEXT FROM TableA_Cursor
        INTO @SchemaName, @TableName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @TableFullName =
            QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@TableName);

        SET @CloneTableName = N'Clone_' + @TableName;
        SET @CloneFullName =
            QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@CloneTableName);

        SET @ErrorMsg = N'STEP 0A: Rebuilding _A table ' + @TableFullName;
        RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

    IF OBJECT_ID(@CloneFullName, 'U') IS NOT NULL
        BEGIN
        SET @SQL = N'DROP TABLE ' + @CloneFullName + N';';

        BEGIN TRY
                EXEC sys.sp_executesql @SQL;
            END TRY
            BEGIN CATCH
                SET @ErrorMsg = N'Warning dropping existing clone ' + @CloneFullName + N': ' + ERROR_MESSAGE();
                RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

                BEGIN TRY
                    SET @ResultMessage =
                        N'DROP_CLONE_A_ERROR: ' + @CloneFullName
                        + N'; Error=' + ERROR_MESSAGE();

                    INSERT INTO [dbo].[SSISLoad]
            (PackageID, Result, StatusDT)
        VALUES
            (@PackageID, @ResultMessage, GETDATE());
                END TRY
                BEGIN CATCH
                END CATCH;
            END CATCH;
    END;

    IF OBJECT_ID(@TableFullName, 'U') IS NOT NULL
        BEGIN
        SET @SQL =
                N'SELECT TOP (0) * '
                + N'INTO ' + @CloneFullName + N' '
                + N'FROM ' + @TableFullName + N';';

        BEGIN TRY
                EXEC sys.sp_executesql @SQL;

                BEGIN TRY
                    SET @ResultMessage =
                        N'CREATE_CLONE_A_FROM_A_EMPTY: ' + @CloneFullName
                        + N' FROM ' + @TableFullName
                        + N' (structure only, no data).';

                    INSERT INTO [dbo].[SSISLoad]
            (PackageID, Result, StatusDT)
        VALUES
            (@PackageID, @ResultMessage, GETDATE());
                END TRY
                BEGIN CATCH
                END CATCH;
            END TRY
            BEGIN CATCH
                SET @ErrorMsg = N'ERROR creating structure-only clone from _A table ' + @TableFullName + N': ' + ERROR_MESSAGE();
                RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

                BEGIN TRY
                    SET @ResultMessage =
                        N'CREATE_CLONE_A_FROM_A_EMPTY_ERROR: '
                        + @CloneFullName + N' FROM ' + @TableFullName
                        + N'; Error=' + ERROR_MESSAGE();

                    INSERT INTO [dbo].[SSISLoad]
            (PackageID, Result, StatusDT)
        VALUES
            (@PackageID, @ResultMessage, GETDATE());
                END TRY
                BEGIN CATCH
                END CATCH;

                GOTO NextA;
            END CATCH;
    END
        ELSE
        BEGIN
        SET @ErrorMsg = N'STEP 0A Warning: _A table not found for ' + @TableFullName;
        RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

    BEGIN TRY
                SET @ResultMessage =
                    N'A_TABLE_MISSING_DURING_CLONE: ' + @TableFullName;

                INSERT INTO [dbo].[SSISLoad]
        (PackageID, Result, StatusDT)
    VALUES
        (@PackageID, @ResultMessage, GETDATE());
            END TRY
            BEGIN CATCH
            END CATCH;
END;

SET @SQL = N'DROP TABLE ' + @TableFullName + N';';

BEGIN TRY
            EXEC sys.sp_executesql @SQL;

            BEGIN TRY
                SET @ResultMessage =
                    N'DROP_ORIGINAL_A_TABLE: Dropped ' + @TableFullName;

                INSERT INTO [dbo].[SSISLoad]
    (PackageID, Result, StatusDT)
VALUES
    (@PackageID, @ResultMessage, GETDATE());
            END TRY
            BEGIN CATCH
            END CATCH;
        END TRY
        BEGIN CATCH
            SET @ErrorMsg = N'ERROR dropping _A table ' + @TableFullName + N': ' + ERROR_MESSAGE();
            RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

            BEGIN TRY
                SET @ResultMessage =
                    N'DROP_ORIGINAL_A_TABLE_ERROR: ' + @TableFullName
                    + N'; Error=' + ERROR_MESSAGE();

                INSERT INTO [dbo].[SSISLoad]
    (PackageID, Result, StatusDT)
VALUES
    (@PackageID, @ResultMessage, GETDATE());
            END TRY
            BEGIN CATCH
            END CATCH;

            GOTO NextA;
        END CATCH;

SET @ObjName = @SchemaName + N'.' + @CloneTableName;

BEGIN TRY
            EXEC sys.sp_rename
                  @objname = @ObjName
                , @newname = @TableName
                , @objtype = N'OBJECT';

            BEGIN TRY
                SET @ResultMessage =
                    N'RENAME_CLONE_TO_A: ' + @CloneFullName
                    + N' -> ' + @TableFullName;

                INSERT INTO [dbo].[SSISLoad]
    (PackageID, Result, StatusDT)
VALUES
    (@PackageID, @ResultMessage, GETDATE());
            END TRY
            BEGIN CATCH
            END CATCH;
        END TRY
        BEGIN CATCH
            SET @ErrorMsg = N'ERROR renaming clone ' + @CloneFullName + N' to ' + @TableFullName + N': ' + ERROR_MESSAGE();
            RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

            BEGIN TRY
                SET @ResultMessage =
                    N'RENAME_CLONE_TO_A_ERROR: ' + @CloneFullName
                    + N' -> ' + @TableFullName
                    + N'; Error=' + ERROR_MESSAGE();

                INSERT INTO [dbo].[SSISLoad]
    (PackageID, Result, StatusDT)
VALUES
    (@PackageID, @ResultMessage, GETDATE());
            END TRY
            BEGIN CATCH
            END CATCH;
        END CATCH;

NextA:
FETCH NEXT FROM TableA_Cursor
            INTO @SchemaName, @TableName;
END;

CLOSE TableA_Cursor;
DEALLOCATE TableA_Cursor;

/****************************************************************
     STEP 0B: TxDecisionHMDAData clone/drop/rename
    ****************************************************************/
DECLARE
          @TxSchema SYSNAME
        , @TxTable  SYSNAME = N'TxDecisionHMDAData';

SELECT
    @TxSchema = s.name
FROM sys.tables  AS t
    JOIN sys.schemas AS s ON t.schema_id = s.schema_id
WHERE
          t.name          = @TxTable
    AND t.is_ms_shipped = 0;

IF @TxSchema IS NOT NULL
    BEGIN
    SET @TableFullName =
            QUOTENAME(@TxSchema) + N'.' + QUOTENAME(@TxTable);

    SET @CloneTableName = N'Clone_' + @TxTable;
    SET @CloneFullName =
            QUOTENAME(@TxSchema) + N'.' + QUOTENAME(@CloneTableName);
    SET @ObjName = @TxSchema + N'.' + @CloneTableName;

    RAISERROR(N'STEP 0B: Rebuilding TxDecisionHMDAData via clone/drop/rename', 0, 1) WITH NOWAIT;

    IF OBJECT_ID(@CloneFullName, 'U') IS NOT NULL
        BEGIN
        SET @SQL = N'DROP TABLE ' + @CloneFullName + N';';

        BEGIN TRY
                EXEC sys.sp_executesql @SQL;
            END TRY
            BEGIN CATCH
                SET @ErrorMsg = N'Warning dropping existing clone ' + @CloneFullName + N': ' + ERROR_MESSAGE();
                RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;
            END CATCH;
    END;

    IF EXISTS
        (
            SELECT 1
    FROM sys.columns AS c
    WHERE
                  c.object_id = OBJECT_ID(@TableFullName)
        AND c.name      = N'AssocID'
        )
        BEGIN
        SET @SQL =
                N'SELECT * '
                + N'INTO ' + @CloneFullName + N' '
                + N'FROM ' + @TableFullName + N' AS S '
                + N'WHERE S.AssocID = @pBaseAssocID;';

        BEGIN TRY
                EXEC sys.sp_executesql
                      @SQL
                    , N'@pBaseAssocID CHAR(3)'
                    , @pBaseAssocID = @BaseAssocID;

                BEGIN TRY
                    SET @ResultMessage =
                        N'CREATE_CLONE_TxDecisionHMDAData_BASEASSOC: '
                        + @CloneFullName + N' FROM ' + @TableFullName
                        + N'; BaseAssocID='
                        + CAST(@BaseAssocID AS NVARCHAR(10));

                    INSERT INTO [dbo].[SSISLoad]
            (PackageID, Result, StatusDT)
        VALUES
            (@PackageID, @ResultMessage, GETDATE());
                END TRY
                BEGIN CATCH
                END CATCH;
            END TRY
            BEGIN CATCH
                SET @ErrorMsg = N'ERROR cloning TxDecisionHMDAData (AssocID filter): ' + ERROR_MESSAGE();
                RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

                BEGIN TRY
                    SET @ResultMessage =
                        N'CREATE_CLONE_TxDecisionHMDAData_BASEASSOC_ERROR: '
                        + @CloneFullName + N' FROM ' + @TableFullName
                        + N'; BaseAssocID='
                        + CAST(@BaseAssocID AS NVARCHAR(10))
                        + N'; Error=' + ERROR_MESSAGE();

                    INSERT INTO [dbo].[SSISLoad]
            (PackageID, Result, StatusDT)
        VALUES
            (@PackageID, @ResultMessage, GETDATE());
                END TRY
                BEGIN CATCH
                END CATCH;
            END CATCH;
    END
        ELSE
        BEGIN
        SET @SQL =
                N'SELECT * '
                + N'INTO ' + @CloneFullName + N' '
                + N'FROM ' + @TableFullName + N';';

        BEGIN TRY
                EXEC sys.sp_executesql @SQL;

                BEGIN TRY
                    SET @ResultMessage =
                        N'CREATE_CLONE_TxDecisionHMDAData_NOASSOC: '
                        + @CloneFullName + N' FROM ' + @TableFullName;

                    INSERT INTO [dbo].[SSISLoad]
            (PackageID, Result, StatusDT)
        VALUES
            (@PackageID, @ResultMessage, GETDATE());
                END TRY
                BEGIN CATCH
                END CATCH;
            END TRY
            BEGIN CATCH
                SET @ErrorMsg = N'ERROR cloning TxDecisionHMDAData (no AssocID): ' + ERROR_MESSAGE();
                RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

                BEGIN TRY
                    SET @ResultMessage =
                        N'CREATE_CLONE_TxDecisionHMDAData_NOASSOC_ERROR: '
                        + @CloneFullName + N' FROM ' + @TableFullName
                        + N'; Error=' + ERROR_MESSAGE();

                    INSERT INTO [dbo].[SSISLoad]
            (PackageID, Result, StatusDT)
        VALUES
            (@PackageID, @ResultMessage, GETDATE());
                END TRY
                BEGIN CATCH
                END CATCH;
            END CATCH;
    END;

    SET @SQL = N'DROP TABLE ' + @TableFullName + N';';

    BEGIN TRY
            EXEC sys.sp_executesql @SQL;

            BEGIN TRY
                SET @ResultMessage =
                    N'DROP_TxDecisionHMDAData: Dropped ' + @TableFullName;

                INSERT INTO [dbo].[SSISLoad]
        (PackageID, Result, StatusDT)
    VALUES
        (@PackageID, @ResultMessage, GETDATE());
            END TRY
            BEGIN CATCH
            END CATCH;
        END TRY
        BEGIN CATCH
            SET @ErrorMsg = N'ERROR dropping TxDecisionHMDAData: ' + ERROR_MESSAGE();
            RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

            BEGIN TRY
                SET @ResultMessage =
                    N'DROP_TxDecisionHMDAData_ERROR: ' + @TableFullName
                    + N'; Error=' + ERROR_MESSAGE();

                INSERT INTO [dbo].[SSISLoad]
        (PackageID, Result, StatusDT)
    VALUES
        (@PackageID, @ResultMessage, GETDATE());
            END TRY
            BEGIN CATCH
            END CATCH;
        END CATCH;

    BEGIN TRY
            EXEC sys.sp_rename
                  @objname = @ObjName
                , @newname = @TxTable
                , @objtype = N'OBJECT';

            BEGIN TRY
                SET @ResultMessage =
                    N'RENAME_CLONE_TO_TxDecisionHMDAData: '
                    + @CloneFullName + N' -> ' + @TableFullName;

                INSERT INTO [dbo].[SSISLoad]
        (PackageID, Result, StatusDT)
    VALUES
        (@PackageID, @ResultMessage, GETDATE());
            END TRY
            BEGIN CATCH
            END CATCH;
        END TRY
        BEGIN CATCH
            SET @ErrorMsg = N'ERROR renaming clone to TxDecisionHMDAData: ' + ERROR_MESSAGE();
            RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

            BEGIN TRY
                SET @ResultMessage =
                    N'RENAME_CLONE_TO_TxDecisionHMDAData_ERROR: '
                    + @CloneFullName + N' -> ' + @TableFullName
                    + N'; Error=' + ERROR_MESSAGE();

                INSERT INTO [dbo].[SSISLoad]
        (PackageID, Result, StatusDT)
    VALUES
        (@PackageID, @ResultMessage, GETDATE());
            END TRY
            BEGIN CATCH
            END CATCH;
        END CATCH;
END
    ELSE
    BEGIN
    RAISERROR(N'STEP 0B Warning: TxDecisionHMDAData not found in this database.', 0, 1) WITH NOWAIT;

    BEGIN TRY
            SET @ResultMessage =
                N'TxDecisionHMDAData_MISSING_DURING_CLONE';

            INSERT INTO [dbo].[SSISLoad]
        (PackageID, Result, StatusDT)
    VALUES
        (@PackageID, @ResultMessage, GETDATE());
        END TRY
        BEGIN CATCH
        END CATCH;
END;

/****************************************************************
     CONFIG: Target AssocIDs (static list) into temp table
    ****************************************************************/
IF OBJECT_ID('tempdb..#TargetAssociations') IS NOT NULL
        DROP TABLE #TargetAssociations;

CREATE TABLE #TargetAssociations
(
    AssocID CHAR(3) NOT NULL PRIMARY KEY
);

INSERT INTO #TargetAssociations
    (AssocID)
VALUES
    ('085'),
    ('026'),
    ('057'),
    ('017'),
    ('042'),
    ('020'),
    ('007'),
    ('076'),
    ('087'),
    ('089'),
    ('033'),
    ('003'),
    ('014'),
    ('092'),
    ('088'),
    ('084'),
    ('081'),
    ('040'),
    ('077'),
    ('075'),
    ('050'),
    ('035'),
    ('203'),
    ('078'),
    ('079'),
    ('086'),
    ('032');

/****************************************************************
     STEP 1: Identify tables that contain an AssocID column (excluding *_A)
    ****************************************************************/
IF OBJECT_ID('tempdb..#TablesWithAssocID') IS NOT NULL
        DROP TABLE #TablesWithAssocID;

CREATE TABLE #TablesWithAssocID
(
    RowNum INT IDENTITY(1,1) PRIMARY KEY
        ,
    SchemaName SYSNAME NOT NULL
        ,
    TableName SYSNAME NOT NULL
);

INSERT INTO #TablesWithAssocID
    (SchemaName, TableName)
SELECT DISTINCT
    s.name AS SchemaName
        , t.name AS TableName
FROM sys.tables  AS t
    JOIN sys.schemas AS s ON t.schema_id = s.schema_id
    JOIN sys.columns AS c ON c.object_id = t.object_id
WHERE
          c.name          = N'AssocID'
    AND t.is_ms_shipped = 0
    AND RIGHT(t.name, 2) <> '_A';

DECLARE @TableCount INT;

SELECT @TableCount = COUNT(*)
FROM #TablesWithAssocID;

SET @ErrorMsg = N'Tables with AssocID found (non-_A): ' + CAST(@TableCount AS VARCHAR(10));
RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

/****************************************************************
     STEP 1C: Identify tables that contain a CustAssocID column (excluding *_A)
    ****************************************************************/
IF OBJECT_ID('tempdb..#TablesWithCustAssocID') IS NOT NULL
        DROP TABLE #TablesWithCustAssocID;

CREATE TABLE #TablesWithCustAssocID
(
    RowNum INT IDENTITY(1,1) PRIMARY KEY
        ,
    SchemaName SYSNAME NOT NULL
        ,
    TableName SYSNAME NOT NULL
);

INSERT INTO #TablesWithCustAssocID
    (SchemaName, TableName)
SELECT DISTINCT
    s.name AS SchemaName
        , t.name AS TableName
FROM sys.tables  AS t
    JOIN sys.schemas AS s ON t.schema_id = s.schema_id
    JOIN sys.columns AS c ON c.object_id = t.object_id
WHERE
          c.name          = N'CustAssocID'
    AND t.is_ms_shipped = 0
    AND RIGHT(t.name, 2) <> '_A';

DECLARE @CustTableCount INT;

SELECT @CustTableCount = COUNT(*)
FROM #TablesWithCustAssocID;

SET @ErrorMsg = N'Tables with CustAssocID found (non-_A): ' + CAST(@CustTableCount AS VARCHAR(10));
RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

/****************************************************************
     STEP 2: Rebuild each table with AssocID from BaseAssocID only
     TxLoanID strategy:
       - For tables with TxLoanID:
         L = max number of digits in TxLoanID (base rows)
         F = 10^L
         NewTxLoanID = CAST(TA.AssocID AS INT) * F + CAST(S.TxLoanID AS INT)
         Implemented via dbo.ufn_AssocDerivedID
       - For GUID columns in UNIQUE indexes (PK or otherwise):
         cloned rows use NEWID() to preserve uniqueness
       - CustomerId treated similarly to TxLoanID
    ****************************************************************/
DECLARE
          @RowNum2             INT
        , @MaxRowNum2          INT
        , @FullName            NVARCHAR(512)
        , @CloneBaseName       SYSNAME
        , @CloneBaseFull       NVARCHAR(512)
        , @HasBaseRows         INT
        , @HasAssocID          BIT
        , @HasTxLoanID         BIT
        , @TxLoanIdIsIdent     BIT
        , @TxLoanIdShiftFactor INT
        , @HasCustomerId       BIT
        , @CustomerIdIsIdent   BIT
        , @CustomerIdShiftFactor INT
        , @ColumnListBase      NVARCHAR(MAX)
        , @SelectListBase      NVARCHAR(MAX)
        , @ColumnListClone     NVARCHAR(MAX)
        , @SelectListClone     NVARCHAR(MAX)
        , @RowsInsertedBase    INT
        , @RowsInsertedClone   INT
        , @GuidUniqueCols      NVARCHAR(MAX);

SELECT @MaxRowNum2 = MAX(RowNum)
FROM #TablesWithAssocID;

SET @RowNum2 = 1;

WHILE @RowNum2 <= @MaxRowNum2
    BEGIN
    SELECT
        @SchemaName = SchemaName
            , @TableName  = TableName
    FROM #TablesWithAssocID
    WHERE RowNum = @RowNum2;

    IF @SchemaName IS NULL
        BEGIN
        SET @RowNum2 = @RowNum2 + 1;
        CONTINUE;
    END;

    SET @FullName =
            QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@TableName);

    SET @ErrorMsg = N'STEP 2: Rebuilding table ' + @FullName + N' from base AssocID = ' + @BaseAssocID;
    RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

----------------------------------------------------------------
-- SAFETY RULE:
-- If a table has neither AssocID nor TxLoanID nor CustomerId, its data must
-- not be changed by this cloning phase.
----------------------------------------------------------------
IF NOT EXISTS
        (
            SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID(@FullName)
        AND name      = N'AssocID'
        )
    AND NOT EXISTS
        (
            SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID(@FullName)
        AND name      = N'TxLoanID'
        )
    AND NOT EXISTS
        (
            SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID(@FullName)
        AND name      = N'CustomerId'
        )
        BEGIN
    SET @ErrorMsg = N' Skipping: table ' + @FullName + N' has no AssocID, TxLoanID, or CustomerId; data unchanged.';
    RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

SET @RowNum2 = @RowNum2 + 1;
CONTINUE;
END;

----------------------------------------------------------------
-- LOG GUID UNIQUE KEY USAGE (PKs and other UNIQUE indexes)
----------------------------------------------------------------
SET @RowsInsertedClone = @@ROWCOUNT;

    IF EXISTS
            (
                SELECT 1
    FROM sys.columns
    WHERE object_id   = OBJECT_ID(@FullName)
        AND name        = N'TxLoanID'
        AND is_identity = 1
            )
            BEGIN
        SET @TxLoanIdIsIdent = 1;
    END;


IF EXISTS
        (
            SELECT 1
FROM sys.columns
WHERE object_id = OBJECT_ID(@FullName)
    AND name      = N'CustomerId'
        )
        BEGIN
    SET @HasCustomerId = 1;

    IF EXISTS
            (
                SELECT 1
    FROM sys.columns
    WHERE object_id   = OBJECT_ID(@FullName)
        AND name        = N'CustomerId'
        AND is_identity = 1
            )
            BEGIN
        SET @CustomerIdIsIdent = 1;
    END;
END;

------------------------------------------------------------
-- 2.2 Check if table has any base rows for @BaseAssocID
------------------------------------------------------------
SET @SQL =
            N'SELECT @HasBaseRows_OUT = COUNT(*) '
            + N'FROM ' + @FullName + N' '
            + N'WHERE AssocID = @pBaseAssocID;';

SET @HasBaseRows = 0;

EXEC sys.sp_executesql
              @SQL
            , N'@pBaseAssocID CHAR(3), @HasBaseRows_OUT INT OUTPUT'
            , @pBaseAssocID    = @BaseAssocID
            , @HasBaseRows_OUT = @HasBaseRows OUTPUT;

IF @HasBaseRows = 0
        BEGIN
    SET @ErrorMsg = N' Skipping: no base rows with AssocID = ' + @BaseAssocID;
    RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;
SET @RowNum2 = @RowNum2 + 1;
CONTINUE;
END;

------------------------------------------------------------
-- 2.3 Create base clone table with only @BaseAssocID rows
------------------------------------------------------------
SET @CloneBaseName = N'CloneBase_' + @TableName;
SET @CloneBaseFull =
            QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@CloneBaseName);

IF OBJECT_ID(@CloneBaseFull, 'U') IS NOT NULL
        BEGIN
    SET @SQL = N'DROP TABLE ' + @CloneBaseFull + N';';
    BEGIN TRY
        EXEC sys.sp_executesql @SQL;
    END TRY
    BEGIN CATCH
        SET @ErrorMsg = N'ERROR inserting into ' + @FullName + N': ' + ERROR_MESSAGE();
        RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

        SET @ErrorTableFull = QUOTENAME(@SchemaName) + N'.' + QUOTENAME(N'Error_' + @TableName);
        IF OBJECT_ID(@ErrorTableFull, 'U') IS NULL
        BEGIN
            SET @DynCreate = N'SELECT TOP 0 * INTO ' + @ErrorTableFull + N' FROM ' + @CloneBaseFull + N';';
            EXEC(@DynCreate);
        END;

        SET @DynInsertErr = N'INSERT INTO ' + @ErrorTableFull + N' SELECT * FROM ' + @CloneBaseFull + N';';
        EXEC(@DynInsertErr);
    END CATCH;
END;

SET @SQL =
            N'SELECT * '
            + N'INTO ' + @CloneBaseFull + N' '
            + N'FROM ' + @FullName + N' AS S '
            + N'WHERE S.AssocID = @pBaseAssocID;';

BEGIN TRY
    EXEC sys.sp_executesql
          @SQL
        , N'@pBaseAssocID CHAR(3)'
        , @pBaseAssocID = @BaseAssocID;
END TRY
BEGIN CATCH
    SET @ErrorMsg = N'ERROR inserting into ' + @FullName + N': ' + ERROR_MESSAGE();
    RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

    SET @ErrorTableFull = QUOTENAME(@SchemaName) + N'.' + QUOTENAME(N'Error_' + @TableName);
    IF OBJECT_ID(@ErrorTableFull, 'U') IS NULL
    BEGIN
        SET @DynCreate = N'SELECT TOP 0 * INTO ' + @ErrorTableFull + N' FROM ' + @CloneBaseFull + N';';
        EXEC(@DynCreate);
    END;

    SET @DynInsertErr = N'INSERT INTO ' + @ErrorTableFull + N' SELECT * FROM ' + @CloneBaseFull + N';';
    EXEC(@DynInsertErr);
END CATCH;

------------------------------------------------------------
-- 2.3a Build TxLoanID shift factor if TxLoanID exists
------------------------------------------------------------
IF @HasTxLoanID = 1
        BEGIN
    DECLARE @TxLoanIdLen INT;

    SET @SQL =
                N'SELECT @Len_OUT = MAX(LEN(CAST([TxLoanID] AS VARCHAR(20)))) '
                + N'FROM ' + @CloneBaseFull + N';';

    SET @TxLoanIdLen = NULL;

    EXEC sys.sp_executesql
                  @SQL
                , N'@Len_OUT INT OUTPUT'
                , @Len_OUT = @TxLoanIdLen OUTPUT;

    IF @TxLoanIdLen IS NULL OR @TxLoanIdLen <= 0
                SET @TxLoanIdLen = 1;

    SET @TxLoanIdShiftFactor =
                CONVERT(INT, POWER(10.0, @TxLoanIdLen));
END;

------------------------------------------------------------
-- 2.3b Build CustomerId shift factor if CustomerId exists
------------------------------------------------------------
IF @HasCustomerId = 1
        BEGIN
    DECLARE @CustomerIdLen INT;

    SET @SQL =
                N'SELECT @Len_OUT = MAX(LEN(CAST([CustomerId] AS VARCHAR(20)))) '
                + N'FROM ' + @CloneBaseFull + N';';

    SET @CustomerIdLen = NULL;

    EXEC sys.sp_executesql
                  @SQL
                , N'@Len_OUT INT OUTPUT'
                , @Len_OUT = @CustomerIdLen OUTPUT;

    IF @CustomerIdLen IS NULL OR @CustomerIdLen <= 0
                SET @CustomerIdLen = 1;

    SET @CustomerIdShiftFactor =
                CONVERT(INT, POWER(10.0, @CustomerIdLen));
END;

------------------------------------------------------------
-- 2.4 Truncate original table (all rows removed)
------------------------------------------------------------
SET @SQL = N'TRUNCATE TABLE ' + @FullName + N';';
EXEC sys.sp_executesql @SQL;

------------------------------------------------------------
-- 2.5 Build column lists for base reinsert and cloned inserts
------------------------------------------------------------
SET @ColumnListBase  = NULL;
SET @SelectListBase  = NULL;
SET @ColumnListClone = NULL;
SET @SelectListClone = NULL;

IF @HasTxLoanID = 1 OR @HasCustomerId = 1
        BEGIN
    -- With TxLoanID or CustomerId: use UDFs for them; NEWID() for GUID unique keys
    IF OBJECT_ID('tempdb..#IdentityMaxValues') IS NOT NULL
    BEGIN
        SELECT
            @ColumnListBase = STRING_AGG(CAST(QUOTENAME(c.name) AS NVARCHAR(MAX)), N', ') WITHIN GROUP (ORDER BY c.column_id)
          , @SelectListBase = STRING_AGG(CAST(N'S.' + QUOTENAME(c.name) AS NVARCHAR(MAX)), N', ') WITHIN GROUP (ORDER BY c.column_id)
          , @ColumnListClone = STRING_AGG(CAST(QUOTENAME(c.name) AS NVARCHAR(MAX)), N', ') WITHIN GROUP (ORDER BY c.column_id)
          , @SelectListClone = STRING_AGG(
                CAST(
                    CASE
                        WHEN imv.ColumnName IS NOT NULL
                            THEN N'dbo.ufn_AssocDerivedID(S.' + QUOTENAME(c.name) + N', TA.AssocID, ' + CAST(ISNULL(imv.ShiftFactor, 0) AS NVARCHAR(20)) + N')'
                        WHEN c.name = N'AssocID'
                            THEN N'TA.AssocID'
                        WHEN c.name = N'TxLoanID'
                            THEN N'dbo.ufn_AssocDerivedID(S.[TxLoanID], TA.AssocID, @pShiftFactor)'
                        WHEN c.name = N'CustomerId'
                            THEN N'dbo.ufn_AssocDerivedID(S.[CustomerId], TA.AssocID, @pCustomerIdShiftFactor)'
                        WHEN uq.IsGuidUnique = 1
                            THEN N'NEWID()'
                        ELSE N'S.' + QUOTENAME(c.name)
                    END
                AS NVARCHAR(MAX)),
            N', '
        ) WITHIN GROUP (ORDER BY c.column_id)
        FROM sys.columns AS c
            OUTER APPLY
            (
                -- GUID columns participating in ANY UNIQUE index (PK or other)
                SELECT TOP (1) 1 AS IsGuidUnique
                FROM sys.indexes AS ix
                    JOIN sys.index_columns AS ic
                        ON ix.object_id = ic.object_id
                        AND ix.index_id  = ic.index_id
                    JOIN sys.columns AS c_orig
                        ON c_orig.object_id = ix.object_id
                        AND c_orig.column_id = ic.column_id
                WHERE
                      ix.object_id     = OBJECT_ID(@FullName)
                    AND ix.is_unique     = 1
                    AND c_orig.name      = c.name
                    AND c_orig.system_type_id = TYPE_ID(N'uniqueidentifier')
            ) AS uq
            LEFT JOIN #IdentityMaxValues AS imv
                ON imv.SchemaName = @SchemaName
                AND imv.TableName  = @TableName
                AND imv.ColumnName = c.name
        WHERE c.object_id   = OBJECT_ID(@CloneBaseFull)
          AND c.is_computed = 0;
    END
    ELSE
    BEGIN
        SELECT
            @ColumnListBase = STRING_AGG(CAST(QUOTENAME(c.name) AS NVARCHAR(MAX)), N', ') WITHIN GROUP (ORDER BY c.column_id)
          , @SelectListBase = STRING_AGG(CAST(N'S.' + QUOTENAME(c.name) AS NVARCHAR(MAX)), N', ') WITHIN GROUP (ORDER BY c.column_id)
          , @ColumnListClone = STRING_AGG(CAST(QUOTENAME(c.name) AS NVARCHAR(MAX)), N', ') WITHIN GROUP (ORDER BY c.column_id)
          , @SelectListClone = STRING_AGG(
                CAST(
                    CASE
                        WHEN c.name = N'AssocID'
                            THEN N'TA.AssocID'
                        WHEN c.name = N'TxLoanID'
                            THEN N'dbo.ufn_AssocDerivedID(S.[TxLoanID], TA.AssocID, @pShiftFactor)'
                        WHEN c.name = N'CustomerId'
                            THEN N'dbo.ufn_AssocDerivedID(S.[CustomerId], TA.AssocID, @pCustomerIdShiftFactor)'
                        WHEN uq.IsGuidUnique = 1
                            THEN N'NEWID()'
                        ELSE N'S.' + QUOTENAME(c.name)
                    END
                AS NVARCHAR(MAX)),
            N', '
        ) WITHIN GROUP (ORDER BY c.column_id)
        FROM sys.columns AS c
            OUTER APPLY
            (
                SELECT TOP (1) 1 AS IsGuidUnique
                FROM sys.indexes AS ix
                    JOIN sys.index_columns AS ic
                        ON ix.object_id = ic.object_id
                        AND ix.index_id  = ic.index_id
                    JOIN sys.columns AS c_orig
                        ON c_orig.object_id = ix.object_id
                        AND c_orig.column_id = ic.column_id
                WHERE
                      ix.object_id     = OBJECT_ID(@FullName)
                    AND ix.is_unique     = 1
                    AND c_orig.name      = c.name
                    AND c_orig.system_type_id = TYPE_ID(N'uniqueidentifier')
            ) AS uq
        WHERE c.object_id   = OBJECT_ID(@CloneBaseFull)
          AND c.is_computed = 0;
    END;
END
        ELSE
        BEGIN
    -- No TxLoanID or CustomerId: NEWID() for GUID unique keys; AssocID cloned
    SELECT
        @ColumnListBase =
                    STRING_AGG(
                        CAST(QUOTENAME(c.name) AS NVARCHAR(MAX)),
                        N', '
                    ) WITHIN GROUP (ORDER BY c.column_id)
                , @SelectListBase =
                    STRING_AGG(
                        CAST(N'S.' + QUOTENAME(c.name) AS NVARCHAR(MAX)),
                        N', '
                    ) WITHIN GROUP (ORDER BY c.column_id)
                , @ColumnListClone =
                    STRING_AGG(
                        CAST(QUOTENAME(c.name) AS NVARCHAR(MAX)),
                        N', '
                    ) WITHIN GROUP (ORDER BY c.column_id)
                , @SelectListClone =
                    STRING_AGG(
                        CAST(
                            CASE
                                WHEN c.name = N'AssocID'
                                    THEN N'TA.AssocID'
                                WHEN uq.IsGuidUnique = 1
                                    THEN N'NEWID()'
                                ELSE N'S.' + QUOTENAME(c.name)
                            END
                            AS NVARCHAR(MAX)
                        ),
                        N', '
                    ) WITHIN GROUP (ORDER BY c.column_id)
    FROM sys.columns AS c
            OUTER APPLY
            (
                -- GUID columns participating in ANY UNIQUE index (PK or other)
                SELECT TOP (1)
            1 AS IsGuidUnique
        FROM sys.indexes AS ix
            JOIN sys.index_columns AS ic
            ON ix.object_id = ic.object_id
                AND ix.index_id  = ic.index_id
            JOIN sys.columns AS c_orig
            ON c_orig.object_id = ix.object_id
                AND c_orig.column_id = ic.column_id
        WHERE
                      ix.object_id     = OBJECT_ID(@FullName)
            AND ix.is_unique     = 1
            AND c_orig.name      = c.name
            AND c_orig.system_type_id = TYPE_ID(N'uniqueidentifier')
            ) AS uq
    WHERE
                  c.object_id   = OBJECT_ID(@CloneBaseFull)
        AND c.is_identity = 0
        AND c.is_computed = 0;
END;

IF @ColumnListBase IS NULL OR LEN(@ColumnListBase) = 0
        BEGIN
    RAISERROR(N' Skipping: table has only identity/computed columns.', 0, 1) WITH NOWAIT;

    SET @SQL = N'DROP TABLE ' + @CloneBaseFull + N';';
    BEGIN TRY
        EXEC sys.sp_executesql @SQL;
    END TRY
    BEGIN CATCH
        SET @ErrorMsg = N'ERROR inserting into ' + @FullName + N': ' + ERROR_MESSAGE();
        RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

        SET @ErrorTableFull = QUOTENAME(@SchemaName) + N'.' + QUOTENAME(N'Error_' + @TableName);
        IF OBJECT_ID(@ErrorTableFull, 'U') IS NULL
        BEGIN
            SET @DynCreate = N'SELECT TOP 0 * INTO ' + @ErrorTableFull + N' FROM ' + @CloneBaseFull + N';';
            EXEC(@DynCreate);
        END;

        SET @DynInsertErr = N'INSERT INTO ' + @ErrorTableFull + N' SELECT * FROM ' + @CloneBaseFull + N';';
        EXEC(@DynInsertErr);
    END CATCH;

    SET @RowNum2 = @RowNum2 + 1;
    CONTINUE;
END;

------------------------------------------------------------
-- 2.6 Insert base rows back (AssocID = Base, original TxLoanID/CustomerId/keys)
------------------------------------------------------------
SET @RowsInsertedBase  = 0;
SET @RowsInsertedClone = 0;

    IF (
            (@HasTxLoanID = 1 AND @TxLoanIdIsIdent = 1)
        OR  (@HasCustomerId = 1 AND @CustomerIdIsIdent = 1)
        OR  (
                OBJECT_ID('tempdb..#IdentityMaxValues') IS NOT NULL
            AND EXISTS(
                    SELECT 1
                    FROM tempdb..#IdentityMaxValues imv
                    JOIN sys.columns sc ON sc.object_id = OBJECT_ID(@FullName) AND sc.name = imv.ColumnName AND sc.is_identity = 1
                    WHERE imv.SchemaName = @SchemaName AND imv.TableName = @TableName
                )
            )
    )
    BEGIN
        SET @SQL =
            N'SET IDENTITY_INSERT ' + @FullName + N' ON; '
            + N'INSERT INTO ' + @FullName + N' WITH (TABLOCK) ('
            + @ColumnListBase + N') '
            + N'SELECT ' + @SelectListBase + N' '
            + N'FROM ' + @CloneBaseFull + N' AS S; '
            + N'SET IDENTITY_INSERT ' + @FullName + N' OFF;';

        BEGIN TRY
            EXEC sys.sp_executesql @SQL;
        END TRY
        BEGIN CATCH
            SET @ErrorMsg = N'ERROR inserting base rows into ' + @FullName + N': ' + ERROR_MESSAGE();
            RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

            SET @ErrorTableFull = QUOTENAME(@SchemaName) + N'.' + QUOTENAME(N'Error_' + @TableName);
            IF OBJECT_ID(@ErrorTableFull, 'U') IS NULL
            BEGIN
                IF @SelectListBase IS NOT NULL
                BEGIN
                    SET @DynSelectInto =
                        N'SELECT ' + @SelectListBase
                        + N' INTO ' + @ErrorTableFull
                        + N' FROM ' + @CloneBaseFull + N' AS S;';

                    EXEC(@DynSelectInto);
                END
                ELSE
                BEGIN
                    SET @DynCreate = N'SELECT TOP 0 * INTO ' + @ErrorTableFull + N' FROM ' + @CloneBaseFull + N';';
                    EXEC(@DynCreate);
                END
            END;

            IF @SelectListBase IS NOT NULL
            BEGIN
                SET @DynInsertErr =
                    N'INSERT INTO ' + @ErrorTableFull
                    + N' SELECT ' + @SelectListBase
                    + N' FROM ' + @CloneBaseFull + N' AS S;';

                EXEC(@DynInsertErr);
            END
            ELSE
            BEGIN
                SET @DynInsertErr = N'INSERT INTO ' + @ErrorTableFull + N' SELECT * FROM ' + @CloneBaseFull + N';';
                EXEC(@DynInsertErr);
            END;
        END CATCH;
    END
    ELSE
    BEGIN
        SET @SQL =
            N'INSERT INTO ' + @FullName + N' WITH (TABLOCK) ('
            + @ColumnListBase + N') '
            + N'SELECT ' + @SelectListBase + N' '
            + N'FROM ' + @CloneBaseFull + N' AS S;';

        BEGIN TRY
            EXEC sys.sp_executesql @SQL;
        END TRY
        BEGIN CATCH
            SET @ErrorMsg = N'ERROR inserting base rows into ' + @FullName + N': ' + ERROR_MESSAGE();
            RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

            SET @ErrorTableFull = QUOTENAME(@SchemaName) + N'.' + QUOTENAME(N'Error_' + @TableName);
            IF OBJECT_ID(@ErrorTableFull, 'U') IS NULL
            BEGIN
                IF @SelectListBase IS NOT NULL
                BEGIN
                    SET @DynSelectInto =
                        N'SELECT ' + @SelectListBase
                        + N' INTO ' + @ErrorTableFull
                        + N' FROM ' + @CloneBaseFull + N' AS S;';

                    EXEC(@DynSelectInto);
                END
                ELSE
                BEGIN
                    SET @DynCreate = N'SELECT TOP 0 * INTO ' + @ErrorTableFull + N' FROM ' + @CloneBaseFull + N';';
                    EXEC(@DynCreate);
                END
            END;

            IF @SelectListBase IS NOT NULL
            BEGIN
                SET @DynInsertErr =
                    N'INSERT INTO ' + @ErrorTableFull
                    + N' SELECT ' + @SelectListBase
                    + N' FROM ' + @CloneBaseFull + N' AS S;';

                EXEC(@DynInsertErr);
            END
            ELSE
            BEGIN
                SET @DynInsertErr = N'INSERT INTO ' + @ErrorTableFull + N' SELECT * FROM ' + @CloneBaseFull + N';';
                EXEC(@DynInsertErr);
            END;
        END CATCH;
    END;

SET @RowsInsertedBase = @@ROWCOUNT;

BEGIN TRY
            SET @ResultMessage =
                N'REBUILD_BASE_ASSOC_TABLE: Table=' + @FullName
                + N'; BaseAssocID=' + CAST(@BaseAssocID AS NVARCHAR(10))
                + N'; RowsInserted='
                + CAST(@RowsInsertedBase AS NVARCHAR(50));

            INSERT INTO [dbo].[SSISLoad]
    (PackageID, Result, StatusDT)
VALUES
    (@PackageID, @ResultMessage, GETDATE());
        END TRY
        BEGIN CATCH
        END CATCH;

------------------------------------------------------------
-- 2.7 Insert cloned rows for all target AssocIDs (excluding base)
------------------------------------------------------------
IF (
        (@HasTxLoanID = 1 AND @TxLoanIdIsIdent = 1)
    OR  (@HasCustomerId = 1 AND @CustomerIdIsIdent = 1)
    OR  (
            OBJECT_ID('tempdb..#IdentityMaxValues') IS NOT NULL
        AND EXISTS(
                SELECT 1
                FROM tempdb..#IdentityMaxValues imv
                JOIN sys.columns sc ON sc.object_id = OBJECT_ID(@FullName) AND sc.name = imv.ColumnName AND sc.is_identity = 1
                WHERE imv.SchemaName = @SchemaName AND imv.TableName = @TableName
            )
        )
)
        BEGIN
    SET @SQL =
                N'SET IDENTITY_INSERT ' + @FullName + N' ON; '
                + N'INSERT INTO ' + @FullName + N' WITH (TABLOCK) ('
                + @ColumnListClone + N') '
                + N'SELECT ' + @SelectListClone + N' '
                + N'FROM ' + @CloneBaseFull + N' AS S '
                + N'CROSS JOIN #TargetAssociations AS TA '
                + N'WHERE TA.AssocID <> @pBaseAssocID; '
                + N'SET IDENTITY_INSERT ' + @FullName + N' OFF;';

    BEGIN TRY
        EXEC sys.sp_executesql
              @SQL
            , N'@pBaseAssocID CHAR(3), @pShiftFactor INT, @pCustomerIdShiftFactor INT'
            , @pBaseAssocID = @BaseAssocID
            , @pShiftFactor = @TxLoanIdShiftFactor
            , @pCustomerIdShiftFactor = @CustomerIdShiftFactor;
    END TRY
    BEGIN CATCH
        SET @ErrorMsg = N'ERROR inserting into ' + @FullName + N': ' + ERROR_MESSAGE();
        RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

        SET @ErrorTableFull = QUOTENAME(@SchemaName) + N'.' + QUOTENAME(N'Error_' + @TableName);
        IF OBJECT_ID(@ErrorTableFull, 'U') IS NULL
        BEGIN
            -- Try to reproduce the insert SELECT into the Error_ table so structure and transformed data match
            IF @SelectListClone IS NOT NULL
            BEGIN
                SET @DynSelectInto =
                    N'SELECT ' + @SelectListClone
                    + N' INTO ' + @ErrorTableFull
                    + N' FROM ' + @CloneBaseFull + N' AS S CROSS JOIN #TargetAssociations AS TA WHERE TA.AssocID <> @pBaseAssocID;';

                EXEC sys.sp_executesql
                    @DynSelectInto,
                    N'@pBaseAssocID CHAR(3), @pShiftFactor INT, @pCustomerIdShiftFactor INT',
                    @pBaseAssocID = @BaseAssocID,
                    @pShiftFactor = @TxLoanIdShiftFactor,
                    @pCustomerIdShiftFactor = @CustomerIdShiftFactor;
            END
            ELSE
            BEGIN
                SET @DynCreate = N'SELECT TOP 0 * INTO ' + @ErrorTableFull + N' FROM ' + @CloneBaseFull + N';';
                EXEC(@DynCreate);
            END
        END;

        -- Populate Error_ table with the transformed rows where possible, fallback to copying clone base rows
        IF @SelectListClone IS NOT NULL
        BEGIN
            SET @DynInsertErr =
                N'INSERT INTO ' + @ErrorTableFull
                + N' SELECT ' + @SelectListClone
                + N' FROM ' + @CloneBaseFull + N' AS S CROSS JOIN #TargetAssociations AS TA WHERE TA.AssocID <> @pBaseAssocID;';

            EXEC sys.sp_executesql
                @DynInsertErr,
                N'@pBaseAssocID CHAR(3), @pShiftFactor INT, @pCustomerIdShiftFactor INT',
                @pBaseAssocID = @BaseAssocID,
                @pShiftFactor = @TxLoanIdShiftFactor,
                @pCustomerIdShiftFactor = @CustomerIdShiftFactor;
        END
        ELSE
        BEGIN
            SET @DynInsertErr = N'INSERT INTO ' + @ErrorTableFull + N' SELECT * FROM ' + @CloneBaseFull + N';';
            EXEC(@DynInsertErr);
        END;
    END CATCH;
END
        ELSE
        BEGIN
    SET @SQL =
                N'INSERT INTO ' + @FullName + N' WITH (TABLOCK) ('
                + @ColumnListClone + N') '
                + N'SELECT ' + @SelectListClone + N' '
                + N'FROM ' + @CloneBaseFull + N' AS S '
                + N'CROSS JOIN #TargetAssociations AS TA '
                + N'WHERE TA.AssocID <> @pBaseAssocID;';

    BEGIN TRY
        EXEC sys.sp_executesql
              @SQL
            , N'@pBaseAssocID CHAR(3), @pShiftFactor INT, @pCustomerIdShiftFactor INT'
            , @pBaseAssocID = @BaseAssocID
            , @pShiftFactor = @TxLoanIdShiftFactor
            , @pCustomerIdShiftFactor = @CustomerIdShiftFactor;
    END TRY
    BEGIN CATCH
        SET @ErrorMsg = N'ERROR inserting into ' + @FullName + N': ' + ERROR_MESSAGE();
        RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

        SET @ErrorTableFull = QUOTENAME(@SchemaName) + N'.' + QUOTENAME(N'Error_' + @TableName);
        IF OBJECT_ID(@ErrorTableFull, 'U') IS NULL
        BEGIN
            IF @SelectListClone IS NOT NULL
            BEGIN
                SET @DynSelectInto =
                    N'SELECT ' + @SelectListClone
                    + N' INTO ' + @ErrorTableFull
                    + N' FROM ' + @CloneBaseFull + N' AS S CROSS JOIN #TargetAssociations AS TA WHERE TA.AssocID <> @pBaseAssocID;';

                EXEC sys.sp_executesql
                    @DynSelectInto,
                    N'@pBaseAssocID CHAR(3), @pShiftFactor INT, @pCustomerIdShiftFactor INT',
                    @pBaseAssocID = @BaseAssocID,
                    @pShiftFactor = @TxLoanIdShiftFactor,
                    @pCustomerIdShiftFactor = @CustomerIdShiftFactor;
            END
            ELSE
            BEGIN
                SET @DynCreate = N'SELECT TOP 0 * INTO ' + @ErrorTableFull + N' FROM ' + @CloneBaseFull + N';';
                EXEC(@DynCreate);
            END
        END;

        IF @SelectListClone IS NOT NULL
        BEGIN
            SET @DynInsertErr =
                N'INSERT INTO ' + @ErrorTableFull
                + N' SELECT ' + @SelectListClone
                + N' FROM ' + @CloneBaseFull + N' AS S CROSS JOIN #TargetAssociations AS TA WHERE TA.AssocID <> @pBaseAssocID;';

            EXEC sys.sp_executesql
                @DynInsertErr,
                N'@pBaseAssocID CHAR(3), @pShiftFactor INT, @pCustomerIdShiftFactor INT',
                @pBaseAssocID = @BaseAssocID,
                @pShiftFactor = @TxLoanIdShiftFactor,
                @pCustomerIdShiftFactor = @CustomerIdShiftFactor;
        END
        ELSE
        BEGIN
            SET @DynInsertErr = N'INSERT INTO ' + @ErrorTableFull + N' SELECT * FROM ' + @CloneBaseFull + N';';
            EXEC(@DynInsertErr);
        END;
    END CATCH;
END;

SET @RowsInsertedClone = @@ROWCOUNT;

BEGIN TRY
            SET @ResultMessage =
                N'REBUILD_CLONE_ASSOC_TABLE: Table=' + @FullName
                + N'; BaseAssocID=' + CAST(@BaseAssocID AS NVARCHAR(10))
                + N'; RowsInserted='
                + CAST(@RowsInsertedClone AS NVARCHAR(50));

            INSERT INTO [dbo].[SSISLoad]
    (PackageID, Result, StatusDT)
VALUES
    (@PackageID, @ResultMessage, GETDATE());
        END TRY
        BEGIN CATCH
        END CATCH;

------------------------------------------------------------
-- 2.8 Drop base clone table
------------------------------------------------------------
SET @SQL = N'DROP TABLE ' + @CloneBaseFull + N';';
EXEC sys.sp_executesql @SQL;

SET @ErrorMsg = N' Rebuild complete for ' + @FullName + N'. Base rows: ' + CAST(@RowsInsertedBase AS VARCHAR(20)) + N'; Clone rows: ' + CAST(@RowsInsertedClone AS VARCHAR(20)) + N'.';
RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

SET @RowNum2 = @RowNum2 + 1;
END;
-- WHILE

/****************************************************************
     STEP 2C: Rebuild each table with CustAssocID from BaseAssocID only
     CustomerId strategy:
       - For tables with CustomerId:
         L = max number of digits in CustomerId (base rows)
         F = 10^L
         NewCustTxLoanID = CAST(TA.AssocID AS INT) * F + CAST(S.CustomerId AS INT)
         Implemented via dbo.ufn_AssocDerivedID
       - For GUID columns in UNIQUE indexes (PK or otherwise):
         cloned rows use NEWID() to preserve uniqueness
    ****************************************************************/
DECLARE
          @RowNum2C             INT
        , @MaxRowNum2C          INT
        , @HasBaseRowsC         INT
        , @HasCustAssocID       BIT
        , @HasCustomerIDC       BIT
        , @CustomerIDCIsIdent   BIT
        , @CustTxLoanIdShiftFactor INT
        , @ColumnListBaseC      NVARCHAR(MAX)
        , @SelectListBaseC      NVARCHAR(MAX)
        , @ColumnListCloneC     NVARCHAR(MAX)
        , @SelectListCloneC     NVARCHAR(MAX)
        , @RowsInsertedBaseC    INT
        , @RowsInsertedCloneC   INT
        , @GuidUniqueColsC      NVARCHAR(MAX),
        @CloneBaseNameC nvarchar(max)

SELECT @MaxRowNum2C = MAX(RowNum)
FROM #TablesWithCustAssocID;

SET @RowNum2C = 1;

WHILE @RowNum2C <= @MaxRowNum2C
    BEGIN
    SELECT
        @SchemaName = SchemaName
            , @TableName  = TableName
    FROM #TablesWithCustAssocID
    WHERE RowNum = @RowNum2C;

    IF @SchemaName IS NULL
        BEGIN
        SET @RowNum2C = @RowNum2C + 1;
        CONTINUE;
    END;

    SET @FullNameC =
            QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@TableName);

    SET @ErrorMsg = N'STEP 2C: Rebuilding table ' + @FullNameC + N' from base AssocID = ' + @BaseAssocID;
    RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

----------------------------------------------------------------
-- SAFETY RULE:
-- If a table has neither CustAssocID nor CustomerId, its data must
-- not be changed by this cloning phase.
----------------------------------------------------------------
IF NOT EXISTS
        (
            SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID(@FullNameC)
        AND name      = N'CustAssocID'
        )
    AND NOT EXISTS
        (
            SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID(@FullNameC)
        AND name      = N'CustomerId'
        )
        BEGIN
    SET @ErrorMsg = N' Skipping: table ' + @FullNameC + N' has no CustAssocID and no CustomerId; data unchanged.';
    RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

SET @RowNum2C = @RowNum2C + 1;
CONTINUE;
END;

----------------------------------------------------------------
-- LOG GUID UNIQUE KEY USAGE (PKs and other UNIQUE indexes)
----------------------------------------------------------------
SET @GuidUniqueColsC = NULL;

SELECT
    @GuidUniqueColsC =
                STRING_AGG(QUOTENAME(c.name), N', ')
FROM sys.indexes AS ix
    JOIN sys.index_columns AS ic
    ON ix.object_id = ic.object_id
        AND ix.index_id  = ic.index_id
    JOIN sys.columns AS c
    ON c.object_id  = ix.object_id
        AND c.column_id  = ic.column_id
WHERE
              ix.object_id      = OBJECT_ID(@FullNameC)
    AND ix.is_unique      = 1
    AND c.system_type_id  = TYPE_ID(N'uniqueidentifier');

IF @GuidUniqueColsC IS NOT NULL
        BEGIN
    BEGIN TRY
                SET @ResultMessage =
                    N'GUID_UNIQUE_KEY_DETECTED: Table=' + @FullNameC
                    + N'; Columns=' + @GuidUniqueColsC
                    + N'; Cloned rows will use NEWID() for these GUID unique keys.';

                INSERT INTO [dbo].[SSISLoad]
        (PackageID, Result, StatusDT)
    VALUES
        (@PackageID, @ResultMessage, GETDATE());
            END TRY
            BEGIN CATCH
                -- Do not fail procedure on logging issues
            END CATCH;
END;

------------------------------------------------------------
-- 2C.1 Detect CustAssocID and CustomerId presence and identity
------------------------------------------------------------
SET @HasCustAssocID       = 0;
SET @HasCustomerIDC      = 0;
SET @CustomerIDCIsIdent  = 0;
SET @CustTxLoanIdShiftFactor = NULL;

IF EXISTS
        (
            SELECT 1
FROM sys.columns
WHERE object_id = OBJECT_ID(@FullNameC)
    AND name      = N'CustAssocID'
        )
        BEGIN
    SET @HasCustAssocID = 1;
END;

IF EXISTS
        (
            SELECT 1
FROM sys.columns
WHERE object_id = OBJECT_ID(@FullNameC)
    AND name      = N'CustomerId'
        )
        BEGIN
    SET @HasCustomerIDC = 1;

    IF EXISTS
            (
                SELECT 1
    FROM sys.columns
    WHERE object_id   = OBJECT_ID(@FullNameC)
        AND name        = N'CustomerId'
        AND is_identity = 1
            )
            BEGIN
        SET @CustomerIDCIsIdent = 1;
    END;
END;

------------------------------------------------------------
-- 2C.2 Check if table has any base rows for @BaseAssocID
------------------------------------------------------------
SET @SQL =
            N'SELECT @HasBaseRows_OUT = COUNT(*) '
            + N'FROM ' + @FullNameC + N' '
            + N'WHERE CustAssocID = @pBaseAssocID;';

SET @HasBaseRowsC = 0;

EXEC sys.sp_executesql
              @SQL
            , N'@pBaseAssocID CHAR(3), @HasBaseRows_OUT INT OUTPUT'
            , @pBaseAssocID    = @BaseAssocID
            , @HasBaseRows_OUT = @HasBaseRowsC OUTPUT;

IF @HasBaseRowsC = 0
        BEGIN
    SET @ErrorMsg = N' Skipping: no base rows with CustAssocID = ' + @BaseAssocID;
    RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;
SET @RowNum2C = @RowNum2C + 1;
CONTINUE;
END;

------------------------------------------------------------
-- 2C.3 Create base clone table with only @BaseAssocID rows
------------------------------------------------------------
SET @CloneBaseNameC = N'CloneBase_' + @TableName;
SET @CloneBaseFullC =
            QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@CloneBaseNameC);

IF OBJECT_ID(@CloneBaseFullC, 'U') IS NOT NULL
        BEGIN
    SET @SQL = N'DROP TABLE ' + @CloneBaseFullC + N';';
    EXEC sys.sp_executesql @SQL;
END;

SET @SQL =
            N'SELECT * '
            + N'INTO ' + @CloneBaseFullC + N' '
            + N'FROM ' + @FullNameC + N' AS S '
            + N'WHERE S.CustAssocID = @pBaseAssocID;';

EXEC sys.sp_executesql
              @SQL
            , N'@pBaseAssocID CHAR(3)'
            , @pBaseAssocID = @BaseAssocID;

------------------------------------------------------------
-- 2C.3a Build CustomerId shift factor if CustomerId exists
------------------------------------------------------------
IF @HasCustomerIDC = 1
        BEGIN
    DECLARE @CustTxLoanIdLen INT;

    SET @SQL =
                N'SELECT @Len_OUT = MAX(LEN(CAST([CustomerId] AS VARCHAR(20)))) '
                + N'FROM ' + @CloneBaseFullC + N';';

    SET @CustTxLoanIdLen = NULL;

    EXEC sys.sp_executesql
                  @SQL
                , N'@Len_OUT INT OUTPUT'
                , @Len_OUT = @CustTxLoanIdLen OUTPUT;

    IF @CustTxLoanIdLen IS NULL OR @CustTxLoanIdLen <= 0
                SET @CustTxLoanIdLen = 1;

    SET @CustTxLoanIdShiftFactor =
                CONVERT(INT, POWER(10.0, @CustTxLoanIdLen));
END;

------------------------------------------------------------
-- 2C.4 Truncate original table (all rows removed)
------------------------------------------------------------
SET @SQL = N'TRUNCATE TABLE ' + @FullNameC + N';';
EXEC sys.sp_executesql @SQL;

------------------------------------------------------------
-- 2C.5 Build column lists for base reinsert and cloned inserts
------------------------------------------------------------
SET @ColumnListBaseC  = NULL;
SET @SelectListBaseC  = NULL;
SET @ColumnListCloneC = NULL;
SET @SelectListCloneC = NULL;

IF @HasCustomerIDC = 1
        BEGIN
    -- With CustomerId: use UDF for CustomerId; NEWID() for GUID unique keys
    SELECT
        @ColumnListBaseC =
                    STRING_AGG(
                        CAST(QUOTENAME(c.name) AS NVARCHAR(MAX)),
                        N', '
                    ) WITHIN GROUP (ORDER BY c.column_id)
                , @SelectListBaseC =
                    STRING_AGG(
                        CAST(N'S.' + QUOTENAME(c.name) AS NVARCHAR(MAX)),
                        N', '
                    ) WITHIN GROUP (ORDER BY c.column_id)
                , @ColumnListCloneC =
                    STRING_AGG(
                        CAST(QUOTENAME(c.name) AS NVARCHAR(MAX)),
                        N', '
                    ) WITHIN GROUP (ORDER BY c.column_id)
                , @SelectListCloneC =
                    STRING_AGG(
                        CAST(
                            CASE
                                WHEN c.name = N'CustAssocID'
                                    THEN N'TA.AssocID'
                                WHEN c.name = N'CustomerId'
                                    THEN N'dbo.ufn_AssocDerivedID(S.[CustomerId], TA.AssocID, @pShiftFactor)'
                                WHEN uq.IsGuidUnique = 1
                                    THEN N'NEWID()'
                                ELSE N'S.' + QUOTENAME(c.name)
                            END
                            AS NVARCHAR(MAX)
                        ),
                        N', '
                    ) WITHIN GROUP (ORDER BY c.column_id)
    FROM sys.columns AS c
            OUTER APPLY
            (
                -- GUID columns participating in ANY UNIQUE index (PK or other)
                SELECT TOP (1)
            1 AS IsGuidUnique
        FROM sys.indexes AS ix
            JOIN sys.index_columns AS ic
            ON ix.object_id = ic.object_id
                AND ix.index_id  = ic.index_id
            JOIN sys.columns AS c_orig
            ON c_orig.object_id = ix.object_id
                AND c_orig.column_id = ic.column_id
        WHERE
                      ix.object_id     = OBJECT_ID(@FullNameC)
            AND ix.is_unique     = 1
            AND c_orig.name      = c.name
            AND c_orig.system_type_id = TYPE_ID(N'uniqueidentifier')
            ) AS uq
    WHERE
                  c.object_id   = OBJECT_ID(@CloneBaseFullC)
        AND c.is_computed = 0;
END
        ELSE
        BEGIN
    -- No CustomerId: NEWID() for GUID unique keys; CustAssocID cloned
    SELECT
        @ColumnListBaseC =
                    STRING_AGG(
                        CAST(QUOTENAME(c.name) AS NVARCHAR(MAX)),
                        N', '
                    ) WITHIN GROUP (ORDER BY c.column_id)
                , @SelectListBaseC =
                    STRING_AGG(
                        CAST(N'S.' + QUOTENAME(c.name) AS NVARCHAR(MAX)),
                        N', '
                    ) WITHIN GROUP (ORDER BY c.column_id)
                , @ColumnListCloneC =
                    STRING_AGG(
                        CAST(QUOTENAME(c.name) AS NVARCHAR(MAX)),
                        N', '
                    ) WITHIN GROUP (ORDER BY c.column_id)
                , @SelectListCloneC =
                    STRING_AGG(
                        CAST(
                            CASE
                                WHEN c.name = N'CustAssocID'
                                    THEN N'TA.AssocID'
                                WHEN uq.IsGuidUnique = 1
                                    THEN N'NEWID()'
                                ELSE N'S.' + QUOTENAME(c.name)
                            END
                            AS NVARCHAR(MAX)
                        ),
                        N', '
                    ) WITHIN GROUP (ORDER BY c.column_id)
    FROM sys.columns AS c
            OUTER APPLY
            (
                -- GUID columns participating in ANY UNIQUE index (PK or other)
                SELECT TOP (1)
            1 AS IsGuidUnique
        FROM sys.indexes AS ix
            JOIN sys.index_columns AS ic
            ON ix.object_id = ic.object_id
                AND ix.index_id  = ic.index_id
            JOIN sys.columns AS c_orig
            ON c_orig.object_id = ix.object_id
                AND c_orig.column_id = ic.column_id
        WHERE
                      ix.object_id     = OBJECT_ID(@FullNameC)
            AND ix.is_unique     = 1
            AND c_orig.name      = c.name
            AND c_orig.system_type_id = TYPE_ID(N'uniqueidentifier')
            ) AS uq
    WHERE
                  c.object_id   = OBJECT_ID(@CloneBaseFullC)
        AND c.is_identity = 0
        AND c.is_computed = 0;
END;

IF @ColumnListBaseC IS NULL OR LEN(@ColumnListBaseC) = 0
        BEGIN
    RAISERROR(N' Skipping: table has only identity/computed columns.', 0, 1) WITH NOWAIT;

    SET @SQL = N'DROP TABLE ' + @CloneBaseFullC + N';';
    EXEC sys.sp_executesql @SQL;

    SET @RowNum2C = @RowNum2C + 1;
    CONTINUE;
END;

------------------------------------------------------------
-- 2C.6 Insert base rows back (CustAssocID = Base, original CustomerId/keys)
------------------------------------------------------------
SET @RowsInsertedBaseC  = 0;
SET @RowsInsertedCloneC = 0;

IF (
        (@HasCustomerIDC = 1 AND @CustomerIDCIsIdent = 1)
    OR  (
            OBJECT_ID('tempdb..#IdentityMaxValues') IS NOT NULL
        AND EXISTS(
                SELECT 1
                FROM tempdb..#IdentityMaxValues imv
                JOIN sys.columns sc ON sc.object_id = OBJECT_ID(@FullNameC) AND sc.name = imv.ColumnName AND sc.is_identity = 1
                WHERE imv.SchemaName = @SchemaName AND imv.TableName = @TableName
            )
        )
)
BEGIN
    SET @SQL =
        N'SET IDENTITY_INSERT ' + @FullNameC + N' ON; '
        + N'INSERT INTO ' + @FullNameC + N' WITH (TABLOCK) ('
        + @ColumnListBaseC + N') '
        + N'SELECT ' + @SelectListBaseC + N' '
        + N'FROM ' + @CloneBaseFullC + N' AS S; '
        + N'SET IDENTITY_INSERT ' + @FullNameC + N' OFF;';

    BEGIN TRY
        EXEC sys.sp_executesql @SQL;
    END TRY
    BEGIN CATCH
        SET @ErrorMsg = N'ERROR inserting base rows into ' + @FullNameC + N': ' + ERROR_MESSAGE();
        RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

        SET @ErrorTableFullC = QUOTENAME(@SchemaName) + N'.' + QUOTENAME(N'Error_' + @TableName);
        IF OBJECT_ID(@ErrorTableFullC, 'U') IS NULL
        BEGIN
            IF @SelectListBaseC IS NOT NULL
            BEGIN
                SET @DynSelectIntoC =
                    N'SELECT ' + @SelectListBaseC
                    + N' INTO ' + @ErrorTableFullC
                    + N' FROM ' + @CloneBaseFullC + N' AS S;';

                EXEC(@DynSelectIntoC);
            END
            ELSE
            BEGIN
                SET @DynCreateC = N'SELECT TOP 0 * INTO ' + @ErrorTableFullC + N' FROM ' + @CloneBaseFullC + N';';
                EXEC(@DynCreateC);
            END
        END;

        IF @SelectListBaseC IS NOT NULL
        BEGIN
            SET @DynInsertErrC =
                N'INSERT INTO ' + @ErrorTableFullC
                + N' SELECT ' + @SelectListBaseC
                + N' FROM ' + @CloneBaseFullC + N' AS S;';

            EXEC(@DynInsertErrC);
        END
        ELSE
        BEGIN
            SET @DynInsertErrC = N'INSERT INTO ' + @ErrorTableFullC + N' SELECT * FROM ' + @CloneBaseFullC + N';';
            EXEC(@DynInsertErrC);
        END;
    END CATCH;
END
ELSE
BEGIN
    SET @SQL =
        N'INSERT INTO ' + @FullNameC + N' WITH (TABLOCK) ('
        + @ColumnListBaseC + N') '
        + N'SELECT ' + @SelectListBaseC + N' '
        + N'FROM ' + @CloneBaseFullC + N' AS S;';

    BEGIN TRY
        EXEC sys.sp_executesql @SQL;
    END TRY
    BEGIN CATCH
        SET @ErrorMsg = N'ERROR inserting base rows into ' + @FullNameC + N': ' + ERROR_MESSAGE();
        RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

        SET @ErrorTableFullC = QUOTENAME(@SchemaName) + N'.' + QUOTENAME(N'Error_' + @TableName);
        IF OBJECT_ID(@ErrorTableFullC, 'U') IS NULL
        BEGIN
            IF @SelectListBaseC IS NOT NULL
            BEGIN
                SET @DynSelectIntoC =
                    N'SELECT ' + @SelectListBaseC
                    + N' INTO ' + @ErrorTableFullC
                    + N' FROM ' + @CloneBaseFullC + N' AS S;';

                EXEC(@DynSelectIntoC);
            END
            ELSE
            BEGIN
                SET @DynCreateC = N'SELECT TOP 0 * INTO ' + @ErrorTableFullC + N' FROM ' + @CloneBaseFullC + N';';
                EXEC(@DynCreateC);
            END
        END;

        IF @SelectListBaseC IS NOT NULL
        BEGIN
            SET @DynInsertErrC =
                N'INSERT INTO ' + @ErrorTableFullC
                + N' SELECT ' + @SelectListBaseC
                + N' FROM ' + @CloneBaseFullC + N' AS S;';

            EXEC(@DynInsertErrC);
        END
        ELSE
        BEGIN
            SET @DynInsertErrC = N'INSERT INTO ' + @ErrorTableFullC + N' SELECT * FROM ' + @CloneBaseFullC + N';';
            EXEC(@DynInsertErrC);
        END;
    END CATCH;
END;

SET @RowsInsertedBaseC = @@ROWCOUNT;

BEGIN TRY
            SET @ResultMessage =
                N'REBUILD_BASE_CUST_ASSOC_TABLE: Table=' + @FullNameC
                + N'; BaseAssocID=' + CAST(@BaseAssocID AS NVARCHAR(10))
                + N'; RowsInserted='
                + CAST(@RowsInsertedBaseC AS NVARCHAR(50));

            INSERT INTO [dbo].[SSISLoad]
    (PackageID, Result, StatusDT)
VALUES
    (@PackageID, @ResultMessage, GETDATE());
        END TRY
        BEGIN CATCH
        END CATCH;

------------------------------------------------------------
-- 2C.7 Insert cloned rows for all target AssocIDs (excluding base)
------------------------------------------------------------
IF (
        (@HasCustomerIDC = 1 AND @CustomerIDCIsIdent = 1)
    OR  (
            OBJECT_ID('tempdb..#IdentityMaxValues') IS NOT NULL
        AND EXISTS(
                SELECT 1
                FROM tempdb..#IdentityMaxValues imv
                JOIN sys.columns sc ON sc.object_id = OBJECT_ID(@FullNameC) AND sc.name = imv.ColumnName AND sc.is_identity = 1
                WHERE imv.SchemaName = @SchemaName AND imv.TableName = @TableName
            )
        )
)
        BEGIN
    SET @SQL =
                N'SET IDENTITY_INSERT ' + @FullNameC + N' ON; '
                + N'INSERT INTO ' + @FullNameC + N' WITH (TABLOCK) ('
                + @ColumnListCloneC + N') '
                + N'SELECT ' + @SelectListCloneC + N' '
                + N'FROM ' + @CloneBaseFullC + N' AS S '
                + N'CROSS JOIN #TargetAssociations AS TA '
                + N'WHERE TA.AssocID <> @pBaseAssocID; '
                + N'SET IDENTITY_INSERT ' + @FullNameC + N' OFF;';

    BEGIN TRY
        EXEC sys.sp_executesql
              @SQL
            , N'@pBaseAssocID CHAR(3), @pShiftFactor INT'
            , @pBaseAssocID = @BaseAssocID
            , @pShiftFactor = @CustTxLoanIdShiftFactor;
    END TRY
    BEGIN CATCH
        SET @ErrorMsg = N'ERROR inserting into ' + @FullNameC + N': ' + ERROR_MESSAGE();
        RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

        SET @ErrorTableFullC = QUOTENAME(@SchemaName) + N'.' + QUOTENAME(N'Error_' + @TableName);
        IF OBJECT_ID(@ErrorTableFullC, 'U') IS NULL
        BEGIN
            IF @SelectListCloneC IS NOT NULL
            BEGIN
                SET @DynSelectIntoC =
                    N'SELECT ' + @SelectListCloneC
                    + N' INTO ' + @ErrorTableFullC
                    + N' FROM ' + @CloneBaseFullC + N' AS S CROSS JOIN #TargetAssociations AS TA WHERE TA.AssocID <> @pBaseAssocID;';

                EXEC sys.sp_executesql
                    @DynSelectIntoC,
                    N'@pBaseAssocID CHAR(3), @pShiftFactor INT',
                    @pBaseAssocID = @BaseAssocID,
                    @pShiftFactor = @CustTxLoanIdShiftFactor;
            END
            ELSE
            BEGIN
                SET @DynCreateC = N'SELECT TOP 0 * INTO ' + @ErrorTableFullC + N' FROM ' + @CloneBaseFullC + N';';
                EXEC(@DynCreateC);
            END
        END;

        IF @SelectListCloneC IS NOT NULL
        BEGIN
            SET @DynInsertErrC =
                N'INSERT INTO ' + @ErrorTableFullC
                + N' SELECT ' + @SelectListCloneC
                + N' FROM ' + @CloneBaseFullC + N' AS S CROSS JOIN #TargetAssociations AS TA WHERE TA.AssocID <> @pBaseAssocID;';

            EXEC sys.sp_executesql
                @DynInsertErrC,
                N'@pBaseAssocID CHAR(3), @pShiftFactor INT',
                @pBaseAssocID = @BaseAssocID,
                @pShiftFactor = @CustTxLoanIdShiftFactor;
        END
        ELSE
        BEGIN
            SET @DynInsertErrC = N'INSERT INTO ' + @ErrorTableFullC + N' SELECT * FROM ' + @CloneBaseFullC + N';';
            EXEC(@DynInsertErrC);
        END;
    END CATCH;
END
        ELSE
        BEGIN
            SET @SQL =
                N'INSERT INTO ' + @FullNameC + N' WITH (TABLOCK) ('
                + @ColumnListCloneC + N') '
                + N'SELECT ' + @SelectListCloneC + N' '
                + N'FROM ' + @CloneBaseFullC + N' AS S '
                + N'CROSS JOIN #TargetAssociations AS TA '
                + N'WHERE TA.AssocID <> @pBaseAssocID;';

            BEGIN TRY
                EXEC sys.sp_executesql
                      @SQL
                    , N'@pBaseAssocID CHAR(3), @pShiftFactor INT'
                    , @pBaseAssocID = @BaseAssocID
                    , @pShiftFactor = @CustTxLoanIdShiftFactor;
            END TRY
            BEGIN CATCH
                SET @ErrorMsg = N'ERROR inserting into ' + @FullNameC + N': ' + ERROR_MESSAGE();
                RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

                SET @ErrorTableFullC = QUOTENAME(@SchemaName) + N'.' + QUOTENAME(N'Error_' + @TableName);
                IF OBJECT_ID(@ErrorTableFullC, 'U') IS NULL
                BEGIN
                    IF @SelectListCloneC IS NOT NULL
                    BEGIN
                        SET @DynSelectIntoC =
                            N'SELECT ' + @SelectListCloneC
                            + N' INTO ' + @ErrorTableFullC
                            + N' FROM ' + @CloneBaseFullC + N' AS S CROSS JOIN #TargetAssociations AS TA WHERE TA.AssocID <> @pBaseAssocID;';

                        EXEC sys.sp_executesql
                            @DynSelectIntoC,
                            N'@pBaseAssocID CHAR(3), @pShiftFactor INT',
                            @pBaseAssocID = @BaseAssocID,
                            @pShiftFactor = @CustTxLoanIdShiftFactor;
                    END
                    ELSE
                    BEGIN
                        SET @DynCreateC = N'SELECT TOP 0 * INTO ' + @ErrorTableFullC + N' FROM ' + @CloneBaseFullC + N';';
                        EXEC(@DynCreateC);
                    END
                END;

                IF @SelectListCloneC IS NOT NULL
                BEGIN
                    SET @DynInsertErrC =
                        N'INSERT INTO ' + @ErrorTableFullC
                        + N' SELECT ' + @SelectListCloneC
                        + N' FROM ' + @CloneBaseFullC + N' AS S CROSS JOIN #TargetAssociations AS TA WHERE TA.AssocID <> @pBaseAssocID;';

                    EXEC sys.sp_executesql
                        @DynInsertErrC,
                        N'@pBaseAssocID CHAR(3), @pShiftFactor INT',
                        @pBaseAssocID = @BaseAssocID,
                        @pShiftFactor = @CustTxLoanIdShiftFactor;
                END
                ELSE
                BEGIN
                    SET @DynInsertErrC = N'INSERT INTO ' + @ErrorTableFullC + N' SELECT * FROM ' + @CloneBaseFullC + N';';
                    EXEC(@DynInsertErrC);
                END;
            END CATCH;
        END;

SET @RowsInsertedCloneC = @@ROWCOUNT;

-- Progress indicator
DECLARE @PercentComplete DECIMAL(5,2) = (@RowNum2C * 100.0) / @MaxRowNum2C;
SET @ErrorMsg = N'STEP 2C.7: Progress ' + CAST(@PercentComplete AS VARCHAR(10)) + N'% complete (' + CAST(@RowNum2C AS VARCHAR(10)) + N'/' + CAST(@MaxRowNum2C AS VARCHAR(10)) + N' tables processed)';
RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

BEGIN TRY
            SET @ResultMessage =
                N'REBUILD_CLONE_CUST_ASSOC_TABLE: Table=' + @FullNameC
                + N'; BaseAssocID=' + CAST(@BaseAssocID AS NVARCHAR(10))
                + N'; RowsInserted='
                + CAST(@RowsInsertedCloneC AS NVARCHAR(50));

            INSERT INTO [dbo].[SSISLoad]
    (PackageID, Result, StatusDT)
VALUES
    (@PackageID, @ResultMessage, GETDATE());
        END TRY
        BEGIN CATCH
        END CATCH;

------------------------------------------------------------
-- 2C.8 Drop base clone table
------------------------------------------------------------
SET @SQL = N'DROP TABLE ' + @CloneBaseFullC + N';';
EXEC sys.sp_executesql @SQL;

SET @ErrorMsg = N' Rebuild complete for ' + @FullNameC + N'. Base rows: ' + CAST(@RowsInsertedBaseC AS VARCHAR(20)) + N'; Clone rows: ' + CAST(@RowsInsertedCloneC AS VARCHAR(20)) + N'.';
RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

SET @RowNum2C = @RowNum2C + 1;
END;
-- WHILE

/****************************************************************
     STEP 2B: Rebuild tables with only TxLoanID (no AssocID, no CustomerId)
    ****************************************************************/
IF OBJECT_ID('tempdb..#TablesWithOnlyTxLoanID') IS NOT NULL DROP TABLE #TablesWithOnlyTxLoanID;
CREATE TABLE #TablesWithOnlyTxLoanID
(
    RowNum INT IDENTITY(1,1) PRIMARY KEY,
    SchemaName SYSNAME,
    TableName SYSNAME
);

INSERT INTO #TablesWithOnlyTxLoanID
    (SchemaName, TableName)
SELECT DISTINCT s.name, t.name
FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.is_ms_shipped = 0
    AND RIGHT(t.name, 2) <> '_A'
    AND EXISTS (SELECT 1
    FROM sys.columns c
    WHERE c.object_id = t.object_id AND c.name = 'TxLoanID')
    AND NOT EXISTS (SELECT 1
    FROM sys.columns c
    WHERE c.object_id = t.object_id AND c.name = 'AssocID')
    AND NOT EXISTS (SELECT 1
    FROM sys.columns c
    WHERE c.object_id = t.object_id AND c.name = 'CustomerId');

DECLARE @RowNum2B INT, @MaxRowNum2B INT;
SELECT @MaxRowNum2B = MAX(RowNum)
FROM #TablesWithOnlyTxLoanID;
SET @RowNum2B = 1;

WHILE @RowNum2B <= @MaxRowNum2B
    BEGIN
    SELECT @SchemaName = SchemaName, @TableName = TableName
    FROM #TablesWithOnlyTxLoanID
    WHERE RowNum = @RowNum2B;
    IF @SchemaName IS NULL BEGIN
        SET @RowNum2B = @RowNum2B + 1;
        CONTINUE;
    END;
    SET @FullName = QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName);

    -- Check for base rows
    SET @SQL = N'SELECT @HasBaseRows_OUT = COUNT(*) FROM ' + @FullName + N' WHERE TxLoanID IN (SELECT TxLoanID FROM dbo.tblTxLoans WHERE AssocID = @pBaseAssocID);';
    SET @HasBaseRows = 0;
    EXEC sys.sp_executesql @SQL, N'@pBaseAssocID CHAR(3), @HasBaseRows_OUT INT OUTPUT', @BaseAssocID, @HasBaseRows OUTPUT;
    IF @HasBaseRows = 0 BEGIN
        SET @RowNum2B = @RowNum2B + 1;
        CONTINUE;
    END;

    -- Clone base rows
    SET @CloneBaseName = N'CloneBase_' + @TableName;
    SET @CloneBaseFull = QUOTENAME(@SchemaName) + '.' + QUOTENAME(@CloneBaseName);
    IF OBJECT_ID(@CloneBaseFull, 'U') IS NOT NULL EXEC('DROP TABLE ' + @CloneBaseFull);
    SET @SQL = N'SELECT * INTO ' + @CloneBaseFull + N' FROM ' + @FullName + N' WHERE TxLoanID IN (SELECT TxLoanID FROM dbo.tblTxLoans WHERE AssocID = @pBaseAssocID);';
    BEGIN TRY
        EXEC sys.sp_executesql @SQL, N'@pBaseAssocID CHAR(3)', @BaseAssocID;
    END TRY
    BEGIN CATCH
        SET @ErrorMsg = N'ERROR creating clone base for ' + @FullName + N': ' + ERROR_MESSAGE();
        RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;
        SET @ErrorTableFull = QUOTENAME(@SchemaName) + N'.' + QUOTENAME(N'Error_' + @TableName);
        IF OBJECT_ID(@ErrorTableFull, 'U') IS NULL
        BEGIN
            SET @DynCreate = N'SELECT TOP 0 * INTO ' + @ErrorTableFull + N' FROM ' + @CloneBaseFull + N';';
            EXEC(@DynCreate);
        END;
        SET @DynInsertErr = N'INSERT INTO ' + @ErrorTableFull + N' SELECT * FROM ' + @CloneBaseFull + N';';
        EXEC(@DynInsertErr);
    END CATCH;

    -- Compute shift factor
    SET @SQL = N'SELECT @Len_OUT = MAX(LEN(CAST(TxLoanID AS VARCHAR(20)))) FROM ' + @CloneBaseFull + N';';
    SET @TxLoanIdLen = NULL;
    EXEC sys.sp_executesql @SQL, N'@Len_OUT INT OUTPUT', @TxLoanIdLen OUTPUT;
    IF @TxLoanIdLen IS NULL OR @TxLoanIdLen <= 0 SET @TxLoanIdLen = 1;
    SET @TxLoanIdShiftFactor = CONVERT(INT, POWER(10.0, @TxLoanIdLen));

    -- Drop original and rename clone
    EXEC('DROP TABLE ' + @FullName);
    EXEC sys.sp_rename @CloneBaseFull, @TableName;

    -- Build column lists for insert (apply #IdentityMaxValues transforms if present)
    IF OBJECT_ID('tempdb..#IdentityMaxValues') IS NOT NULL
    BEGIN
        SELECT
            @ColumnListClone = STRING_AGG(QUOTENAME(c.name), ', ') WITHIN GROUP (ORDER BY c.column_id),
            @SelectListClone = STRING_AGG(
                    CAST(
                        CASE
                            WHEN imv.ColumnName IS NOT NULL
                                THEN N'dbo.ufn_AssocDerivedID(S.' + QUOTENAME(c.name) + N', TA.AssocID, ' + CAST(ISNULL(imv.ShiftFactor, 0) AS NVARCHAR(20)) + N')'
                            WHEN c.name = 'TxLoanID' THEN 'dbo.ufn_AssocDerivedID(S.TxLoanID, TA.AssocID, @pShiftFactor)'
                            WHEN uq.IsGuidUnique = 1 THEN 'NEWID()'
                            ELSE 'S.' + QUOTENAME(c.name)
                        END
                    AS NVARCHAR(MAX)), ', '
                ) WITHIN GROUP (ORDER BY c.column_id)
        FROM sys.columns c
            OUTER APPLY (
                SELECT TOP 1 1 AS IsGuidUnique
                FROM sys.indexes ix
                    JOIN sys.index_columns ic ON ix.object_id = ic.object_id AND ix.index_id = ic.index_id
                    JOIN sys.columns c_orig ON c_orig.object_id = ix.object_id AND c_orig.column_id = ic.column_id
                WHERE ix.object_id = OBJECT_ID(@FullName)
                    AND ix.is_unique = 1
                    AND c_orig.name = c.name
                    AND c_orig.system_type_id = TYPE_ID(N'uniqueidentifier')
            ) uq
            LEFT JOIN #IdentityMaxValues imv
                ON imv.SchemaName = @SchemaName AND imv.TableName = @TableName AND imv.ColumnName = c.name
        WHERE c.object_id = OBJECT_ID(@FullName) AND c.is_computed = 0;
    END
    ELSE
    BEGIN
        SELECT
            @ColumnListClone = STRING_AGG(QUOTENAME(c.name), ', ') WITHIN GROUP (ORDER BY c.column_id),
            @SelectListClone = STRING_AGG(
                    CASE
                        WHEN c.name = 'TxLoanID' THEN 'dbo.ufn_AssocDerivedID(S.TxLoanID, TA.AssocID, @pShiftFactor)'
                        WHEN uq.IsGuidUnique = 1 THEN 'NEWID()'
                        ELSE 'S.' + QUOTENAME(c.name)
                    END, ', '
                ) WITHIN GROUP (ORDER BY c.column_id)
        FROM sys.columns c
            OUTER APPLY (
                SELECT TOP 1 1 AS IsGuidUnique
                FROM sys.indexes ix
                    JOIN sys.index_columns ic ON ix.object_id = ic.object_id AND ix.index_id = ic.index_id
                    JOIN sys.columns c_orig ON c_orig.object_id = ix.object_id AND c_orig.column_id = ic.column_id
                WHERE ix.object_id = OBJECT_ID(@FullName)
                    AND ix.is_unique = 1
                    AND c_orig.name = c.name
                    AND c_orig.system_type_id = TYPE_ID(N'uniqueidentifier')
            ) uq
        WHERE c.object_id = OBJECT_ID(@FullName) AND c.is_computed = 0;
    END;

    -- Insert cloned rows for all target AssocIDs
    SET @SQL = N'INSERT INTO ' + @FullName + N' (' + @ColumnListClone + N') SELECT ' + @SelectListClone + N' FROM ' + @FullName + N' AS S CROSS JOIN #TargetAssociations TA WHERE TA.AssocID <> @pBaseAssocID;';
    BEGIN TRY
        EXEC sys.sp_executesql @SQL, N'@pBaseAssocID CHAR(3), @pShiftFactor INT', @BaseAssocID, @TxLoanIdShiftFactor;
    END TRY
    BEGIN CATCH
        SET @ErrorMsg = N'ERROR inserting into ' + @FullName + N': ' + ERROR_MESSAGE();
        RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;
        SET @ErrorTableFull = QUOTENAME(@SchemaName) + N'.' + QUOTENAME(N'Error_' + @TableName);
        IF OBJECT_ID(@ErrorTableFull, 'U') IS NULL
        BEGIN
            IF @SelectListClone IS NOT NULL
            BEGIN
                SET @DynSelectInto =
                    N'SELECT ' + @SelectListClone
                    + N' INTO ' + @ErrorTableFull
                    + N' FROM ' + @FullName + N' AS S CROSS JOIN #TargetAssociations AS TA WHERE TA.AssocID <> @pBaseAssocID;';

                EXEC sys.sp_executesql @DynSelectInto, N'@pBaseAssocID CHAR(3), @pShiftFactor INT', @pBaseAssocID = @BaseAssocID, @pShiftFactor = @TxLoanIdShiftFactor;
            END
            ELSE
            BEGIN
                SET @DynCreate = N'SELECT TOP 0 * INTO ' + @ErrorTableFull + N' FROM ' + @FullName + N';';
                EXEC(@DynCreate);
            END
        END;

        IF @SelectListClone IS NOT NULL
        BEGIN
            SET @DynInsertErr =
                N'INSERT INTO ' + @ErrorTableFull
                + N' SELECT ' + @SelectListClone
                + N' FROM ' + @FullName + N' AS S CROSS JOIN #TargetAssociations AS TA WHERE TA.AssocID <> @pBaseAssocID;';

            EXEC sys.sp_executesql @DynInsertErr, N'@pBaseAssocID CHAR(3), @pShiftFactor INT', @pBaseAssocID = @BaseAssocID, @pShiftFactor = @TxLoanIdShiftFactor;
        END
        ELSE
        BEGIN
            SET @DynInsertErr = N'INSERT INTO ' + @ErrorTableFull + N' SELECT * FROM ' + @FullName + N';';
            EXEC(@DynInsertErr);
        END;
    END CATCH;

    -- Logging placeholder
    -- INSERT INTO [dbo].[SSISLoad] (PackageID, Result, StatusDT) VALUES (@PackageID, N'Cloned and rebuilt ' + @FullName, GETDATE());

    SET @RowNum2B = @RowNum2B + 1;
END;

/****************************************************************
     STEP 2D: Rebuild tables with only CustomerId (no AssocID, no TxLoanID)
    ****************************************************************/
IF OBJECT_ID('tempdb..#TablesWithOnlyCustomerId') IS NOT NULL DROP TABLE #TablesWithOnlyCustomerId;
CREATE TABLE #TablesWithOnlyCustomerId
(
    RowNum INT IDENTITY(1,1) PRIMARY KEY,
    SchemaName SYSNAME,
    TableName SYSNAME
);

INSERT INTO #TablesWithOnlyCustomerId
    (SchemaName, TableName)
SELECT DISTINCT s.name, t.name
FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.is_ms_shipped = 0
    AND RIGHT(t.name, 2) <> '_A'
    AND EXISTS (SELECT 1
    FROM sys.columns c
    WHERE c.object_id = t.object_id AND c.name = 'CustomerId')
    AND NOT EXISTS (SELECT 1
    FROM sys.columns c
    WHERE c.object_id = t.object_id AND c.name = 'AssocID')
    AND NOT EXISTS (SELECT 1
    FROM sys.columns c
    WHERE c.object_id = t.object_id AND c.name = 'TxLoanID');

DECLARE @RowNum2D INT, @MaxRowNum2D INT;
SELECT @MaxRowNum2D = MAX(RowNum)
FROM #TablesWithOnlyCustomerId;
SET @RowNum2D = 1;

WHILE @RowNum2D <= @MaxRowNum2D
    BEGIN
    SELECT @SchemaName = SchemaName, @TableName = TableName
    FROM #TablesWithOnlyCustomerId
    WHERE RowNum = @RowNum2D;
    IF @SchemaName IS NULL BEGIN
        SET @RowNum2D = @RowNum2D + 1;
        CONTINUE;
    END;
    SET @FullName = QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName);

    -- Check for base rows
    SET @SQL = N'SELECT @HasBaseRows_OUT = COUNT(*) FROM ' + @FullName + N' WHERE CustomerId IN (SELECT CustomerId FROM dbo.TxERCustomers WHERE AssocID = @pBaseAssocID);';
    SET @HasBaseRows = 0;
    EXEC sys.sp_executesql @SQL, N'@pBaseAssocID CHAR(3), @HasBaseRows_OUT INT OUTPUT', @BaseAssocID, @HasBaseRows OUTPUT;
    IF @HasBaseRows = 0 BEGIN
        SET @RowNum2D = @RowNum2D + 1;
        CONTINUE;
    END;

    -- Clone base rows
    SET @CloneBaseName = N'CloneBase_' + @TableName;
    SET @CloneBaseFull = QUOTENAME(@SchemaName) + '.' + QUOTENAME(@CloneBaseName);
    IF OBJECT_ID(@CloneBaseFull, 'U') IS NOT NULL EXEC('DROP TABLE ' + @CloneBaseFull);
    SET @SQL = N'SELECT * INTO ' + @CloneBaseFull + N' FROM ' + @FullName + N' WHERE CustomerId IN (SELECT CustomerId FROM dbo.TxERCustomers WHERE AssocID = @pBaseAssocID);';
    BEGIN TRY
        EXEC sys.sp_executesql @SQL, N'@pBaseAssocID CHAR(3)', @BaseAssocID;
    END TRY
    BEGIN CATCH
        SET @ErrorMsg = N'ERROR creating clone base for ' + @FullName + N': ' + ERROR_MESSAGE();
        RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;
        SET @ErrorTableFull = QUOTENAME(@SchemaName) + N'.' + QUOTENAME(N'Error_' + @TableName);
        IF OBJECT_ID(@ErrorTableFull, 'U') IS NULL
        BEGIN
            SET @DynCreate = N'SELECT TOP 0 * INTO ' + @ErrorTableFull + N' FROM ' + @CloneBaseFull + N';';
            EXEC(@DynCreate);
        END;
        SET @DynInsertErr = N'INSERT INTO ' + @ErrorTableFull + N' SELECT * FROM ' + @CloneBaseFull + N';';
        EXEC(@DynInsertErr);
    END CATCH;

    -- Compute shift factor
    SET @SQL = N'SELECT @Len_OUT = MAX(LEN(CAST(CustomerId AS VARCHAR(20)))) FROM ' + @CloneBaseFull + N';';
    SET @CustomerIdLen = NULL;
    EXEC sys.sp_executesql @SQL, N'@Len_OUT INT OUTPUT', @CustomerIdLen OUTPUT;
    IF @CustomerIdLen IS NULL OR @CustomerIdLen <= 0 SET @CustomerIdLen = 1;
    SET @CustomerIdShiftFactor = CONVERT(INT, POWER(10.0, @CustomerIdLen));

    -- Drop original and rename clone
    EXEC('DROP TABLE ' + @FullName);
    EXEC sys.sp_rename @CloneBaseFull, @TableName;

    -- Build column lists for insert (apply #IdentityMaxValues transforms if present)
    IF OBJECT_ID('tempdb..#IdentityMaxValues') IS NOT NULL
    BEGIN
        SELECT
            @ColumnListClone = STRING_AGG(QUOTENAME(c.name), ', ') WITHIN GROUP (ORDER BY c.column_id),
            @SelectListClone = STRING_AGG(
                    CAST(
                        CASE
                            WHEN imv.ColumnName IS NOT NULL
                                THEN N'dbo.ufn_AssocDerivedID(S.' + QUOTENAME(c.name) + N', TA.AssocID, ' + CAST(ISNULL(imv.ShiftFactor, 0) AS NVARCHAR(20)) + N')'
                            WHEN c.name = 'CustomerId' THEN 'dbo.ufn_AssocDerivedID(S.CustomerId, TA.AssocID, @pCustomerIdShiftFactor)'
                            WHEN uq.IsGuidUnique = 1 THEN 'NEWID()'
                            ELSE 'S.' + QUOTENAME(c.name)
                        END
                    AS NVARCHAR(MAX)), ', '
                ) WITHIN GROUP (ORDER BY c.column_id)
        FROM sys.columns c
            OUTER APPLY (
                SELECT TOP 1 1 AS IsGuidUnique
                FROM sys.indexes ix
                    JOIN sys.index_columns ic ON ix.object_id = ic.object_id AND ix.index_id = ic.index_id
                    JOIN sys.columns c_orig ON c_orig.object_id = ix.object_id AND c_orig.column_id = ic.column_id
                WHERE ix.object_id = OBJECT_ID(@FullName)
                    AND ix.is_unique = 1
                    AND c_orig.name = c.name
                    AND c_orig.system_type_id = TYPE_ID(N'uniqueidentifier')
            ) uq
            LEFT JOIN #IdentityMaxValues imv
                ON imv.SchemaName = @SchemaName AND imv.TableName = @TableName AND imv.ColumnName = c.name
        WHERE c.object_id = OBJECT_ID(@FullName) AND c.is_computed = 0;
    END
    ELSE
    BEGIN
        SELECT
            @ColumnListClone = STRING_AGG(QUOTENAME(c.name), ', ') WITHIN GROUP (ORDER BY c.column_id),
            @SelectListClone = STRING_AGG(
                    CASE
                        WHEN c.name = 'CustomerId' THEN 'dbo.ufn_AssocDerivedID(S.CustomerId, TA.AssocID, @pCustomerIdShiftFactor)'
                        WHEN uq.IsGuidUnique = 1 THEN 'NEWID()'
                        ELSE 'S.' + QUOTENAME(c.name)
                    END, ', '
                ) WITHIN GROUP (ORDER BY c.column_id)
        FROM sys.columns c
            OUTER APPLY (
                SELECT TOP 1
                    1 AS IsGuidUnique
                FROM sys.indexes ix
                    JOIN sys.index_columns ic ON ix.object_id = ic.object_id AND ix.index_id = ic.index_id
                    JOIN sys.columns c_orig ON c_orig.object_id = ix.object_id AND c_orig.column_id = ic.column_id
                WHERE ix.object_id = OBJECT_ID(@FullName)
                    AND ix.is_unique = 1
                    AND c_orig.name = c.name
                    AND c_orig.system_type_id = TYPE_ID(N'uniqueidentifier')
            ) uq
        WHERE c.object_id = OBJECT_ID(@FullName) AND c.is_computed = 0;
    END;

    -- Insert cloned rows for all target AssocIDs
    SET @SQL = N'INSERT INTO ' + @FullName + N' (' + @ColumnListClone + N') SELECT ' + @SelectListClone + N' FROM ' + @FullName + N' AS S CROSS JOIN #TargetAssociations TA WHERE TA.AssocID <> @pBaseAssocID;';
    BEGIN TRY
        EXEC sys.sp_executesql @SQL, N'@pBaseAssocID CHAR(3), @pCustomerIdShiftFactor INT', @BaseAssocID, @pCustomerIdShiftFactor = @CustomerIdShiftFactor;
    END TRY
    BEGIN CATCH
        SET @ErrorMsg = N'ERROR inserting into ' + @FullName + N': ' + ERROR_MESSAGE();
        RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;
        SET @ErrorTableFull = QUOTENAME(@SchemaName) + N'.' + QUOTENAME(N'Error_' + @TableName);
        IF OBJECT_ID(@ErrorTableFull, 'U') IS NULL
        BEGIN
            IF @SelectListClone IS NOT NULL
            BEGIN
                SET @DynSelectInto =
                    N'SELECT ' + @SelectListClone
                    + N' INTO ' + @ErrorTableFull
                    + N' FROM ' + @FullName + N' AS S CROSS JOIN #TargetAssociations AS TA WHERE TA.AssocID <> @pBaseAssocID;';

                EXEC sys.sp_executesql @DynSelectInto, N'@pBaseAssocID CHAR(3), @pCustomerIdShiftFactor INT', @pBaseAssocID = @BaseAssocID, @pCustomerIdShiftFactor = @CustomerIdShiftFactor;
            END
            ELSE
            BEGIN
                SET @DynCreate = N'SELECT TOP 0 * INTO ' + @ErrorTableFull + N' FROM ' + @FullName + N';';
                EXEC(@DynCreate);
            END
        END;

        IF @SelectListClone IS NOT NULL
        BEGIN
            SET @DynInsertErr =
                N'INSERT INTO ' + @ErrorTableFull
                + N' SELECT ' + @SelectListClone
                + N' FROM ' + @FullName + N' AS S CROSS JOIN #TargetAssociations AS TA WHERE TA.AssocID <> @pBaseAssocID;';

            EXEC sys.sp_executesql @DynInsertErr, N'@pBaseAssocID CHAR(3), @pCustomerIdShiftFactor INT', @pBaseAssocID = @BaseAssocID, @pCustomerIdShiftFactor = @CustomerIdShiftFactor;
        END
        ELSE
        BEGIN
            SET @DynInsertErr = N'INSERT INTO ' + @ErrorTableFull + N' SELECT * FROM ' + @FullName + N';';
            EXEC(@DynInsertErr);
        END;
    END CATCH;

    -- Logging placeholder
    -- INSERT INTO [dbo].[SSISLoad] (PackageID, Result, StatusDT) VALUES (@PackageID, N'Cloned and rebuilt ' + @FullName, GETDATE());

    SET @RowNum2D = @RowNum2D + 1;
END;

/****************************************************************
     PHASE FK1: Recreate all previously dropped foreign key constraints
    ****************************************************************/
IF OBJECT_ID('tempdb..#TargetFKs') IS NOT NULL
    BEGIN
    DECLARE
          @CreateFKSql  NVARCHAR(MAX)
        , @CreateFKName SYSNAME
        , @ParentSchema SYSNAME
        , @ParentTable SYSNAME
        , @ReferencedSchema SYSNAME
        , @ReferencedTable SYSNAME
        , @ParentTableFull NVARCHAR(512)
        , @ReferencedTableFull NVARCHAR(512)
        , @FKRowCheckSql NVARCHAR(MAX)
        , @MissingRowsCount INT
        , @FKColumns NVARCHAR(MAX)  -- For multi-column FK validation
        , @RefColumns NVARCHAR(MAX)
        , @IsValid BIT;

    DECLARE CreateFK_Cursor CURSOR FAST_FORWARD FOR
            SELECT CreateScript, FKName, ParentSchema, ParentTable, ReferencedSchema, ReferencedTable
    FROM #TargetFKs
    ORDER BY SortOrder;

    OPEN CreateFK_Cursor;

    FETCH NEXT FROM CreateFK_Cursor
            INTO @CreateFKSql, @CreateFKName, @ParentSchema, @ParentTable, @ReferencedSchema, @ReferencedTable;

    WHILE @@FETCH_STATUS = 0
        BEGIN
        SET @ParentTableFull = QUOTENAME(@ParentSchema) + N'.' + QUOTENAME(@ParentTable);
        SET @ReferencedTableFull = QUOTENAME(@ReferencedSchema) + N'.' + QUOTENAME(@ReferencedTable);
        SET @IsValid = 1;

        BEGIN TRY
                -- Check referenced and parent tables exist before FK creation
                IF OBJECT_ID(@ParentTableFull, 'U') IS NOT NULL AND OBJECT_ID(@ReferencedTableFull, 'U') IS NOT NULL
                BEGIN
            -- Validate FK columns exist (enhanced check)
            SELECT @FKColumns = STRING_AGG(QUOTENAME(c.name), ', ')
            FROM sys.foreign_key_columns fkc
                JOIN sys.columns c ON fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id
            WHERE fkc.constraint_object_id = OBJECT_ID(@CreateFKName);

            SELECT @RefColumns = STRING_AGG(QUOTENAME(c.name), ', ')
            FROM sys.foreign_key_columns fkc
                JOIN sys.columns c ON fkc.referenced_object_id = c.object_id AND fkc.referenced_column_id = c.column_id
            WHERE fkc.constraint_object_id = OBJECT_ID(@CreateFKName);

            -- Check if columns exist in tables (basic validation; assumes column names match)
            IF NOT EXISTS (
                        SELECT 1
                FROM sys.columns c
                WHERE c.object_id = OBJECT_ID(@ParentTableFull) AND c.name IN (SELECT value
                    FROM STRING_SPLIT(@FKColumns, ','))
                    ) OR NOT EXISTS (
                        SELECT 1
                FROM sys.columns c
                WHERE c.object_id = OBJECT_ID(@ReferencedTableFull) AND c.name IN (SELECT value
                    FROM STRING_SPLIT(@RefColumns, ','))
                    )
                    BEGIN
                SET @IsValid = 0;
                SET @ResultMessage =
                            N'SKIP_CREATE_FK_INVALID_COLUMNS: ' + @CreateFKName +
                            N'; Columns missing in tables: ' + @ParentTableFull + N' or ' + @ReferencedTableFull;
                INSERT INTO [dbo].[SSISLoad]
                    (PackageID, Result, StatusDT)
                VALUES
                    (@PackageID, @ResultMessage, GETDATE());
                FETCH NEXT FROM CreateFK_Cursor
                            INTO @CreateFKSql, @CreateFKName, @ParentSchema, @ParentTable, @ReferencedSchema, @ReferencedTable;
                CONTINUE;
            END

            -- Check for orphaned rows (enhanced for multi-column FKs)
            -- Build a dynamic check using FK column mappings
            SET @FKRowCheckSql = N'SELECT @MissingRowsCount_OUT = COUNT(*) FROM ' + @ParentTableFull + N' AS p WHERE NOT EXISTS (SELECT 1 FROM ' + @ReferencedTableFull + N' AS r WHERE ';
            -- Note: For simplicity, assume single-column FKs or matching names. For multi-column, extend with JOIN logic.
            -- Example for single-column: WHERE p.Column = r.Column
            -- For multi-column, this needs expansion (e.g., loop through columns).
            -- Placeholder: Use a simple check if columns match.
            DECLARE @ParentCol NVARCHAR(128), @RefCol NVARCHAR(128);
            SELECT TOP 1
                @ParentCol = c.name
            FROM sys.columns c
            WHERE c.object_id = OBJECT_ID(@ParentTableFull) AND c.name IN (SELECT value
                FROM STRING_SPLIT(@FKColumns, ','));
            SELECT TOP 1
                @RefCol = c.name
            FROM sys.columns c
            WHERE c.object_id = OBJECT_ID(@ReferencedTableFull) AND c.name IN (SELECT value
                FROM STRING_SPLIT(@RefColumns, ','));
            IF @ParentCol IS NOT NULL AND @RefCol IS NOT NULL
                    BEGIN
                SET @FKRowCheckSql = @FKRowCheckSql + N' p.' + QUOTENAME(@ParentCol) + N' = r.' + QUOTENAME(@RefCol) + N')';
                SET @MissingRowsCount = 0;
                EXEC sys.sp_executesql
                            @FKRowCheckSql,
                            N'@MissingRowsCount_OUT INT OUTPUT',
                            @MissingRowsCount_OUT = @MissingRowsCount OUTPUT;

                IF @MissingRowsCount > 0
                        BEGIN
                    SET @IsValid = 0;
                    SET @ResultMessage =
                                N'SKIP_CREATE_FK_ORPHANED_ROWS: ' + @CreateFKName +
                                N'; Orphaned rows in ' + @ParentTableFull +
                                N' for FK columns. Missing referenced rows: ' + CAST(@MissingRowsCount AS NVARCHAR(20));
                    INSERT INTO [dbo].[SSISLoad]
                        (PackageID, Result, StatusDT)
                    VALUES
                        (@PackageID, @ResultMessage, GETDATE());
                    FETCH NEXT FROM CreateFK_Cursor
                                INTO @CreateFKSql, @CreateFKName, @ParentSchema, @ParentTable, @ReferencedSchema, @ReferencedTable;
                    CONTINUE;
                END
            END

            -- If valid, try to create FK
            IF @IsValid = 1
                    BEGIN
                EXEC sys.sp_executesql @CreateFKSql;

                BEGIN TRY
                            SET @ResultMessage =
                                N'CREATE_FK: ' + @CreateFKName;
                            INSERT INTO [dbo].[SSISLoad]
                    (PackageID, Result, StatusDT)
                VALUES
                    (@PackageID, @ResultMessage, GETDATE());
                        END TRY
                        BEGIN CATCH
                        END CATCH;
            END
        END
                ELSE
                BEGIN
            SET @ResultMessage =
                        N'SKIP_CREATE_FK_MISSING_TABLE: ' + @CreateFKName
                        + N'; Parent or referenced table missing: '
                        + @ParentTableFull + N' or ' + @ReferencedTableFull;
            INSERT INTO [dbo].[SSISLoad]
                (PackageID, Result, StatusDT)
            VALUES
                (@PackageID, @ResultMessage, GETDATE());
        END
            END TRY
            BEGIN CATCH
                SET @ErrorMsg = N'ERROR creating FK ' + @CreateFKName + N': ' + ERROR_MESSAGE();
                RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

                BEGIN TRY
                    SET @ResultMessage =
                        N'CREATE_FK_ERROR: ' + @CreateFKName
                        + N'; Error=' + ERROR_MESSAGE()
                        + N'; ParentTable=' + @ParentTableFull
                        + N'; ReferencedTable=' + @ReferencedTableFull;
                    INSERT INTO [dbo].[SSISLoad]
            (PackageID, Result, StatusDT)
        VALUES
            (@PackageID, @ResultMessage, GETDATE());
                END TRY
                BEGIN CATCH
                END CATCH;
            END CATCH;

        FETCH NEXT FROM CreateFK_Cursor
                INTO @CreateFKSql, @CreateFKName, @ParentSchema, @ParentTable, @ReferencedSchema, @ReferencedTable;
    END;

    CLOSE CreateFK_Cursor;
    DEALLOCATE CreateFK_Cursor;

    DROP TABLE #TargetFKs;
END;

/****************************************************************
     PHASE TRG1: Re-enable previously disabled triggers
    ****************************************************************/
IF OBJECT_ID('tempdb..#TargetTriggers') IS NOT NULL
    BEGIN
    DECLARE
              @EnableTrgSql  NVARCHAR(MAX)
            , @EnableTrgName SYSNAME
            , @EnableTrgSchema SYSNAME
            , @EnableTrgTable SYSNAME
            , @EnableTrgFullName NVARCHAR(512);

    DECLARE EnableTrigger_Cursor CURSOR FAST_FORWARD FOR
        SELECT EnableScript, TriggerName, SchemaName, TableName
        FROM #TargetTriggers
        ORDER BY SortOrder;

    OPEN EnableTrigger_Cursor;

    FETCH NEXT FROM EnableTrigger_Cursor INTO @EnableTrgSql, @EnableTrgName, @EnableTrgSchema, @EnableTrgTable;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Build the fully qualified trigger name for existence check
        SET @EnableTrgFullName = QUOTENAME(@EnableTrgSchema) + N'.' + QUOTENAME(@EnableTrgName);

        BEGIN TRY
            -- Check trigger exists before enabling
            IF OBJECT_ID(@EnableTrgFullName, 'TR') IS NOT NULL
            BEGIN
                EXEC sys.sp_executesql @EnableTrgSql;

                BEGIN TRY
                    SET @ResultMessage = N'ENABLE_TRIGGER: ' + @EnableTrgName;
                    INSERT INTO [dbo].[SSISLoad] (PackageID, Result, StatusDT)
                    VALUES (@PackageID, @ResultMessage, GETDATE());
                END TRY
                BEGIN CATCH
                END CATCH;
            END
            ELSE
            BEGIN
                SET @ResultMessage =
                    N'SKIP_ENABLE_TRIGGER_MISSING: ' + @EnableTrgName
                    + N'; Trigger missing for table ' + QUOTENAME(@EnableTrgSchema) + N'.' + QUOTENAME(@EnableTrgTable) + N'.';

                INSERT INTO [dbo].[SSISLoad] (PackageID, Result, StatusDT)
                VALUES (@PackageID, @ResultMessage, GETDATE());
            END
        END TRY
        BEGIN CATCH
            SET @ErrorMsg = N'ERROR enabling trigger ' + @EnableTrgName + N': ' + ERROR_MESSAGE();
            RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT;

            BEGIN TRY
                SET @ResultMessage = N'ENABLE_TRIGGER_ERROR: ' + @EnableTrgName + N'; Error=' + ERROR_MESSAGE();
                INSERT INTO [dbo].[SSISLoad] (PackageID, Result, StatusDT)
                VALUES (@PackageID, @ResultMessage, GETDATE());
            END TRY
            BEGIN CATCH
            END CATCH;
        END CATCH;

        FETCH NEXT FROM EnableTrigger_Cursor INTO @EnableTrgSql, @EnableTrgName, @EnableTrgSchema, @EnableTrgTable;
    END;

    CLOSE EnableTrigger_Cursor;
    DEALLOCATE EnableTrigger_Cursor;

    DROP TABLE #TargetTriggers;
END;

    ----------------------------------------------------------------
    -- Log: procedure completion
    ----------------------------------------------------------------
    BEGIN TRY
        SET @ResultMessage =
            N'COMPLETE: ' + @ProcName
            + N'; BaseAssocID=' + CAST(@BaseAssocID AS NVARCHAR(10));

        INSERT INTO [dbo].[SSISLoad]
        (PackageID, Result, StatusDT)
    VALUES
        (@PackageID, @ResultMessage, GETDATE());

        RAISERROR(N'===========================================================', 0, 1);
        RAISERROR(N'Cloning complete for all target AssocIDs.', 0, 1);
        RAISERROR(N'===========================================================', 0, 1);
    END TRY
    BEGIN CATCH
    END CATCH;

END;