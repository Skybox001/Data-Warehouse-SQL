/*
===============================================================================
Stored Procedure: Load Full Data Warehouse (Bronze -> Silver)
===============================================================================
Script Purpose:
    This orchestration procedure runs the Bronze and Silver loading procedures
    in sequence and reports total batch duration.

Parameters:
    @source_root NVARCHAR(4000) = root folder that contains:
      - source_crm
      - source_erp

Usage Example:
    EXEC dbo.load_datawarehouse;
    EXEC dbo.load_datawarehouse @source_root = 'D:\datasets';
===============================================================================
*/
CREATE OR ALTER PROCEDURE dbo.load_datawarehouse
    @source_root NVARCHAR(4000) = N'C:\sql\dwh_project\datasets'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Running full data warehouse load';
        PRINT '================================================';

        EXEC bronze.load_bronze @source_root = @source_root;
        EXEC silver.load_silver;

        SET @batch_end_time = GETDATE();
        PRINT '================================================';
        PRINT 'Full data warehouse load completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';
    END TRY
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR OCCURRED DURING FULL LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';
        THROW;
    END CATCH
END
