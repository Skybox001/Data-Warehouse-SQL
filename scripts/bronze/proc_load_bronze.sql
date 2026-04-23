/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from CSV files to bronze tables.

Parameters:
    @source_root NVARCHAR(4000) = root folder that contains:
      - source_crm
      - source_erp

Usage Example:
    EXEC bronze.load_bronze;
    EXEC bronze.load_bronze @source_root = 'D:\datasets';
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze
    @source_root NVARCHAR(4000) = N'C:\sql\dwh_project\datasets'
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	DECLARE @file_path NVARCHAR(4000), @sql NVARCHAR(MAX);

	IF NULLIF(LTRIM(RTRIM(@source_root)), N'') IS NULL
	BEGIN
		THROW 50001, '@source_root cannot be empty.', 1;
	END;

	SET @source_root = REPLACE(@source_root, '/', '\');
	IF RIGHT(@source_root, 1) = '\'
	BEGIN
		SET @source_root = LEFT(@source_root, LEN(@source_root) - 1);
	END;

	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		SET @file_path = CONCAT(@source_root, '\source_crm\cust_info.csv');
		SET @sql = N'BULK INSERT bronze.crm_cust_info FROM ''' + REPLACE(@file_path, '''', '''''') + N''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
		EXEC sp_executesql @sql;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		SET @file_path = CONCAT(@source_root, '\source_crm\prd_info.csv');
		SET @sql = N'BULK INSERT bronze.crm_prd_info FROM ''' + REPLACE(@file_path, '''', '''''') + N''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
		EXEC sp_executesql @sql;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		SET @file_path = CONCAT(@source_root, '\source_crm\sales_details.csv');
		SET @sql = N'BULK INSERT bronze.crm_sales_details FROM ''' + REPLACE(@file_path, '''', '''''') + N''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
		EXEC sp_executesql @sql;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		SET @file_path = CONCAT(@source_root, '\source_erp\loc_a101.csv');
		SET @sql = N'BULK INSERT bronze.erp_loc_a101 FROM ''' + REPLACE(@file_path, '''', '''''') + N''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
		EXEC sp_executesql @sql;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		SET @file_path = CONCAT(@source_root, '\source_erp\cust_az12.csv');
		SET @sql = N'BULK INSERT bronze.erp_cust_az12 FROM ''' + REPLACE(@file_path, '''', '''''') + N''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
		EXEC sp_executesql @sql;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		SET @file_path = CONCAT(@source_root, '\source_erp\px_cat_g1v2.csv');
		SET @sql = N'BULK INSERT bronze.erp_px_cat_g1v2 FROM ''' + REPLACE(@file_path, '''', '''''') + N''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
		EXEC sp_executesql @sql;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '==========================================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '==========================================';
	END TRY
	BEGIN CATCH
		PRINT '==========================================';
		PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================';
		THROW;
	END CATCH
END
