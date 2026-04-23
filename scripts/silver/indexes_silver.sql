/*
===============================================================================
Performance Script: Create Silver Layer Indexes
===============================================================================
Script Purpose:
    This script creates nonclustered indexes on common join and lookup columns
    used by Gold views and analytical queries.
===============================================================================
*/

IF EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_silver_crm_cust_info_cst_id'
      AND object_id = OBJECT_ID('silver.crm_cust_info')
)
    DROP INDEX ix_silver_crm_cust_info_cst_id ON silver.crm_cust_info;
GO
CREATE INDEX ix_silver_crm_cust_info_cst_id
    ON silver.crm_cust_info (cst_id)
    INCLUDE (cst_key, cst_gndr, cst_marital_status, cst_create_date);
GO

IF EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_silver_crm_cust_info_cst_key'
      AND object_id = OBJECT_ID('silver.crm_cust_info')
)
    DROP INDEX ix_silver_crm_cust_info_cst_key ON silver.crm_cust_info;
GO
CREATE INDEX ix_silver_crm_cust_info_cst_key
    ON silver.crm_cust_info (cst_key);
GO

IF EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_silver_crm_prd_info_prd_key_prd_end_dt'
      AND object_id = OBJECT_ID('silver.crm_prd_info')
)
    DROP INDEX ix_silver_crm_prd_info_prd_key_prd_end_dt ON silver.crm_prd_info;
GO
CREATE INDEX ix_silver_crm_prd_info_prd_key_prd_end_dt
    ON silver.crm_prd_info (prd_key, prd_end_dt)
    INCLUDE (prd_id, cat_id, prd_nm, prd_cost, prd_line, prd_start_dt);
GO

IF EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_silver_crm_sales_details_sls_prd_key_sls_cust_id'
      AND object_id = OBJECT_ID('silver.crm_sales_details')
)
    DROP INDEX ix_silver_crm_sales_details_sls_prd_key_sls_cust_id ON silver.crm_sales_details;
GO
CREATE INDEX ix_silver_crm_sales_details_sls_prd_key_sls_cust_id
    ON silver.crm_sales_details (sls_prd_key, sls_cust_id)
    INCLUDE (sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price);
GO

IF EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_silver_erp_cust_az12_cid'
      AND object_id = OBJECT_ID('silver.erp_cust_az12')
)
    DROP INDEX ix_silver_erp_cust_az12_cid ON silver.erp_cust_az12;
GO
CREATE INDEX ix_silver_erp_cust_az12_cid
    ON silver.erp_cust_az12 (cid)
    INCLUDE (bdate, gen);
GO

IF EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_silver_erp_loc_a101_cid'
      AND object_id = OBJECT_ID('silver.erp_loc_a101')
)
    DROP INDEX ix_silver_erp_loc_a101_cid ON silver.erp_loc_a101;
GO
CREATE INDEX ix_silver_erp_loc_a101_cid
    ON silver.erp_loc_a101 (cid)
    INCLUDE (cntry);
GO

IF EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_silver_erp_px_cat_g1v2_id'
      AND object_id = OBJECT_ID('silver.erp_px_cat_g1v2')
)
    DROP INDEX ix_silver_erp_px_cat_g1v2_id ON silver.erp_px_cat_g1v2;
GO
CREATE INDEX ix_silver_erp_px_cat_g1v2_id
    ON silver.erp_px_cat_g1v2 (id)
    INCLUDE (cat, subcat, maintenance);
GO
