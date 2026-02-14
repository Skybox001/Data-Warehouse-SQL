EXEC bronze.load_bronze


create or alter procedure bronze.load_bronze as
begin

declare @batchStartTime datetime, @batchEndTime datetime;
    begin try

    set @batchStartTime =GETDATE();
print '==============================================';
print 'Loading bronze layer';
print '=============================================';

print '=============================================';
print 'Loading crm tables';
print '=============================================';
if object_id('bronze.crm_prod_info', 'U') is not null
DROP TABLE bronze.crm_prod_info;
create table bronze.crm_prod_info(
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt datetime,
prd_end_dt datetime,
);
if object_id('bronze.crm_sales_details', 'U') is not null
DROP TABLE bronze.crm_sales_details;
create table bronze.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50) ,
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int,
);


if object_id('bronze.erp_loc_a101', 'U') is not null
DROP TABLE bronze.erp_loc_a101;
create table bronze.erp_loc_a101(
cid nvarchar(50),

cntry nvarchar(50),
);
if object_id('bronze.erp_cust_az12', 'U') is not null
DROP TABLE  bronze.erp_cust_az12;
create table bronze.erp_cust_az12(
cid nvarchar(50),
bdate date,
gen nvarchar(50),
);
if object_id('bronze.erp_px_cat_g1v2', 'U') is not null
DROP TABLE  bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2(
id nvarchar(50),
cat nvarchar(50),
subcate nvarchar(50),
maintenance nvarchar(50),
);

if object_id('bronze.crm_cust_info', 'U') is not null
DROP TABLE  bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             VARCHAR(50),
    cst_firstname       VARCHAR(100),
    cst_lastname        VARCHAR(100),
    cst_marital_status  CHAR(1),
    cst_gndr            CHAR(1),
    cst_create_date     DATE
);

truncate table bronze.crm_cust_info
BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\ASUS\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm/cust_info.csv'
with ( 
FIRSTROW = 2,
fieldterminator =',',
tablock
);

truncate table bronze.crm_prod_info
BULK INSERT bronze.crm_prod_info
FROM 'C:\Users\ASUS\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm/prd_info.csv'
with ( 
FIRSTROW = 2,
fieldterminator =',',
tablock
);

truncate table bronze.crm_sales_details
BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\ASUS\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm/sales_details.csv'
with ( 
FIRSTROW = 2,
fieldterminator =',',
tablock
);
print '==================================';
print 'Loading erp tables';
print '==================================';

truncate table bronze.erp_cust_az12
BULK INSERT bronze.erp_cust_az12
FROM 'C:\Users\ASUS\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
with ( 
FIRSTROW = 2,
fieldterminator =',',
tablock
);

truncate table bronze.erp_loc_a101
BULK INSERT bronze.erp_loc_a101
FROM 'C:\Users\ASUS\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
with ( 
FIRSTROW = 2,
fieldterminator =',',
tablock
);
truncate table bronze.erp_px_cat_g1v2
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Users\ASUS\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
with ( 
FIRSTROW = 2,
fieldterminator =',',
tablock
);
set @batchEndTime = getdate();

print '------------------------------------';
print'total Loading time:'+ cast(datediff(second,@batchStartTime,@batchEndTime)as Nvarchar)+ 'seconds';

print '------------------------------------';

END try

begin catch
print 'Something went wrong'
end catch
end 




