# 📊 Data Warehouse SQL Project

## 🚀 Overview
This project implements an end-to-end SQL Server data warehouse using a Medallion-style architecture:
- **bronze**: raw ingestion from CRM and ERP CSV sources
- **silver**: cleansed and standardized data
- **gold**: business-ready star schema views for analytics

## 🏗️ Architecture
- **Bronze layer**: source-aligned tables loaded via `BULK INSERT`
- **Silver layer**: transformation, deduplication, standardization, and data fixes
- **Gold layer**: `dim_customers`, `dim_products`, and `fact_sales`

## 📁 Project Structure
- `scripts/init_database.sql` - creates database and schemas
- `scripts/bronze/ddl_bronze.sql` - bronze table DDL
- `scripts/bronze/proc_load_bronze.sql` - bronze load procedure (configurable source root)
- `scripts/silver/ddl_silver.sql` - silver table DDL
- `scripts/silver/proc_load_silver.sql` - silver load procedure
- `scripts/silver/indexes_silver.sql` - performance indexes for analytical joins
- `scripts/gold/ddl_gold.sql` - gold dimensional/fact views
- `scripts/proc_load_datawarehouse.sql` - orchestration procedure for full load
- `tests/quality_checks_silver.sql` - silver quality checks
- `tests/quality_checks_gold.sql` - gold quality checks

## ▶️ Quick Start
1. Run `scripts/init_database.sql`
2. Run:
   - `scripts/bronze/ddl_bronze.sql`
   - `scripts/silver/ddl_silver.sql`
   - `scripts/gold/ddl_gold.sql`
3. Run the full pipeline:
   - `scripts/proc_load_datawarehouse.sql` (create procedure)
   - `EXEC dbo.load_datawarehouse;`
4. Optional:
   - Use custom source path:  
     `EXEC dbo.load_datawarehouse @source_root = 'D:\datasets';`
   - Run index script: `scripts/silver/indexes_silver.sql`
5. Validate outputs with:
   - `tests/quality_checks_silver.sql`
   - `tests/quality_checks_gold.sql`

## ✅ Recent Improvements
- Added configurable source root path for Bronze loading
- Added full-load orchestration procedure (`dbo.load_datawarehouse`)
- Added transaction-safe Silver load behavior with rollback on error
- Added Silver-layer indexing script for faster joins and reporting queries

## 🙌 Acknowledgement
This project was inspired by:
https://github.com/DataWithBaraa/sql-data-warehouse-project
