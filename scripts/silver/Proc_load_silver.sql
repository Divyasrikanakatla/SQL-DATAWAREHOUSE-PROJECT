/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
		DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		BEGIN TRY
				SET @batch_start_time =GETDATE();
				PRINT '=================================================================';
				PRINT '         LOADING SILVER LAYER                       ';
				PRINT '=================================================================';

				PRINT '-----------------------------------------------------------------';
				PRINT '         LOADING CRM TABLES                           ';
				PRINT '-----------------------------------------------------------------';
		
				SET @start_time = GETDATE();
			PRINT '>> Truncating table: silver.crm_cust_info'
			TRUNCATE TABLE silver.crm_cust_info
			PRINT '>> Inserting data into: silver.crm_cust_info';
			INSERT INTO silver.crm_cust_info (
				cust_id ,
				cst_key ,
				cst_firstname ,
				cst_lastname ,
				cst_marital_status ,
				cst_gndr ,
				cst_create_date)

			SELECT
				cust_id,	
				cst_key,
				TRIM(cst_firstname) AS cst_firstname,
				TRIM(cst_lastname) AS cst_lastname,
				CASE
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'SINGLE'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'MARRIED'
				 ELSE 'UNKNOWN'
				END cst_marital_status,
				CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
				 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
				 ELSE 'UNKNOWN'
				END cst_gndr,
				cst_create_date
			FROM
			(
				SELECT *,
				ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY cst_create_date DESC ) as flag_last
				FROM bronze.crm_cust_info
				WHERE cust_id IS NOT NULL

			)t WHERE flag_last = 1;
			 SET @end_time = GETDATE();
			PRINT '>>LOAD DURATION :' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
			PRINT '>> ----------------------------'
			SET @start_time = GETDATE();
			PRINT '>> Truncating table: silver.crm_prd_info'
				TRUNCATE TABLE silver.crm_prd_info
			PRINT '>> Inserting data into: silver.crm_prd_info';
			INSERT INTO silver.crm_prd_info (
				prd_id,
				cat_id,
				prd_key,
				prd_nm ,
				prd_cost ,
				prd_line ,
				prd_start_dt ,
				prd_end_dt 
			)

			SELECT
				prd_id ,	
				REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
				SUBSTRING(prd_key,7,len(prd_key)) AS prd_key,	prd_nm ,
				ISNULL(prd_cost,0) AS prd_cost,
				CASE UPPER(TRIM(prd_line))
					WHEN  'R' THEN 'Road' 
					WHEN  'S' THEN 'Other Sales'
					WHEN  'M' THEN 'Montain'
					WHEN  'T' THEN 'Touring'
				ELSE 'N/A'
				END as prd_line,
				CAST(prd_start_dt AS DATE) ,
				CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 as date) AS prd_end_dt 
			FROM [bronze].[crm_prd_info]
			 SET @end_time = GETDATE();
			PRINT '>>LOAD DURATION :' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
			PRINT '>> ----------------------------'
			SET @start_time = GETDATE();
			PRINT '>> Truncating table: silver.crm_sales_details';
				TRUNCATE TABLE silver.crm_sales_details
			PRINT '>> Inserting data into: silver.crm_sales_details';	
			INSERT INTO silver.crm_sales_details (
				sls_ord_num ,
				sls_prd_key,
				sls_cust_id ,
				sls_order_dt ,
				sls_ship_dt ,
				sls_due_dt	,
				sls_sales ,
				sls_quantity,
				sls_price )
	 
			SELECT 
				sls_ord_num, 
				sls_prd_key, 
				sls_cust_id ,
	
				CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE) 
				END AS sls_order_dt,
	
				CASE 
				   WHEN sls_ship_dt = 0 OR LEN(cast(sls_ship_dt as varchar(8))) != 8 THEN CAST(NULL AS DATE)
				   ELSE CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE)
				END AS sls_ship_dt,
	
				CASE 
				WHEN sls_due_dt = 0 OR LEN(cast(sls_due_dt as varchar(8))) != 8 THEN CAST(NULL AS DATE)
				ELSE CAST(CAST(sls_due_dt AS VARCHAR(8)) AS DATE)
				END AS sls_due_dt,
	
				CASE 
				WHEN sls_sales IS NULL or sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
				END as sls_sales,
				sls_quantity,
	
				CASE 
				WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales/NULLIF(sls_quantity,0)
				ELSE sls_price
				END as sls_price
				FROM [bronze].[crm_sales_details]
			SET @end_time = GETDATE();
			PRINT '>>LOAD DURATION :' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
			PRINT '>> ----------------------------'
			PRINT '-----------------------------------------------------------------';
			PRINT '         LOADING CRM TABLES                           ';
			PRINT '-----------------------------------------------------------------';
			SET @start_time = GETDATE();
			PRINT '>> Truncating table: silver.erp_cust_AZ12'
			 TRUNCATE TABLE silver.erp_cust_AZ12
			PRINT '>> Inserting data into: silver.erp_cust_AZ12';
			INSERT INTO silver.erp_cust_AZ12(cid,bdate,gen)
				select 
				CASE 
				WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,len(cid))
				ELSE cid
				END as cid,
				CASE WHEN bdate > getdate() THEN NULL
				ELSE bdate
				END AS bdate,
				CASE 
				WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('F','Male') THEN 'Male'
				ELSE 'Unknown'
				END AS gen
				from bronze.erp_cust_AZ12
				 SET @end_time = GETDATE();
				  PRINT '>>LOAD DURATION :' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
			PRINT '>> ----------------------------'
			SET @start_time = GETDATE();
			PRINT '>> Truncating table: silver.erp_loc_a101'
			 TRUNCATE TABLE silver.erp_loc_a101
			PRINT '>> Inserting data into: silver.erp_loc_a101';
			INSERT INTO silver.erp_loc_a101(cid,country)
			select 
				REPLACE(cid,'-','') AS cid, 
				CASE 
				WHEN TRIM(country) IN ('USA','US') THEN 'United States'
				WHEN TRIM(country) = 'DE' THEN 'Germany'
				WHEN TRIM(country) = '' or country is NULL THEN 'n/a'
				else TRIM(country)
				END as country
			FROM [bronze].[erp_loc_a101]
			 SET @end_time = GETDATE();
				  PRINT '>>LOAD DURATION :' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
			PRINT '>> ----------------------------'
			SET @start_time = GETDATE();
			PRINT '>> Truncating table: silver.erp_px_cat_g1v2'
				TRUNCATE TABLE silver.erp_px_cat_g1v2
			PRINT '>> Inserting data into: silver.erp_px_cat_g1v2';
			INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
			select id,
				cat,
				subcat,
				maintenance 
			FROM bronze.erp_px_cat_g1v2
			 SET @end_time = GETDATE();
				  PRINT '>>LOAD DURATION :' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
			PRINT '>> ----------------------------'
			 SET @batch_end_time = GETDATE();
				  PRINT '**********************************************************************'
				  PRINT '>> LOADING SILVER LAYER IS COMPLETED'
				  PRINT '>> LOAD DURATION:'+ CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) as NVARCHAR) + 'seconds';
				  PRINT '**********************************************************************'

	 END TRY
		   BEGIN CATCH
				PRINT '=======================================================';
				PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
				PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
				PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
				PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
				PRINT 'ERROR MESSAGE' + CAST(ERROR_LINE() AS NVARCHAR);
		   END CATCH
END
