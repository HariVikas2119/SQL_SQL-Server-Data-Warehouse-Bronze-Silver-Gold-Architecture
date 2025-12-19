                                                    ------ TABLE CREATION ------
                                                    
                                                    -->>>>>> CRM SYSTEM <<<<<<--

IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
  DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_first_name NVARCHAR(50),
cst_last_name NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE
);

IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
  DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME);

IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT);

                                                -->>>>>> ERP SYSTEM <<<<<<--

IF OBJECT_ID('bronze.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
ID NVARCHAR(50),
CAT NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(50));


IF OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
CID NVARCHAR(50),
BDATE DATE,
GEN NVARCHAR(50));

IF OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
CID NVARCHAR(50),
CNTRY NVARCHAR(50)
);

                                                  -->>>>>> LOADING PHASE <<<<<<--


CREATE OR ALTER PROCEDURE bronze.load_bronze as
BEGIN
	DECLARE @START_TIME DATETIME ,@END_TIME DATETIME, @BATCH_START_TIME DATETIME, @BATCH_END_TIME DATETIME; ------------- ATTRIBUTES TO TRACK EXECUTION TIME
	BEGIN TRY
			SET @BATCH_START_TIME = GETDATE();
			PRINT '=====================================';
			PRINT 'LOADING BRONZE LAYER';
			PRINT '=====================================';

			PRINT '-------------------------------------';
			PRINT 'LOADING CRM SECTION'
			PRINT '-------------------------------------';

			SET @START_TIME = GETDATE();
			PRINT '>>> TRUNCATING : bronze.crm_cust_info';
			TRUNCATE TABLE bronze.crm_cust_info;
			PRINT '>>> INSERTING : bronze.crm_cust_info';
			BULK INSERT bronze.crm_cust_info
			FROM 'C:\Users\44777\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
			SET @END_TIME = GETDATE();
			PRINT '>>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS' ;
			PRINT '>>>---------------<<<'

			SET @START_TIME = GETDATE();
			PRINT '>>> TRUNCATING : bronze.crm_prd_info';
			TRUNCATE TABLE bronze.crm_prd_info;
			PRINT '>>> INSERTING : bronze.crm_prd_info';
			BULK INSERT bronze.crm_prd_info
			FROM 'C:\Users\44777\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
			SET @END_TIME = GETDATE();
			PRINT '>>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS' ;
			PRINT '>>>---------------<<<'

			SET @START_TIME = GETDATE();
			PRINT '>>> TRUNCATING : bronze.crm_sales_details';
			TRUNCATE TABLE bronze.crm_sales_details;
			PRINT '>>> INSERTING : bronze.crm_sales_details';
			BULK INSERT bronze.crm_sales_details
			FROM 'C:\Users\44777\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK);
			SET @END_TIME = GETDATE();
			PRINT '>>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS' ;
			PRINT '>>>---------------<<<'


			PRINT '-------------------------------------';
			PRINT 'LOADING ERP SECTION'
			PRINT '-------------------------------------';
	
			SET @START_TIME = GETDATE();
			PRINT '>>> TRUNCATING : bronze.erp_cust_az12';
			TRUNCATE TABLE bronze.erp_cust_az12;
			PRINT '>>> INSERTING : bronze.erp_cust_az12';
			BULK INSERT bronze.erp_cust_az12
			FROM 'C:\Users\44777\Downloads\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
			WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
			SET @END_TIME = GETDATE();
			PRINT '>>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS' ;
			PRINT '>>>---------------<<<'
			
			SET @START_TIME = GETDATE();
			PRINT '>>> TRUNCATING : bronze.erp_loc_a101';
			TRUNCATE TABLE bronze.erp_loc_a101;
			PRINT '>>> INSERTING : bronze.erp_loc_a101';
			BULK INSERT bronze.erp_loc_a101
			FROM 'C:\Users\44777\Downloads\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
			WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
			SET @END_TIME = GETDATE();
			PRINT '>>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS' ;
			PRINT '>>>---------------<<<'

			SET @START_TIME = GETDATE();
			PRINT '>>> TRUNCATING : bronze.erp_px_cat_g1v2';
			TRUNCATE TABLE bronze.erp_px_cat_g1v2;
			PRINT '>>> INSERTING : bronze.erp_px_cat_g1v2';
			BULK INSERT bronze.erp_px_cat_g1v2
			FROM 'C:\Users\44777\Downloads\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
			WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
			SET @END_TIME = GETDATE();
			PRINT '>>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS' ;
			PRINT '>>>---------------<<<'

			SET @BATCH_END_TIME = GETDATE();        ---------------------------- ATTRIBUTES TO FULL BATCH EXECUTION TIME
			PRINT '=====================================';
			PRINT 'LAYER LOADING COMPLETED';
			PRINT 'LOAD DURATION FOR WHOLE LAYER: ' + CAST(DATEDIFF(SECOND,@BATCH_START_TIME,@BATCH_END_TIME) AS NVARCHAR) + 'SECONDS' ;
			PRINT '=====================================';
	END TRY
		BEGIN CATCH
    		PRINT '=====================================';
    		PRINT 'ERROR OCCURED';
    		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
    		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER()AS NVARCHAR);
    		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE()AS NVARCHAR);
    		PRINT '=====================================';
		END CATCH
END
                                                              -->>>>>> END OF BRONZE LAYER <<<<<<--
