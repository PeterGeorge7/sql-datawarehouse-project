CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

    DECLARE @step_start DATETIME, @step_end DATETIME;

    ---------------------------
    -- crm_cust_info
    ---------------------------
    SET @step_start = GETDATE();
    PRINT 'STEP 1: Loading crm_cust_info - START at ' + CONVERT(VARCHAR(30), @step_start, 120);

    TRUNCATE TABLE silver.crm_cust_info;
    INSERT INTO silver.crm_cust_info (
        cst_id,
        csv_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT 
        cst_id,
        csv_key,
        COALESCE(TRIM(cst_firstname),'n/a'),
        COALESCE(TRIM(cst_lastname),'n/a'),
        CASE UPPER(TRIM(cst_marital_status))
            WHEN 'M' THEN 'Married'
            WHEN 'S' THEN 'Single'
            ELSE 'n/a'
        END,
        CASE UPPER(TRIM(cst_gndr))
            WHEN 'M' THEN 'Male'
            WHEN 'F' THEN 'Female'
            ELSE 'n/a'
        END,
        cst_create_date
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS latest
        FROM bronze.crm_cust_info 
        WHERE cst_id IS NOT NULL
    ) ranked
    WHERE ranked.latest = 1;

    SET @step_end = GETDATE();
    PRINT 'STEP 1: Loading crm_cust_info - END at ' + CONVERT(VARCHAR(30), @step_end, 120) 
          + ' | Duration: ' + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' sec';


    ---------------------------
    -- crm_prd_info
    ---------------------------
    SET @step_start = GETDATE();
    PRINT 'STEP 2: Loading crm_prd_info - START at ' + CONVERT(VARCHAR(30), @step_start, 120);

    TRUNCATE TABLE silver.crm_prd_info;
    INSERT INTO silver.crm_prd_info (
        prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
    )
    SELECT 
        prd_id,
        REPLACE(SUBSTRING(prd_key,1,5),'-','_'),
        SUBSTRING(prd_key,7,LEN(prd_key)),
        prd_nm,
        ISNULL(prd_cost,0),
        CASE TRIM(prd_line)
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'M' THEN 'Mountain'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END,
        CAST (prd_start_dt AS DATE),
        CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)
    FROM bronze.crm_prd_info;

    SET @step_end = GETDATE();
    PRINT 'STEP 2: Loading crm_prd_info - END at ' + CONVERT(VARCHAR(30), @step_end, 120) 
          + ' | Duration: ' + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' sec';


    ---------------------------
    -- crm_sales_details
    ---------------------------
    SET @step_start = GETDATE();
    PRINT 'STEP 3: Loading crm_sales_details - START at ' + CONVERT(VARCHAR(30), @step_start, 120);

    TRUNCATE TABLE silver.crm_sales_details;
    INSERT INTO silver.crm_sales_details (
        sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt,
        sls_due_dt, sls_sales, sls_quantity, sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
            ELSE CAST(CAST (sls_order_dt AS VARCHAR) AS DATE)
        END,
        CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
            ELSE CAST(CAST (sls_ship_dt AS VARCHAR) AS DATE)
        END,
        CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
            ELSE CAST(CAST (sls_due_dt AS VARCHAR) AS DATE)
        END,
        CASE
            WHEN sls_sales = 0 OR sls_sales IS NULL OR sls_sales != ABS(sls_price) * sls_quantity THEN (ABS(sls_price) * sls_quantity)
            WHEN sls_sales < 0 THEN ABS(sls_sales)
            ELSE sls_sales
        END,
        sls_quantity,
        CASE
            WHEN sls_price = 0 OR sls_price IS NULL THEN (sls_sales / sls_quantity)
            WHEN sls_price < 0 THEN ABS(sls_price)
            ELSE sls_price
        END
    FROM [Datawarehouse].[bronze].[crm_sales_details];

    SET @step_end = GETDATE();
    PRINT 'STEP 3: Loading crm_sales_details - END at ' + CONVERT(VARCHAR(30), @step_end, 120) 
          + ' | Duration: ' + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' sec';


    ---------------------------
    -- erp_cust_az12
    ---------------------------
    SET @step_start = GETDATE();
    PRINT 'STEP 4: Loading erp_cust_az12 - START at ' + CONVERT(VARCHAR(30), @step_start, 120);

    TRUNCATE TABLE silver.erp_cust_az12;
    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) ELSE cid END,
        CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
        CASE
            WHEN gen = 'F' THEN 'Female'
            WHEN gen = 'M' THEN 'Male'
            WHEN gen = '' THEN 'n/a'
            WHEN gen IS NULL THEN 'n/a'
            ELSE gen
        END
    FROM [Datawarehouse].[bronze].[erp_cust_az12];

    SET @step_end = GETDATE();
    PRINT 'STEP 4: Loading erp_cust_az12 - END at ' + CONVERT(VARCHAR(30), @step_end, 120) 
          + ' | Duration: ' + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' sec';


    ---------------------------
    -- erp_loc_a101
    ---------------------------
    SET @step_start = GETDATE();
    PRINT 'STEP 5: Loading erp_loc_a101 - START at ' + CONVERT(VARCHAR(30), @step_start, 120);

    TRUNCATE TABLE silver.erp_loc_a101;
    INSERT INTO silver.erp_loc_a101 (cid, cntry)
    SELECT 
        REPLACE(cid,'-',''),
        CASE
            WHEN UPPER(TRIM(cntry)) IN ('DE','GERMANY') THEN 'Germany'
            WHEN UPPER(TRIM(cntry)) IN ('USA','US','UNITED STATES') THEN 'United States'
            WHEN cntry IS NULL OR cntry = '' THEN 'n/a'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;

    SET @step_end = GETDATE();
    PRINT 'STEP 5: Loading erp_loc_a101 - END at ' + CONVERT(VARCHAR(30), @step_end, 120) 
          + ' | Duration: ' + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' sec';


    ---------------------------
    -- erp_px_cat_g1v2
    ---------------------------
    SET @step_start = GETDATE();
    PRINT 'STEP 6: Loading erp_px_cat_g1v2 - START at ' + CONVERT(VARCHAR(30), @step_start, 120);

    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT id, cat, subcat, maintenance
    FROM bronze.erp_px_cat_g1v2;

    SET @step_end = GETDATE();
    PRINT 'STEP 6: Loading erp_px_cat_g1v2 - END at ' + CONVERT(VARCHAR(30), @step_end, 120) 
          + ' | Duration: ' + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' sec';


    PRINT 'ALL STEPS COMPLETED SUCCESSFULLY at ' + CONVERT(VARCHAR(30), GETDATE(), 120);

END
