SELECT 
	REPLACE(cid,'-','') AS cid
FROM 
	bronze.erp_loc_a101


SELECT 
	DISTINCT
	CASE
		WHEN UPPER(TRIM(cntry)) IN ('DE','GERMANY') THEN 'Germany'
		WHEN UPPER(TRIM(cntry)) IN ('USA','US','UNITED STATES') THEN 'United States'
		WHEN cntry is NULL or cntry = '' THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
FROM 
	bronze.erp_loc_a101


-- Final query for transformation
INSERT INTO silver.erp_loc_a101 (
	cid,
	cntry
)
SELECT 
	REPLACE(cid,'-','') AS cid
	,
	CASE
		WHEN UPPER(TRIM(cntry)) IN ('DE','GERMANY') THEN 'Germany'
		WHEN UPPER(TRIM(cntry)) IN ('USA','US','UNITED STATES') THEN 'United States'
		WHEN cntry is NULL or cntry = '' THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
FROM 
	bronze.erp_loc_a101




SELECT DISTINCT cntry
FROM silver.erp_loc_a101

