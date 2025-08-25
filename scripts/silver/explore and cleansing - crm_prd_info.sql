SELECT *
FROM bronze.crm_prd_info


-- check for the duplicates and nulls prd_id
-- Result: No return
SELECT prd_id,COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- extract cat_id to join with the erp categories
SELECT * FROM bronze.erp_px_cat_g1v2 
WHERE id IN (
SELECT 
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id
FROM bronze.crm_prd_info p
)

-- extract prd_key to join with the crm sales details
SELECT * FROM bronze.crm_sales_details 
WHERE sls_prd_key NOT IN (
SELECT 
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key
FROM bronze.crm_prd_info p
)

-- check for prd name needs any trim ? 
-- result none
SELECT *
FROM bronze.crm_prd_info
WHERE TRIM(prd_nm) != prd_nm


SELECT ISNULL(prd_cost,0)
FROM bronze.crm_prd_info


INSERT INTO silver.crm_prd_info (
	prd_id,  
	cat_id,	 
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
	SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost,0) AS prd_cost,
	CASE TRIM(prd_line)
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'M' THEN 'Mountain'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line,
	CAST (prd_start_dt AS DATE) AS prd_start_dt,
	CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info