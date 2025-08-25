-- check for the dates
SELECT 
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt < 1900101 OR sls_order_dt <= 0
OR LEN(sls_order_dt) = 0 OR LEN(sls_order_dt) != 8


-- sls int numbers
-- should not be nulls or zeros or negatives
SELECT
	sls_sales AS old_sales,
	sls_quantity,
	sls_price AS old_price,
	CASE
		WHEN sls_sales = 0 OR sls_sales IS NULL OR sls_sales != ABS(sls_price) * sls_quantity THEN (ABS(sls_price) * sls_quantity)
		WHEN sls_sales < 0 THEN ABS(sls_sales)
		ELSE sls_sales
	END as sls_sales,

	CASE
		WHEN sls_price = 0 OR sls_price IS NULL THEN (sls_sales / sls_quantity)
		WHEN sls_price < 0 THEN ABS(sls_price)
		ELSE sls_price
	END as sls_price
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL OR 
sls_sales <= 0 OR 
sls_price IS NULL OR
sls_price <= 0


-- final query ready to transformation
INSERT INTO silver.crm_sales_details (
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT 
	[sls_ord_num],
	[sls_prd_key],
	[sls_cust_id],
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	ELSE CAST(CAST (sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,

	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	ELSE CAST(CAST (sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,

	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	ELSE CAST(CAST (sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,

	CASE
		WHEN sls_sales = 0 OR sls_sales IS NULL OR sls_sales != ABS(sls_price) * sls_quantity THEN (ABS(sls_price) * sls_quantity)
		WHEN sls_sales < 0 THEN ABS(sls_sales)
		ELSE sls_sales
	END as sls_sales,

	sls_quantity,

	CASE
		WHEN sls_price = 0 OR sls_price IS NULL THEN (sls_sales / sls_quantity)
		WHEN sls_price < 0 THEN ABS(sls_price)
		ELSE sls_price
	END as sls_price

FROM [Datawarehouse].[bronze].[crm_sales_details]






SELECT *
FROM silver.crm_sales_details a
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt