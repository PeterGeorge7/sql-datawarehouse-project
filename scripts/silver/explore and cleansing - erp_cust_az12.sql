SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) ELSE cid END AS cid
FROM [Datawarehouse].[bronze].[erp_cust_az12]
WHERE bdate > GETDATE()


SELECT
	CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END AS bdate
FROM [Datawarehouse].[bronze].[erp_cust_az12]
WHERE bdate > GETDATE()



SELECT 
	DISTINCT
	CASE
		WHEN gen = 'F' THEN 'Female'
		WHEN gen = 'M' THEN 'Male'
		WHEN gen = '' THEN 'n/a'
		WHEN gen IS NULL THEN 'n/a'
		ELSE gen
	END AS gen
FROM 
	bronze.erp_cust_az12



-- final query transformation
INSERT INTO silver.erp_cust_az12 (
	cid,
	bdate,
	gen
)
SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) ELSE cid END AS cid,
	CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END AS bdate,
	CASE
		WHEN gen = 'F' THEN 'Female'
		WHEN gen = 'M' THEN 'Male'
		WHEN gen = '' THEN 'n/a'
		WHEN gen IS NULL THEN 'n/a'
		ELSE gen
	END AS gen
FROM [Datawarehouse].[bronze].[erp_cust_az12]


-- date quality check
SELECT *
FROM silver.erp_cust_az12