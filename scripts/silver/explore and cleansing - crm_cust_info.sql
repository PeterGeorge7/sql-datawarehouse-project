USE Datawarehouse
GO

-- Checking for duplicated customer id
-- Expectations: No result
-- what i got: There is duplicated values and nulls

SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL
GO

-- got here that we could take only the latest using the date
SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29449


-- way to order by date to choose the latest
WITH ranked AS (
SELECT * FROM (SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS latest
FROM bronze.crm_cust_info 
WHERE cst_id IS NOT NULL
) t
WHERE t.latest = 1
)
SELECT cst_id, COUNT(*)
FROM ranked
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL -- correct , here is no result


SELECT TRIM(cst_firstname) cst_firstname,TRIM(cst_lastname) cst_lastname
FROM bronze.crm_cust_info -- TRIM the space before and after the string 
-- dont forget to handle the null values in the first and last name


SELECT 
CASE cst_gndr
	WHEN 'M' THEN 'Male'
	WHEN 'F' THEN 'Female'
	ELSE 'n/a'
END cst_gndr2
FROM bronze.crm_cust_info


-- Query to Transform and load data in the silver layer
-- crm_cust_info
INSERT INTO silver.crm_cust_info (
	cst_id,
	csv_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)
SELECT 
	cst_id,
	csv_key,
	COALESCE(TRIM(cst_firstname),'n/a') cst_firstname, -- to normalize nulls
	COALESCE(TRIM(cst_lastname),'n/a') cst_lastname, -- to normalize nulls

	CASE UPPER(TRIM(cst_marital_status))
		WHEN 'M' THEN 'Married'
		WHEN 'S' THEN 'Single'
		ELSE 'n/a'
	END cst_marital_status,

	CASE UPPER(TRIM(cst_gndr))
		WHEN 'M' THEN 'Male'
		WHEN 'F' THEN 'Female'
		ELSE 'n/a'
	END cst_gndr,

	cst_create_date
FROM (
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS latest
	FROM bronze.crm_cust_info 
	WHERE cst_id IS NOT NULL
) ranked
WHERE ranked.latest = 1

-- ==================================================
-- crm_prd_info : 

SELECT * 
FROM bronze.crm_prd_info



SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL