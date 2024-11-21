
-- TASK 1

-- How many stores does the business have and in which countries?
SELECT country_code, COUNT(*) AS total_no_stores  --Counts the total number of rows for each country_code and names this count total_no_stores.
FROM dim_store_original_v2
GROUP BY country_code
ORDER BY total_no_stores DESC;

--Task 2 

-- Which locations currently have the most stores?
SELECT locality, COUNT(*) AS stores_location --Counts the total number of rows for each locality and names this count stores_location
FROM dim_store_original_v2
GROUP BY locality 
ORDER BY stores_location DESC;

--Task 3


-- Which months produced the largest amount of sales?
SELECT
    ddt.month,
    SUM(dp.product_price * ot.product_quantity) AS total_sales
FROM
    orders_table ot --Table orders that were working with
JOIN
    dim_products_final dp ON ot.product_code = dp.product_code   --Joining products table and orders table together to form new orders
JOIN
    dim_date_times ddt ON ot.date_uuid = ddt.date_uuid --Joining date times and orders table together to form new orders
GROUP BY
    ddt.month
ORDER BY
    total_sales DESC;

--TASK 4


-- How many sales are coming from online?
SELECT
    SUM(CASE WHEN store_type = 'Web Portal' THEN 1 ELSE 0 END) AS online_sales,
    SUM(CASE WHEN store_type != 'Web Portal' THEN 1 ELSE 0 END) AS offline_sales
FROM
    dim_store_original_v2;
SELECT
	COUNT(orders_table.product_code) AS number_of_sales,
	SUM(orders_table.product_quantity) AS product_quantity_count,
CASE 
   	WHEN dim_store_original_v2.store_type = 'Web Portal' THEN 'Web'
   	ELSE 'Offline'
END AS location
FROM
    orders_table
JOIN
    dim_store_original_v2 ON orders_table.store_code = dim_store_original_v2.store_code
GROUP BY
	location
ORDER BY 
	location;

--TASK 5


-- What percentage of sales come through each type of store?
WITH total_sales_cte AS (
    SELECT
        SUM(product_quantity * product_price) AS total_sales
    FROM
        orders_table
    JOIN
        dim_products_final ON orders_table.product_code = dim_products_final.product_code
    JOIN
        dim_store_original_v2 ON orders_table.store_code = dim_store_original_v2.store_code
)   --USING WITH as a subquery to maintain clean query and referencing. This calculates the total sales without group
	-- i.e, the total sales. 

SELECT 
	store_type,
	SUM(product_quantity * product_price) AS total_sales,
	SUM(product_quantity * product_price) / (SELECT total_sales FROM total_sales_cte) * 100 AS percentage_of_total_sales	
FROM orders_table
JOIN dim_products_final ON orders_table.product_code = dim_products_final.product_code 
JOIN dim_store_original_v2 ON orders_table.store_code = dim_store_original_v2.store_code
GROUP BY store_type
ORDER BY total_sales DESC;

--TASK 6


-- Which month in each year produced the highest cost of sales?
SELECT 
	SUM(product_quantity * product_price) AS total_sales,
	ddt.year,
	ddt.month	
FROM orders_table ot
JOIN dim_products_final dp ON ot.product_code = dp.product_code 
JOIN dim_date_times ddt ON ot.date_uuid = ddt.date_uuid
GROUP BY 
    ddt.year, ddt.month
ORDER BY total_sales DESC;

--TASK 7


-- What is our staff headcount?
SELECT 
	SUM(staff_numbers) AS total_staff_numbers,
	country_code 
FROM dim_store_original_v2
GROUP BY 
country_code
ORDER BY total_staff_numbers DESC 

--TASK 8


-- Which German store type is selling the most?
	
SELECT 
	SUM(product_quantity * dp.product_price) AS total_sales,
	ds.store_type,
	ds.country_code
FROM orders_table ot
JOIN dim_products_final dp ON ot.product_code = dp.product_code 
JOIN dim_store_original_v2 ds ON ot.store_code = ds.store_code
WHERE country_code = 'DE'
GROUP BY ds.store_type, ds.country_code
ORDER BY total_sales DESC;



--TASK 9

-- How quickly is the company making sales?

-- Creating the full date column
WITH FullDateTime AS (
SELECT
year,
TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, ' ', time_only), 'YYYY-MM-DD HH24:MI:SS') AS full_timestamp
FROM dim_date_times
ORDER BY full_timestamp DESC
),
-- Comparing the timestamp created to the next timestamp
SalesWithNext AS (
SELECT
year,
full_timestamp,
LEAD(full_timestamp) OVER (ORDER BY full_timestamp DESC) AS next_timestamp
FROM
FullDateTime
) ,
-- finding the average.
AvgTimeSeconds AS (
SELECT
year,
AVG(full_timestamp - next_timestamp) AS avg_time_seconds
FROM SalesWithNext
-- WHERE next_timestamp IS NOT NULL
GROUP BY year
)
-- debug SQL statements for each CTE
-- SELECT * FROM date_times;
-- SELECT * FROM SalesWithNext;
-- SELECT * FROM AvgTimeSeconds;
SELECT
year,
avg_time_seconds
FROM AvgTimeSeconds
ORDER BY avg_time_seconds DESC;



--ALTER TABLE dim_date_times ADD COLUMN time_only TEXT;
--UPDATE dim_date_times
--SET time_only = SUBSTRING(timestamp::TEXT FROM 12 FOR 8);

--ALTER TABLE dim_date_times ALTER COLUMN time_only TYPE CHAR(8);

