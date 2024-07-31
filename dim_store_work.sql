

ALTER TABLE dim_store_original_v2
	ALTER COLUMN store_code TYPE VARCHAR(12)

ALTER TABLE dim_store_original_v2
ADD PRIMARY KEY (store_code);


ALTER TABLE orders_table
ADD CONSTRAINT fk_store_code
FOREIGN KEY (store_code) 
REFERENCES dim_store_original_v2 (store_code);


SELECT * FROM dim_store_original
WHERE store_code = 'WEB-1388012W'