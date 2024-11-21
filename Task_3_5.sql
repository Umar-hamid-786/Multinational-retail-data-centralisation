ALTER TABLE dim_products_final
RENAME COLUMN removed TO still_available;

--SELECT MAX(LENGTH("EAN")) FROM dim_products;
--SELECT MAX(LENGTH("product_code")) FROM dim_products;
--SELECT MAX(LENGTH("weight_class")) FROM dim_products;

-- Replace Pound sign with empty space to normalise column 

UPDATE dim_products_final
SET product_price = REPLACE(product_price, '£', '')

-- Cast correct data types to dim_proucts table including a bool type column.
  
ALTER TABLE dim_products_final
ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT,
ALTER COLUMN "EAN" TYPE VARCHAR(17) USING "EAN"::VARCHAR(17),
ALTER COLUMN product_code TYPE VARCHAR(11) USING  product_code::VARCHAR(11),
ALTER COLUMN weight_class TYPE VARCHAR(14) USING weight_class::VARCHAR(14),
ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
ALTER COLUMN still_available TYPE BOOL USING
  CASE
    WHEN still_available = 'Still_avaliable' THEN true
    WHEN still_available = 'Removed' THEN false
    ELSE NULL
  END;
