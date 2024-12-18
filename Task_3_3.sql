--ALTER TABLE dim_store_details
--DROP COLUMN lat;

--Alter dim store table to ensure all N/A entries are NULL
UPDATE dim_store_original_v2
SET longitude = NULL
WHERE longitude = 'N/A';

UPDATE dim_store_original_v2
SET latitude = NULL
WHERE latitude = 'N/A';


SELECT MAX(LENGTH(store_code)) FROM dim_store_original;

--Cast the correct data types in dim store table 
ALTER TABLE dim_store_original_v2
    ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT,
    ALTER COLUMN locality TYPE VARCHAR(255),
    --ALTER COLUMN store_code TYPE VARCHAR(11),
    ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
    ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT,
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN continent TYPE VARCHAR(255);

