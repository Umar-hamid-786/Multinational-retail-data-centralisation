--SELECT MAX(LENGTH(card_number::TEXT)) FROM orders_table; -- Result: 19
--SELECT MAX(LENGTH(store_code::TEXT)) FROM orders_table; -- Result: 12
--SELECT MAX(LENGTH(product_code::TEXT)) FROM orders_table; -- Result: 11

-- Alter table based on the determined lengths
ALTER TABLE dim_users_cleaned
	ALTER COLUMN first_name TYPE VARCHAR(255),
	ALTER COLUMN last_name TYPE VARCHAR(255),
	ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE,
	ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
	ALTER COLUMN join_date TYPE DATE USING join_date::DATE;
	
