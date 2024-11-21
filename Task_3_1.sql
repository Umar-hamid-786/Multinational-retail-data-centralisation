--SELECT MAX(LENGTH(card_number::TEXT)) FROM orders_table; -- Result: 19
--SELECT MAX(LENGTH(store_code::TEXT)) FROM orders_table; -- Result: 12
--SELECT MAX(LENGTH(product_code::TEXT)) FROM orders_table; -- Result: 11

-- Cast the columns of Orders table to correct data types
ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN card_number TYPE VARCHAR(19) USING card_number::TEXT,
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN product_quantity TYPE SMALLINT;
