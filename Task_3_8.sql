-- Create foreign and primary key relationship between orders table and the other tables


ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

ALTER TABLE dim_products_final
ADD PRIMARY KEY (product_code);

ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_card_details_new
ADD PRIMARY KEY (card_number);

--Updating card details to ensure card number is in the correct format (numbers) via REGEX expression

UPDATE dim_card_details
SET card_number = REGEXP_REPLACE(card_number, '[^0-9]', '', 'g')
WHERE card_number ~ '[^0-9]';

-- Creating constriants for each table to secure the primary and foriegn key relationship

ALTER TABLE orders_table
ADD CONSTRAINT fk_product_code
FOREIGN KEY (product_code)
REFERENCES dim_products_final (product_code);

ALTER TABLE orders_table DROP CONSTRAINT fk_product_code;
