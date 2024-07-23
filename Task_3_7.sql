--SELECT MAX(LENGTH(card_number)) FROM dim_card_details_new;
--SELECT MAX(LENGTH(expiry_date)) FROM dim_card_details_new;

ALTER TABLE dim_card_details_new
ALTER COLUMN card_number TYPE VARCHAR(22),
ALTER COLUMN expiry_date TYPE VARCHAR(5),
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;