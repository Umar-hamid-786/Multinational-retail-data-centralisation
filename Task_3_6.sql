--SELECT MAX(LENGTH("month")) FROM dim_date_times
--SELECT MAX(LENGTH("year")) FROM dim_date_times
--SELECT MAX(LENGTH("day")) FROM dim_date_times
--SELECT MAX(LENGTH("time_period")) FROM dim_date_times

ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2) USING month::VARCHAR(2),
ALTER COLUMN year TYPE VARCHAR(4) USING year::VARCHAR(4),
ALTER COLUMN day TYPE VARCHAR(2) USING day::VARCHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(10) USING time_period::VARCHAR(10),
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;