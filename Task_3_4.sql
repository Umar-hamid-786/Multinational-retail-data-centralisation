ALTER TABLE dim_products_final
ADD COLUMN weight_class VARCHAR(20);

-- Create a new weight_class column to create classes for the weight of products 

UPDATE dim_products_final
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
    ELSE NULL
END;
