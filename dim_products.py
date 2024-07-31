from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning



    # Initialize DatabaseConnector for local db
db_connector_2 = DatabaseConnector(creds_path='sales_data_creds.yaml')

data_extractor = DataExtractor()

# Extract the data from S3 task 6
s3_address = 's3://data-handling-public/products.csv'
df_s3 = data_extractor.extract_from_s3(s3_address)
print("Data extracted from S3:", df_s3)

# Initialize DataCleaning of extracted RDS table
   
data_cleaning = DataCleaning()


# Initialize DataCleaning of extracted RDS table
df_s3_cleaned_weights= data_cleaning.convert_product_weights(df_s3)
df_s3_cleaned = data_cleaning.clean_products_data(df_s3_cleaned_weights)

upload_df_s3 = df_s3_cleaned
db_connector_2.upload_to_db(upload_df_s3, 'dim_products_final')