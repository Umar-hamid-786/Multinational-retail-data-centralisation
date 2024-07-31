    
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning



    # Initialize DatabaseConnector for local db
db_connector_2 = DatabaseConnector(creds_path='sales_data_creds.yaml')

data_extractor = DataExtractor()

    # Extract the data from S3 task 6
s3_address_json =  'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json' 
bucket_name = 'data-handling-public'
file_path = 'date_details.json'
df_s3_date_times = data_extractor.extract_from_s3_general(bucket_name, file_path)
print("Data extracted from S3 date_times:", df_s3_date_times)

# Initialize DataCleaning of extracted RDS table
   
data_cleaning = DataCleaning()


# Initialize DataCleaning of extracted RDS table
df_s3_cleaned_date_times = data_cleaning.clean_date_times(df_s3_date_times)


# Upload the cleaned DataFrame to the local db
upload_df_s3_date_times = df_s3_cleaned_date_times
db_connector_2.upload_to_db(upload_df_s3_date_times, 'dim_date_times')