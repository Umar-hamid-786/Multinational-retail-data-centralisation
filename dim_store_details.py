from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

db_connector_1 = DatabaseConnector(creds_path='db_creds.yaml') 

db_connector_2 = DatabaseConnector(creds_path='sales_data_creds.yaml')

data_extractor = DataExtractor()

data_cleaning = DataCleaning()


 # Define the headers
headers = {
"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
    }

    # Define the endpoints
number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
store_details_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"

    # Step 1: Get the number of stores
number_of_stores = DataExtractor.list_number_of_stores(number_of_stores_endpoint, headers)
print(number_of_stores)

    # Step 2: Retrieve all stores data
stores_data_df = DataExtractor.retrieve_stores_data(store_details_endpoint, number_of_stores, headers)
    #print(stores_data_df)

df_cleaned_stores = data_cleaning.clean_store_data(stores_data_df)

upload_df_stores = df_cleaned_stores
db_connector_2.upload_to_db(upload_df_stores, 'dim_store_original_v2')

