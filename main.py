from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import requests
import os

def main():


    #######  DATA UTILITY ########

    #  # Initialize DatabaseConnector for the RDS
    db_connector_1 = DatabaseConnector(creds_path='db_creds.yaml') 
    
    # Initialize the database engine for RDS
    #engine_1 = db_connector_1.init_db_engine()
    
    # Initialize DatabaseConnector for local db
    db_connector_2 = DatabaseConnector(creds_path='sales_data_creds.yaml')
    
    # Initialize the database engine for local db
    #engine_2 = db_connector_2.init_db_engine()

    # List all tables in RDS
    tables_1 = db_connector_1.list_db_tables()
    print(tables_1)


    ################# DATA EXTRACTION ########################################################################################

    #Intialise the DataExtractor class
    data_extractor = DataExtractor()

    ###### DIM USERS EXTRACTION ######


    #Extract the data users from RDS FOR USERS
    df = data_extractor.read_rds_table(db_connector_1, 'legacy_users')
    #print(df.head())

        ###### DIM CARD DETAILS EXTRACTION ######

    #Extract the data users from RDS FOR CARD DETAILS
    df_card = data_extractor.read_rds_table(db_connector_1, 'dim_card_details')
    print(df_card.head())

    #Extract the df from PDF FOR CARD DETAILS
    pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    pdf_data= data_extractor.retrieve_pdf_data(pdf_link)
    #print(pdf_data)

     ###### DIM STORE DETAILS EXTRACTION #####

    # Define the headers
    headers = {
    "x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
    }

    # Define the endpoints
    number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    store_details_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"

    # Get the number of stores
    number_of_stores = DataExtractor.list_number_of_stores(number_of_stores_endpoint, headers)
    print(number_of_stores)

    # Retrieve all stores data
    stores_data_df = DataExtractor.retrieve_stores_data(store_details_endpoint, number_of_stores, headers)
    print(stores_data_df)


    #### DIM PRODUCTS EXTRACTION ####

    # Extract the data from S3 task 6 FOR PRODUCT TABLE
    s3_address = 's3://data-handling-public/products.csv'
    df_s3 = data_extractor.extract_from_s3(s3_address)
    print("Data extracted from S3:", df_s3)

    #### DIM DATE TIMES EXTRACTION ####

    # Extract the TIME DATA 
    s3_address_json =  'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json' 
    bucket_name = 'data-handling-public'
    file_path = 'date_details.json'
    df_s3_date_times = data_extractor.extract_from_s3_general(bucket_name, file_path)
    print("Data extracted from S3 date_times:", df_s3_date_times)

     #### ORDERS TABLE EXTRACTION ####
    
    #orders table extract
    orders_table = data_extractor.read_rds_table(db_connector_1, 'orders_table')

    #########DATA CLEANING ########################################################################################


    # Initialize DataCleaning of extracted RDS table
    data_cleaning = DataCleaning()

    # Initialize DataCleaning of extracted RDS table FOR USER DATA
    df_cleaned = data_cleaning.clean_user_data(df)
    print(df_cleaned.head())

    # Initialize DataCleaning of extracted RDS table FOR CARD DETAILS
    df_cleaned_card = data_cleaning.clean_card_data(pdf_data)
    print(df_cleaned_card.head())

    #Initialize DataCleaning of extracted RDS table FOR STORES
    df_cleaned_stores = data_cleaning.clean_store_data(stores_data_df)


    # Initialize DataCleaning of extracted RDS table FOR PRODUCTS
    df_s3_cleaned_weights= data_cleaning.convert_product_weights(df_s3)
    df_s3_cleaned = data_cleaning.clean_products_data(df_s3_cleaned_weights)


    # Initialize DataCleaning of extracted RDS table FOR DATE TIMES
    df_s3_cleaned_date_times = data_cleaning.clean_date_times(df_s3_date_times)

    # Initialize DataCleaning of extracted RDS table FOR ORDERS
    orders_table_cleaned = data_cleaning.clean_orders_data(orders_table)


     #########DATA LOADING ########################################################################################


    # Upload the cleaned DataFrame to the local db FOR USERS TABLE
    upload_df = df
    db_connector_2.upload_to_db(upload_df, 'raw_legacy')

    # Upload the cleaned DataFrame to the local db FOR CARD TABLE
    upload_df_card = pdf_data
    db_connector_2.upload_to_db(upload_df_card, 'dim_card_details_new_3')

    # Upload the cleaned DataFrame to the local db FOR STORE TABLE
    upload_df_stores = df_cleaned_stores
    db_connector_2.upload_to_db(upload_df_stores, 'dim_store_details')

     # Upload the cleaned DataFrame to the local db FOR PRODUCTS TABLE
    upload_df_s3 = df_s3_cleaned
    db_connector_2.upload_to_db(upload_df_s3, 'dim_products_final')

    # Upload the cleaned DataFrame to the local db FOR DATE TIMES TABLE
    upload_df_s3_date_times = df_s3_cleaned_date_times
    db_connector_2.upload_to_db(upload_df_s3_date_times, 'dim_date_times')


    # Upload the cleaned DataFrame to the local db FOR ORDERS TABLE
    upload_orders_table = orders_table_cleaned 
    db_connector_2.upload_to_db(upload_orders_table, 'orders_table_raw')


    # Extract the data from S3
    #s3_address = 's3://data-handling-public/products.csv'
    #df_s3 = data_extractor.extract_from_s3(s3_address)
    #print("Data extracted from S3:", df_s3.head())

    #local_path = os.path.join(os.getcwd(), "unclean_user_data.csv")
    #df.to_csv(local_path, index=False)

if __name__ == "__main__":
    main()
