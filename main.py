# main.py

from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import requests


def main():
    # Initialize DatabaseConnector for the RDS
    db_connector_1 = DatabaseConnector(creds_path='db_creds.yaml') 
    
    # Initialize the database engine for RDS
    #engine_1 = db_connector_1.init_db_engine()
    
    # Initialize DatabaseConnector for local db
    db_connector_2 = DatabaseConnector(creds_path='sales_data_creds.yaml')
    
    # Initialize the database engine for local db
    #engine_2 = db_connector_2.init_db_engine()

    # List all tables in RDS
    tables_1 = db_connector_1.list_db_tables()
    #print(tables_1)

    #Intialise the DataExtractor class
    data_extractor = DataExtractor()

    #Extract the data users from RDS
    df = data_extractor.read_rds_table(db_connector_1, 'legacy_users')
    #print(df.head())

    #Extract the data users from RDS
    #df_card = data_extractor.read_rds_table(db_connector_1, 'dim_card_details')
    #print(df_card.head())

    #Extract the df from PDF
    pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    pdf_data= data_extractor.retrieve_pdf_data(pdf_link)
    #print(pdf_data)

    # Initialize DataCleaning of extracted RDS table
    data_cleaning = DataCleaning()

    # Initialize DataCleaning of extracted RDS table
    df_cleaned = data_cleaning.clean_user_data(df)
    #print(df_cleaned.head())

    # Upload the cleaned DataFrame to the local db
    upload_df = df_cleaned
    db_connector_2.upload_to_db(upload_df, 'dim users 2')

    # Initialize DataCleaning of extracted RDS table
    df_cleaned_card = data_cleaning.clean_card_data(pdf_data)
    #print(df_cleaned_card.head())

    # Upload the cleaned DataFrame to the local db
    upload_df_card = df_cleaned_card 
    db_connector_2.upload_to_db(upload_df_card, 'dim_card_details')

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
    print(stores_data_df)

    # Initialize DataCleaning of extracted RDS table
    df_cleaned_stores = data_cleaning.clean_store_data(stores_data_df)

    # Upload the cleaned DataFrame to the local db
    upload_df_stores = df_cleaned_stores
    db_connector_2.upload_to_db(upload_df_stores, 'dim_store_details')




if __name__ == "__main__":
    main()    

  
    

