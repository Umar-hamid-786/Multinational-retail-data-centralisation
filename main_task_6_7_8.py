from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import requests

def main():
    
    # Initialize DatabaseConnector for the RDS
    #db_connector_1 = DatabaseConnector(creds_path='db_creds.yaml') 
    
    # Initialize the database engine for RDS
    #engine_1 = db_connector_1.init_db_engine()
    
    # Initialize DatabaseConnector for local db
    db_connector_2 = DatabaseConnector(creds_path='sales_data_creds.yaml')


    data_extractor = DataExtractor()

    # Extract the data from S3 task 6
    s3_address = 's3://data-handling-public/products.csv'
    df_s3 = data_extractor.extract_from_s3(s3_address)
    print("Data extracted from S3:", df_s3)

    # Extract the data from S3 task 6
    #s3_address_json =  'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json' 
    #bucket_name = 'data-handling-public'
    #file_path = 'date_details.json'
    #df_s3_date_times = data_extractor.extract_from_s3_general(bucket_name, file_path)
    #print("Data extracted from S3 date_times:", df_s3_date_times)

    # Initialize DataCleaning of extracted RDS table
    data_cleaning = DataCleaning()


    # Initialize DataCleaning of extracted RDS table
    df_s3_cleaned_weights= data_cleaning.convert_product_weights(df_s3)
    df_s3_cleaned = data_cleaning.clean_products_data(df_s3_cleaned_weights)

    # Initialize DataCleaning of extracted RDS table
    #df_s3_cleaned_date_times = data_cleaning.clean_date_times(df_s3_date_times)


    # Upload the cleaned DataFrame to the local db
    upload_df_s3 = df_s3_cleaned
    db_connector_2.upload_to_db(upload_df_s3, 'dim_products_final')
    
    # Upload the cleaned DataFrame to the local db
    #upload_df_s3_date_times = df_s3_cleaned_date_times
    #db_connector_2.upload_to_db(upload_df_s3_date_times, 'dim_date_times')

    # orders table extract
    #orders_table = data_extractor.read_rds_table(db_connector_1, 'orders_table')
    #orders_table_cleaned = data_cleaning.clean_orders_data(orders_table)

    # Upload the cleaned DataFrame to the local db
    #upload_orders_table = orders_table_cleaned 
    #db_connector_2.upload_to_db(upload_orders_table, 'orders_table_raw')






if __name__ == "__main__":
    main()        