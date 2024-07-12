# main.py

from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

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
    print(tables_1)

    #Intialise the DataExtractor class
    data_extractor = DataExtractor()

    #Extract the data from RDS
    df = data_extractor.read_rds_table(db_connector_1, 'legacy_store_details')
    print(df.head())

    # Initialize DataCleaning of extracted RDS table
    data_cleaning = DataCleaning()
    df_cleaned = data_cleaning.clean_user_data(df)
    print(df_cleaned.head())

    # Upload the cleaned DataFrame to the local db
    upload_df = df_cleaned
    db_connector_2.upload_to_db(upload_df, 'legacy_store_details_cleaned')


if __name__ == "__main__":
    main()    

  
    

