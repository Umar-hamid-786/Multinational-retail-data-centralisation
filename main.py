# main.py

from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def main():
    # Initialize DatabaseConnector for the RDS
    db_connector_1 = DatabaseConnector(creds_path='db_creds.yaml') 
    
    # Initialize the database engine for RDS
    engine_1 = db_connector_1.init_db_engine()
    
    # List all tables in RDS
    tables_1 = db_connector_1.list_db_tables(engine_1)
    print("Tables in first database:", tables_1)
    
    # Initialize DatabaseConnector for local db
    db_connector_2 = DatabaseConnector(creds_path='sales_data_creds.yaml')
    
    # Initialize the database engine for local db
    engine_2 = db_connector_2.init_db_engine()
    
    # List all tables in local db
    tables_2 = db_connector_2.list_db_tables(engine_2)
    print("Tables in second database:", tables_2)
    
    # Initialize DataExtractor
    data_extractor = DataExtractor()
    
    # Read data from a specific table in the first database
    table_name_1 = 'your_table_name_1'  # Replace with your actual table name in the first database
    df_1 = data_extractor.read_rds_table(db_connector_1, table_name_1)
    print(df_1.head())

    # Read data from a specific table in the second database
    table_name_2 = 'your_table_name_2'  # Replace with your actual table name in the second database
    df_2 = data_extractor.read_rds_table(db_connector_2, table_name_2)
    print(df_2.head())

    # Initialize DataCleaning
    data_cleaning = DataCleaning()
    
    # Clean the data from the first database
    df_1_cleaned = data_cleaning.clean_user_data(df_1)
    print(df_1_cleaned.head())
    
    # Clean the data from the second database
    df_2_cleaned = data_cleaning.clean_user_data(df_2)
    print(df_2_cleaned.head())

    # Upload the cleaned DataFrame to the first database
    upload_table_name = 'cleaned_table_1'
    db_connector_1.upload_to_db(df_1_cleaned, upload_table_name)
    print(f"Cleaned DataFrame uploaded to table {upload_table_name} in the first database.")

    # Upload the cleaned DataFrame to the second database
    upload_table_name_2 = 'cleaned_table_2'
    db_connector_2.upload_to_db(df_2_cleaned, upload_table_name_2)
    print(f"Cleaned DataFrame uploaded to table {upload_table_name_2} in the second database.")

if __name__ == "__main__":
    main()