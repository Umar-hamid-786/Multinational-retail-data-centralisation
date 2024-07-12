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
    

