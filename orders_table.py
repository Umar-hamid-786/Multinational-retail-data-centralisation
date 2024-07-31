from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning   

 # Initialize DatabaseConnector for the RDS
db_connector_1 = DatabaseConnector(creds_path='db_creds.yaml') 

# Initialize DatabaseConnector for local db
db_connector_2 = DatabaseConnector(creds_path='sales_data_creds.yaml')

data_extractor = DataExtractor()

# Initialize DataCleaning of extracted RDS table
data_cleaning = DataCleaning()


# orders table extract
orders_table = data_extractor.read_rds_table(db_connector_1, 'orders_table')
orders_table_cleaned = data_cleaning.clean_orders_data(orders_table)

# Upload the cleaned DataFrame to the local db
upload_orders_table = orders_table_cleaned 
db_connector_2.upload_to_db(upload_orders_table, 'orders_table_raw')