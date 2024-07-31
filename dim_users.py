from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

db_connector_1 = DatabaseConnector(creds_path='db_creds.yaml') 

db_connector_2 = DatabaseConnector(creds_path='sales_data_creds.yaml')

tables_1 = db_connector_1.list_db_tables()
print(tables_1)

data_extractor = DataExtractor()

df = data_extractor.read_rds_table(db_connector_1, 'legacy_users')

data_cleaning = DataCleaning()

df_cleaned = data_cleaning.clean_user_data(df)

print(df_cleaned)

db_connector_2.upload_to_db(df_cleaned, 'dim_users_cleaned')