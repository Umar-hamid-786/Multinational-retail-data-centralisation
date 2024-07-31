from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
    
db_connector_1 = DatabaseConnector(creds_path='db_creds.yaml') 

db_connector_2 = DatabaseConnector(creds_path='sales_data_creds.yaml')

data_extractor = DataExtractor()


pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
pdf_data= data_extractor.retrieve_pdf_data(pdf_link)

data_cleaning = DataCleaning()

df_cleaned_card = data_cleaning.clean_card_data(pdf_data)

upload_df_card = pdf_data
db_connector_2.upload_to_db(upload_df_card, 'dim_card_details_test')




