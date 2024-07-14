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

    #Extract the data users from RDS
    df = data_extractor.read_rds_table(db_connector_1, 'legacy_users')
    print(df.head())

    #Extract the data users from RDS
    df_card = data_extractor.read_rds_table(db_connector_1, 'dim_card_details')
    print(df_card.head())

    #Extract the df from PDF
    pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    pdf_data= data_extractor.retrieve_pdf_data(pdf_link)
    print(pdf_data)

    # Initialize DataCleaning of extracted RDS table
    data_cleaning = DataCleaning()

    # Initialize DataCleaning of extracted RDS table
    df_cleaned = data_cleaning.clean_user_data(df)
    print(df_cleaned.head())

    # Initialize DataCleaning of extracted RDS table
    df_cleaned_card = data_cleaning.clean_card_data(df_card)
    print(df_cleaned_card.head())

    # Upload the cleaned DataFrame to the local db
    upload_df = df_cleaned
    upload_df_card = df_cleaned_card 
    db_connector_2.upload_to_db(upload_df, 'dim users 2')
    db_connector_2.upload_to_db(upload_df_card, 'dim_card_details')

if __name__ == "__main__":
    main()    

  
    

