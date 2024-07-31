# data_extraction.py
import pandas as pd
import tabula
import requests
import boto3
import os
import tempfile
import json

class DataExtractor:
    def __init__(self):
        pass
        """
        Initializes the DataExtractor class.

        """
    def read_rds_table(self, db_connector, table_name):
        """
        Reads data from an RDS table.

        Args:
            db_connector: The database connector object used to connect to the database.
            table_name (str): The name of the table to read from.

        Returns:
            pd.DataFrame: The data from the table as a pandas DataFrame.
        """
        engine = db_connector.init_db_engine()
        df = pd.read_sql_table(table_name, engine)
        return df
    
    def retrieve_pdf_data(self,pdf_path):
        """
        Extracts data from a PDF file.

        Args:
            pdf_path (str): The file path to the PDF document.

        Returns:
            pd.DataFrame: A concatenated DataFrame containing all data extracted from the PDF.
        """
        dfs = tabula.read_pdf(pdf_path, pages= 'all')
        all_data = pd.concat(dfs, ignore_index=True)
        print(all_data)
        return all_data
    
    def list_number_of_stores(endpoint: str, headers: dict) -> int:
        """
        Retrieves the number of stores from an API endpoint.

        Args:
            endpoint (str): The API endpoint URL.
            headers (dict): The headers to include in the API request.

        Returns:
            int: The number of stores.
        """
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            return response.json().get('number_stores')
        else:
            response.raise_for_status()

    def retrieve_stores_data(base_endpoint: str, number_of_stores: int, headers: dict) -> pd.DataFrame:
        """
        Retrieves detailed data for each store from an API.

        Args:
            base_endpoint (str): The base API endpoint URL.
            number_of_stores (int): The total number of stores to retrieve data for.
            headers (dict): The headers to include in the API requests.

        Returns:
            pd.DataFrame: A DataFrame containing data for all stores.
        """
        store_data_list = []
        
        for store_number in range(0, (number_of_stores)):
            endpoint = f"{base_endpoint}/{store_number}"
            response = requests.get(endpoint, headers=headers)        
            print(response)  
            if response.status_code == 200:
                store_data = response.json()
                store_data_list.append(store_data)
            else:
                response.raise_for_status()
        # Convert the list of store data to a pandas DataFrame
        store_data_df = pd.DataFrame(store_data_list)
        return store_data_df     

    def extract_from_s3(self, s3_address):
        """
    Downloads a CSV file from an S3 bucket and loads it into a pandas DataFrame.

    Args:
        s3_address (str): The S3 address of the CSV file in the format 's3://bucket_name/key'.

    Returns:
        pd.DataFrame: A DataFrame containing the data from the CSV file.
        """

        # Remove the "s3://" prefix and split into bucket name and key
        s3_address = s3_address.replace("s3://", "")
        bucket_name, key = s3_address.split('/', 1) 
        s3 = boto3.client('s3')
        local_path = os.getcwd() + "/product_tables.csv"

        s3.download_file(bucket_name, key, local_path)
        # Read the CSV content into a pandas DataFrame
        df = pd.read_csv(local_path)
        return df
    
    def extract_from_s3_general(self, bucket_name, file_key):
        """
    Downloads a JSON file from an S3 bucket and loads it into a pandas DataFrame.

    Args:
        bucket_name (str): The name of the S3 bucket.
        file_key (str): The key (path) of the JSON file within the bucket.

    Returns:
        pd.DataFrame: A DataFrame containing the data from the JSON file.
        """

        # Initialize S3 client
        s3_client = boto3.client('s3')
        local_path = os.getcwd() + "/date_details.json"
        # Download the JSON file from S3
        s3_client.download_file(bucket_name, file_key, local_path)
        with open('date_details.json') as f:
            data = json.load(f)
            df = pd.DataFrame(data)
        return df 






