# data_extraction.py
import pandas as pd
import tabula
import requests


class DataExtractor:
    def __init__(self):
        pass

    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine()
        df = pd.read_sql_table(table_name, engine)
        return df
    
    def retrieve_pdf_data(self,pdf_path):
        dfs = tabula.read_pdf(pdf_path, pages= 'all')
        all_data = pd.concat(dfs, ignore_index=True)
        print(all_data)
        return all_data
    
    def list_number_of_stores(endpoint: str, headers: dict) -> int:
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            return response.json().get('number_stores')
        else:
            response.raise_for_status()

    def retrieve_stores_data(base_endpoint: str, number_of_stores: int, headers: dict) -> pd.DataFrame:
        store_data_list = []
        
        for store_number in range(1, number_of_stores):
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






