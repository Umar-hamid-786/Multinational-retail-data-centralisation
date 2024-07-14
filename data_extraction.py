# data_extraction.py
import pandas as pd
import tabula

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






