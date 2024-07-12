# data_extraction.py
import pandas as pd

class DataExtractor:
    def __init__(self):
        pass

    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine()
        df = pd.read_sql_table(table_name, engine)
        return df





    # Methods for extracting data from different sources will be defined here
    # These methods will include extracting data from CSV files, APIs, and S3 buckets