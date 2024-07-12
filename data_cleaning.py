# data_cleaning.py
import pandas as pd

class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self, df):
        df = df.dropna()  # Drop rows with NULL values
        df = df.drop_duplicates()  # Remove duplicate rows
        return df


