# data_cleaning.py
import pandas as pd
import numpy as np

class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self, df):
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format='%Y-%m-%d', errors='coerce')
        df['join_date'] = pd.to_datetime(df['join_date'], format='%Y-%m-%d', errors='coerce')
        valid_country_codes = ['DE', 'GB', 'US']
        df = df[df['country_code'].isin(valid_country_codes)]
        df.drop(columns=['index'], inplace=True)
        #regex_expression = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$' #Our regular expression to match
        #df.loc[~df['phone_number'].str.match(regex_expression), 'phone_number'] = np.nan # For every row  where the Phone column does not match our regular expression, replace the value with NaN
        #df['phone_number'] = df['phone_number'].replace({r'\+44': '0', r'\(': '', r'\)': '', r'-': '', r' ': ''}, regex=True)
        df.dropna(inplace=True)  # Drop rows with NULL values
        df.drop_duplicates(inplace=True)  # Remove duplicate rows
        #df['first_name'] = df['first_name'].str.strip()
        #df['last_name'] = df['last_name'].str.strip()
        #valid_country_codes = ['DE', 'GB', 'US']
        #df = df[df['country_code'].isin(valid_country_codes)]
        return df
    def clean_card_data(self, df):
         df.dropna(inplace=True)  # Drop rows with NULL values
         df.drop_duplicates(inplace=True)  # Remove duplicate rows
         return df


