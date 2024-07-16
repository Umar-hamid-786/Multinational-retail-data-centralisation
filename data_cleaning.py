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
         df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
         df.dropna(inplace=True)  # Drop rows with NULL values
         df.drop_duplicates(inplace=True)  # Remove duplicate rows
         return df
    def clean_store_data(self,df):
        df['address'] = df['address'].str.replace('\n', ', ')
        # For simplicity, filling NaNs with a placeholder. Adjust based on the context of your data.
        df['lat'].fillna(0, inplace=True)
        df['longitude'].fillna(0, inplace=True)
        df['locality'].fillna('Unknown', inplace=True)
        df['continent'] = df['continent'].replace('eeEurope', 'Europe')
        df['continent'] = df['continent'].replace('eeAmerica', 'America')
        df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        # Trim whitespace from text fields
        df['address'] = df['address'].str.strip()
        df['locality'] = df['locality'].str.strip()
        df['country_code'] = df['country_code'].str.strip()
        df['continent'] = df['continent'].str.strip()
        df.drop(columns=['index'], inplace=True)
        valid_country_codes = ['DE', 'GB', 'US']
        df = df[df['country_code'].isin(valid_country_codes)]
        df.dropna(inplace=True)  # Drop rows with NULL values
        df.drop_duplicates(inplace=True)  # Remove duplicate rows
        return df





