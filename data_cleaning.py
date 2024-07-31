# data_cleaning.py
import pandas as pd
import numpy as np
from dateutil import parser


class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self, df):
        df['phone_number'] = df['phone_number'].str.replace(r'[^0-9]', '')
        df['country_code'] = df['country_code'].str.replace('GGB', 'GB')
        valid_country_codes = ['DE', 'GB', 'US']
        df = df[df['country_code'].isin(valid_country_codes)]
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format = 'mixed')
        df['join_date'] = pd.to_datetime(df['join_date'], format = 'mixed')
        #df.drop(columns=['index'], inplace=True)
        #regex_expression = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$' 
        #df.loc[~df['phone_number'].str.match(regex_expression), 'phone_number'] = np.nan 
        #df['phone_number'] = df['phone_number'].replace({r'\+44': '0', r'\(': '', r'\)': '', r'-': '', r' ': ''}, regex=True)
        df.dropna(inplace=True)  # Drop rows with NULL values
        df.drop_duplicates(inplace=True)  # Remove duplicate rows
        #df['first_name'] = df['first_name'].str.strip()
        #df['last_name'] = df['last_name'].str.strip()
        #valid_country_codes = ['DE', 'GB', 'US']
        #df = df[df['country_code'].isin(valid_country_codes)]
        return df
    
    def clean_card_data(self, df):
        def parse_and_format_date(date):
            try:
                parsed_date = parser.parse(date)
                return parsed_date.strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                return pd.NaT     
        df['date_payment_confirmed'] = df['date_payment_confirmed'].apply(parse_and_format_date)
        #df['card_number'] = df['card_number'].str.replace('?', '')
        df.dropna(inplace=True)  # Drop rows with NULL values
        df.drop_duplicates(inplace=True)  # Remove duplicate rows
        return df
    
    def clean_store_data(self,df):
        #df['address'] = df['address'].str.replace('\n', ', ')
        #df['locality'].fillna('Unknown', inplace=True)
        valid_country_codes = ['DE', 'GB', 'US']
        df = df[df['country_code'].isin(valid_country_codes)]
        df['continent'] = df['continent'].replace('eeEurope', 'Europe')
        df['continent'] = df['continent'].replace('eeAmerica', 'America')
        df['staff_numbers'] = df['staff_numbers'].str.replace(r'\D', '', regex=True)
        #df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        #df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        #df['opening_date'] = pd.to_datetime(df['opening_date'], format = 'mixed')
        #df['address'] = df['address'].str.strip()
        #df['locality'] = df['locality'].str.strip()
        #df['country_code'] = df['country_code'].str.strip()
        #df['continent'] = df['continent'].str.strip()
        #df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce')
        df['latitude'].fillna(0, inplace=True)
        df['longitude'].fillna(0, inplace=True)
        df.drop(columns=['index'], inplace=True)
        df.drop(columns=['lat'], inplace=True)
        #df.dropna(inplace=True)  # Drop rows with NULL values
        #df.drop_duplicates(inplace=True)  # Remove duplicate rows
        return df
    
    def convert_product_weights(self,df):

        def convert_multiples(weight):
            try:
                weight = str(weight)
                if 'x' in weight:
                    weight = str(weight).strip('gkg')
                    parts = weight.split('x')
                    quantity = float(parts[0].strip())
                    unit_weight = float(parts[1].strip())
                    total_weight = quantity * unit_weight
                    return str(total_weight)
                else:
                    return weight
            except ValueError:
                return None


        def convert_weight(weight):
            try:
                weight = str(weight).lower().strip()
                weight = weight.split()[0]
                if 'kg' in weight:
                    return float(weight.replace('kg', '').strip())
                elif 'g' in weight:
                    return float(weight.replace('g', '').strip()) / 1000
                elif 'ml' in weight:
                    return float(weight.replace('ml', '').strip()) / 1000
                elif 'l' in weight:
                    return float(weight.replace('l', '').strip())
                elif 'oz' in weight:
                    return float(weight.replace('oz','').strip()) * 0.28 
                else:
                    return float(weight)
            except ValueError:
                return None
        

        df['weight'] = df['weight'].apply(convert_multiples) 
        df['weight'] = df['weight'].apply(convert_weight)
        #df['weight'] = pd.to_numeric(df['weight'], errors='coerce')
        #df.dropna(subset=['weight'], inplace=True)
        return df       
        


    def clean_products_data(self, df):
        df.drop(columns=['Unnamed: 0'], inplace=True)
        #df = df.drop(df[df['weight'] != float])
        df.dropna(inplace=True)  # Drop rows with NULL values
        df.drop_duplicates(inplace=True)  # Remove duplicate rows
        df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
        return df


    def clean_orders_data(self, df):
            df.drop(columns=['index'], inplace=True)
            df.drop(columns=['first_name'], inplace=True)
            df.drop(columns=['last_name'], inplace=True)
            df.drop(columns=['1'], inplace=True)
            df.dropna(inplace=True)  # Drop rows with NULL values
            df.drop_duplicates(inplace=True)  # Remove duplicate rows
            return df
    
    def clean_date_times(self,df):
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df.dropna(inplace=True)  # Drop rows with NULL values
        df.drop_duplicates(inplace=True)  # Remove duplicate rows
        return df 