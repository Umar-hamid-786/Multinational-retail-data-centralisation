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
        df.dropna(inplace=True)  # Drop rows with NULL values
        df.drop_duplicates(inplace=True)  # Remove duplicate rows
        return df
    
    def clean_card_data(self, df):
        def parse_and_format_date(date):
            try:
                parsed_date = parser.parse(date)
                return parsed_date.strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                return pd.NaT     
        df['date_payment_confirmed'] = df['date_payment_confirmed'].apply(parse_and_format_date)
        df.dropna(inplace=True)  # Drop rows with NULL values
        df.drop_duplicates(inplace=True)  # Remove duplicate rows
        return df
    
    def clean_store_data(self,df):
        valid_country_codes = ['DE', 'GB', 'US']
        df = df[df['country_code'].isin(valid_country_codes)]
        df['continent'] = df['continent'].replace('eeEurope', 'Europe')
        df['continent'] = df['continent'].replace('eeAmerica', 'America')
        df['staff_numbers'] = df['staff_numbers'].str.replace(r'\D', '', regex=True)
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
