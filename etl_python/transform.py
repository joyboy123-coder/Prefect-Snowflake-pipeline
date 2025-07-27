import pandas as pd
import logging
import random
import re
import numpy as np


def transform_data(df):
    try:
        logging.info('Started Data Cleaning and transformations...\n')

        # Standardize column names
        df.columns = df.columns.str.strip().str.upper()

        # Remove duplicates
        logging.info('Removing Duplicate Rows :>')
        df.drop_duplicates(inplace=True)

        # Clean NAME column
        logging.info('Cleaning NAME Column... :>')
        df['NAME'] = df['NAME'].astype(str).apply(lambda x: re.sub(r'[^a-zA-Z\s]', '', x)).str.strip()
        df['NAME'] = df['NAME'].replace('', np.nan)

        names = ['Frank', 'Charlie', 'Heidi', 'Eve', 'David', 'Bob', 'Alice', 'Grace']
        df['NAME'] = df['NAME'].apply(lambda x: random.choice(names) if pd.isna(x) else x)

        # Clean EMAIL column
        logging.info('Cleaning EMAIL Column... :>')
        df['EMAIL'] = df['EMAIL'].fillna('invalid_email@gmail.com')
        df['EMAIL'] = df['NAME'].str.replace(' ', '') + df.index.astype(str) + '@gmail.com'
        df['EMAIL'] = df['EMAIL'].str.lower()

        # Clean AGE column
        logging.info('Cleaning AGE Column... :>')
        df['AGE'] = pd.to_numeric(df['AGE'], errors='coerce')
        df['AGE'] = df['AGE'].apply(lambda x: random.randint(18, 60) if pd.isna(x) else x).astype(int)

        # Clean COUNTRY column
        logging.info('Cleaning COUNTRY Column... :>')
        df['COUNTRY'] = df['COUNTRY'].astype(str).str.strip()
        df['COUNTRY'] = df['COUNTRY'].apply(lambda x: re.sub(r'[^a-zA-Z\s]', '', x))
        df['COUNTRY'] = df['COUNTRY'].replace(['nan', ''], np.nan)
        country_names = ['USA', 'France', 'Germany', 'UK', 'India']
        df['COUNTRY'] = df['COUNTRY'].apply(lambda x: random.choice(country_names) if pd.isna(x) else x)

        # Clean SALARY column
        logging.info('Cleaning SALARY Column... :>')
        df['SALARY'] = pd.to_numeric(df['SALARY'], errors='coerce')
        df['SALARY'] = df['SALARY'].apply(lambda x: random.randint(30000, 140000) if pd.isna(x) else x)

        # Clean JOIN_DATE column
        logging.info('Cleaning JOIN_DATE Column... :>')
        df['JOIN_DATE'] = pd.to_datetime(df['JOIN_DATE'], errors='coerce')
        num_missing = df['JOIN_DATE'].isna().sum()
        if num_missing > 0:
            random_dates = pd.to_datetime(
                np.random.choice(
                    pd.date_range(start='2021-01-01', end='2025-06-25'),
                    size=num_missing
                )
            )
            df.loc[df['JOIN_DATE'].isna(), 'JOIN_DATE'] = random_dates
        
        df['JOIN_DATE'] = df['JOIN_DATE'].dt.date

        # Clean IS_ACTIVE column
        logging.info('Cleaning IS_ACTIVE Column... :>')

        def is_bool(x):
            return str(x).strip().lower() in ['1', 'yes', 'true']

        df['IS_ACTIVE'] = df['IS_ACTIVE'].apply(is_bool)

        logging.info('Transformation and Data Cleaning Successful... :>')
        return df

    except Exception as e:
        logging.error(f'Exception failed: {e}')
        raise e  # Optional: stop the flow so you know something is wrong
    finally:
        logging.info('----------------------------------------------------------------')

