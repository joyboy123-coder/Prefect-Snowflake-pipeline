import pandas as pd
import logging

logging.basicConfig(
    filename='log_file.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def extract_data(raw_data):
    try:
        logging.info('Extracting the Data :>')
        df = pd.read_csv(raw_data)
        return df
    except Exception as e:
        logging.error(f'Extraction failed {e}')
    finally:
        logging.info('Extraction Successful')
        logging.info('---------------------------------------\n')
