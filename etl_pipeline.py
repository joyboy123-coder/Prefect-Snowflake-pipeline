from etl_python.extract import extract_data
from etl_python.transform import transform_data
from etl_python.load import load
import logging
import os

logging.basicConfig(
    filename='log_file.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    raw_data = os.path.join(BASE_DIR, 'data', 'raw_data.csv')
    df = extract_data(raw_data)     # ✅ Returns df
    df = transform_data(df)         # ✅ Accepts df
    load(df)
    logging.info('ETL Pipeline Finished Successfully :>')

if __name__ == "__main__":
    main()
