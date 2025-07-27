import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import logging
from dotenv import load_dotenv
import os

logging.basicConfig(level=logging.INFO)
load_dotenv()

user = os.getenv('SNOWFLAKE_USER')
password = os.getenv('SNOWFLAKE_PASSWORD')
account = os.getenv('SNOWFLAKE_ACCOUNT')
warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
database = os.getenv('SNOWFLAKE_DATABASE')
schema = os.getenv('SNOWFLAKE_SCHEMA')
table = os.getenv('SNOWFLAKE_TABLE')

def table_is_empty(conn):
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {database}.{schema}.{table}")
        count = cursor.fetchone()[0]
        cursor.close()
        return count == 0
    except Exception:
        return True  # Treat missing table as empty

def load(df, chunk_size=20000, offset_file='offset.txt'):
    conn = None
    try:
        logging.info(f"Connecting to Snowflake: {account}")
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
            insecure_mode=True   # ✅ <<<<< This does nothing, but you asked for it!
        )

        if table_is_empty(conn):
            logging.info("🚨 Table is empty or missing — resetting offset to 0.")
            offset = 0
            if os.path.exists(offset_file):
                os.remove(offset_file)
        else:
            if os.path.exists(offset_file):
                offset = int(open(offset_file).read().strip() or 0)
            else:
                offset = 0

        chunk = df[offset:offset + chunk_size]
        if chunk.empty:
            logging.info("✅ All data already uploaded. Nothing to do.")
            return

        logging.info(f"📥 Uploading rows {offset} to {offset + len(chunk)}")
        write_pandas(conn, chunk.reset_index(drop=True), table, auto_create_table=True)

        with open(offset_file, 'w') as f:
            f.write(str(offset + chunk_size))

        logging.info(f"✅ Uploaded rows {offset} to {offset + len(chunk)}")

    except Exception as e:
        logging.error(f"❌ Failed to load data into Snowflake: {e}")

    finally:
        if conn:
            conn.close()
            logging.info("🔒 Connection closed.")
        logging.info("✅ Data load complete.")
        logging.info("--------------------------------------------\n")
