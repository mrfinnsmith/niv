import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import logging
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import traceback
import pytz
import re

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

URL = "https://travel.state.gov/content/travel/en/us-visas/visa-information-resources/global-visa-wait-times.html"

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.environ.get('SNOWFLAKE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
        account=os.environ.get('SNOWFLAKE_ACCOUNT'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
        database=os.environ.get('SNOWFLAKE_DATABASE'),
        schema=os.environ.get('SNOWFLAKE_SCHEMA')
    )

def is_weekday():
    return datetime.now(pytz.timezone('America/New_York')).weekday() < 5

def parse_html_table(html):
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table')
    if not table:
        raise ValueError("No table found in the HTML content")

    headers = [th.text.strip() for th in table.find_all('th')]
    visa_types = headers[1:]

    table_data = []
    current_date = datetime.now(pytz.timezone('America/New_York')).date()
    
    for row in table.find_all('tr')[1:]:
        cells = row.find_all('td')
        if cells:
            post = cells[0].text.strip()
            for j, cell in enumerate(cells[1:], 1):
                appointment_wait_time_raw = cell.text.strip()
                if appointment_wait_time_raw:
                    visa_type = visa_types[j-1].replace('\xa0', ' ')
                    row_data = [
                        current_date,
                        post,
                        visa_type,
                        appointment_wait_time_raw
                    ]
                    table_data.append(row_data)

    return table_data

def scrape_visa_wait_times():
    if not is_weekday():
        logger.info("Today is not a weekday. Exiting.")
        return None

    try:
        response = requests.get(URL)
        response.raise_for_status()
        table_data = parse_html_table(response.text)
        df = pd.DataFrame(table_data, columns=['DATE', 'POST', 'NONIMMIGRANT_VISA_TYPE', 'APPOINTMENT_WAIT_TIME_RAW'])
        return df

    except requests.RequestException as e:
        logger.error(f"Error fetching the page: {e}")
    except Exception as e:
        logger.error(f"Error parsing the data: {e}")

    return None

def process_visa_type(visa_type):
    return visa_type.replace('Interview Required', '').strip().replace('\xa0', ' ')

def parse_appointment_wait_time(wait_time_raw):
    if wait_time_raw.lower() == 'same day':
        return 0
    days_match = re.search(r'(\d+)\s*(day|days)', wait_time_raw, re.IGNORECASE)
    return int(days_match.group(1)) if days_match else None

def determine_status(wait_time_raw, wait_time_days):
    if wait_time_raw and (wait_time_days is None or wait_time_days == '' or wait_time_days == 0):
        return wait_time_raw
    return ''

def append_to_snowflake_raw(df, conn):
    cursor = conn.cursor()
    table_name = os.environ.get('SNOWFLAKE_VISA_WAIT_TIME_RAW_TABLE')
    
    try:
        cursor.execute(f"SELECT MAX(DATE) FROM {table_name}")
        max_date = cursor.fetchone()[0]
        
        if max_date is None or df['DATE'].max() > max_date:
            success, num_chunks, num_rows, output = write_pandas(conn, df, table_name)
            logger.info(f"Inserted {num_rows} new rows into the raw table.")
        else:
            logger.info("No new data to insert into raw table.")
    
    except Exception as e:
        logger.error(f"Error inserting data into Snowflake raw table: {e}")
        print(f"Full error traceback:\n{traceback.format_exc()}")
    
    finally:
        cursor.close()

def append_to_snowflake_processed(df, conn):
    cursor = conn.cursor()
    table_name = os.environ.get('SNOWFLAKE_VISA_WAIT_TIME_TABLE')
    
    try:
        cursor.execute(f"SELECT MAX(DATE) FROM {table_name}")
        max_date = cursor.fetchone()[0]
        
        if max_date is None or df['DATE'].max() > max_date:
            processed_df = df.copy()
            processed_df['NONIMMIGRANT_VISA_TYPE'] = processed_df['NONIMMIGRANT_VISA_TYPE'].apply(process_visa_type)
            processed_df['APPOINTMENT_WAIT_TIME'] = processed_df['APPOINTMENT_WAIT_TIME_RAW'].apply(parse_appointment_wait_time)
            processed_df['STATUS'] = processed_df.apply(lambda row: determine_status(row['APPOINTMENT_WAIT_TIME_RAW'], row['APPOINTMENT_WAIT_TIME']), axis=1)
            processed_df = processed_df.drop(columns=['APPOINTMENT_WAIT_TIME_RAW'])

            success, num_chunks, num_rows, output = write_pandas(conn, processed_df, table_name)
            logger.info(f"Inserted {num_rows} new rows into the not-raw table.")
        else:
            logger.info("No new data to insert into not-raw table.")
    
    except Exception as e:
        logger.error(f"Error inserting data into Snowflake not-raw table: {e}")
        print(f"Full error traceback:\n{traceback.format_exc()}")
    
    finally:
        cursor.close()


if __name__ == "__main__":
    conn = get_snowflake_connection()
    try:
        data_raw = scrape_visa_wait_times()
        if data_raw is not None:
            append_to_snowflake_raw(data_raw, conn)
            append_to_snowflake_processed(data_raw, conn)
        else:
            logger.error("Failed to retrieve data.")
    finally:
        conn.close()