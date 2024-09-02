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

def append_to_snowflake_raw(df):
    conn = get_snowflake_connection()
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
        conn.close()

if __name__ == "__main__":
    data_raw = scrape_visa_wait_times()
    if data_raw is not None:
        append_to_snowflake_raw(data_raw)
    else:
        logger.error("Failed to retrieve data.")