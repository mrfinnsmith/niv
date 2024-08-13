import requests
from bs4 import BeautifulSoup
import datetime
import re
import pandas as pd

def log_all_links(url):
    # Specify the parser to use
    parser = 'html.parser'
    
    response = requests.get(url, verify=False)

    soup = BeautifulSoup(response.content, parser)

    # Find all 'a' tags and log their text if 'href' contains 'pdf'
    links = soup.find_all('a')
    latest_date = datetime.datetime.min
    latest_excel_link = None

    for link in links:
        href = link.get('href')
        if href and 'pdf' in href:  # Check if 'href' attribute exists and contains 'pdf'
            print(link.text)  # Print the text of the link
            
            # Extracting the date from the link text
            match = re.search(r'\b(\w+)\s+(\d{4})\b', link.text)
            if match:
                month, year = match.groups()
                date_value = datetime.datetime.strptime(f'{month} {year}', '%B %Y')
                print(date_value)  # Print the date value
                
                # Update latest date if found date is more recent
                if date_value > latest_date:
                    latest_date = date_value
                    # Adjust to find the Excel link
                    excel_link = link.find_next_sibling('a')
                    if excel_link and '.xlsx' in excel_link.get('href', ''):
                        latest_excel_link = 'https://travel.state.gov' + excel_link.get('href')

    # Print the latest date if it's a valid date found in the links
    if latest_date > datetime.datetime.min:
        print("Latest date found:", latest_date)
        print("Here is the latest Excel link:", latest_excel_link)
        
        # Read and display the first 10 lines from the Excel file
        if latest_excel_link:
            df = pd.read_excel(latest_excel_link, sheet_name='Sheet1')
            print(df.head(10))

# URL to be passed to the function
url = "https://travel.state.gov/content/travel/en/legal/visa-law0/visa-statistics/nonimmigrant-visa-statistics/monthly-nonimmigrant-visa-issuances.html"
log_all_links(url)