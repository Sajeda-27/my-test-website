import os
import json
import pandas as pd
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import sqlite3

# Path to your OAuth2 credentials file
OAUTH2_CREDENTIALS_FILE = r'C:\\Users\\LigaData-Sajedah\\Desktop\\project2\\oauth_sajeda.json'  # Adjust the path

# The ID of your GA4 property
PROPERTY_ID = '450680225'  # Replace with the actual property ID

def get_credentials():
    """Authenticate and return the OAuth2 credentials."""
    flow = InstalledAppFlow.from_client_secrets_file(
        OAUTH2_CREDENTIALS_FILE,
        scopes=['https://www.googleapis.com/auth/analytics.readonly']
    )
    credentials = flow.run_local_server(port=0)
    return credentials

def initialize_analyticsdata(credentials):
    """Initializes a GA4 Analytics Data API service object.
    Returns:
      An authorized Analytics Data API service object.
    """
    analytics = build('analyticsdata', 'v1beta', credentials=credentials)
    return analytics

def get_report(analytics, start_date, end_date):
    """Queries the Analytics Data API.
    Args:
      analytics: An authorized Analytics Data API service object.
      start_date: The start date for fetching data.
      end_date: The end date for fetching data.
    Returns:
      The Analytics Data API response.
    """
    request_body = {
        "dateRanges": [
            {"startDate": start_date, "endDate": end_date}
        ],
        "metrics": [
            {"name": "activeUsers"},
            {"name": "sessions"},
            {"name": "screenPageViews"}
        ]
    }

    return analytics.properties().runReport(
        property=f"properties/{PROPERTY_ID}",
        body=request_body
    ).execute()

def process_data(response):
    """Processes the Analytics Data API response.
    Args:
      response: An Analytics Data API response.
    Returns:
      Processed data as a list of dictionaries.
    """
    processed_data = []
    rows = response.get('rows', [])
    for row in rows:
        data = {}
        dimensions = row['dimensionValues']
        metrics = row['metricValues']
        data['country'] = dimensions[0]['value']
        data['city'] = dimensions[1]['value']
        data['activeUsers'] = metrics[0]['value']
        data['sessions'] = metrics[1]['value']
        data['screenPageViews'] = metrics[2]['value']
        data['date'] = datetime.today().strftime('%Y-%m-%d')  # Adding current date
        processed_data.append(data)
    
    return processed_data

def save_to_csv(data, filename):
    """Saves processed data to a CSV file.
    Args:
      data: Processed data as a list of dictionaries.
      filename: The name of the CSV file to save.
    """
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)

def insert_data_to_db(data, db_name):
    """Inserts processed data into the SQLite database."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    for entry in data:
        cursor.execute('''
            INSERT INTO analytics_table (country, city, activeUsers, sessions, screenPageViews, date)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (entry['country'], entry['city'], entry['activeUsers'], entry['sessions'], entry['screenPageViews'], entry['date']))
    conn.commit()
    conn.close()

def main():
    try:
        # Calculate the start and end date for the previous day
        end_date = datetime.today() - timedelta(days=1)
        start_date = end_date

        # Format dates as strings
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        credentials = get_credentials()
        analytics = initialize_analyticsdata(credentials)
        response = get_report(analytics, start_date_str, end_date_str)

        # Process the data
        processed_data = process_data(response)

        # Save the data to CSV
        save_to_csv(processed_data, 'analytics_data.csv')

        # Insert data into the database
        db_name = 'C:\\Users\\LigaData-Sajedah\\Desktop\\project2\\testDB.db'  # Adjust the path to your database file
        insert_data_to_db(processed_data, db_name)

        print(f"Data saved to analytics_data.csv and inserted into {db_name}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    main()
