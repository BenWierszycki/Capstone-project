import requests
from pprint import pprint
import pandas as pd
from datetime import datetime, timedelta
import psycopg2 as psql
import numpy as np
import time

# URL for data
url = 'https://api.openaq.org/v2/measurements'

# Define the locations with their location_ids
locations = {
    'Westminster': 159,
    'Hillingdon': 153,
    'Manchester': 2312,
    'Oxford': 2469
}

# Define the pollution parameters
parameters = ['pm25', 'o3', 'no2']

# Function with a waiting and retry mechanism when usage limits are exceeded

def fetch_data_with_retry(url, params):
    max_retries = 10 
    retries = 0
    
    while retries < max_retries:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()['results']
        elif response.status_code == 429 or response.status_code == 408:
            time.sleep(30)
            retries += 1
        else:
            return []

    return []

# Function to fetch data for a location and create dataframe
def fetch_location_data(location_name, location_id, parameters, date_from, date_to):
    url = 'https://api.openaq.org/v2/measurements'
    records = {}

# Set parameters
    for parameter in parameters:
        params = {
            'date_from': date_from,
            'date_to': date_to,
            'limit': 100000,
            'sort': 'desc',
            'parameter': parameter,
            'location_id': location_id,
            'order_by': 'datetime'
        }
        
# Fetch data with retry mechanism
        data = fetch_data_with_retry(url, params)
        
# Parse data and organize it into a dictionary
        for result in data:
            date = result['date']['utc']
            value = result['value']
            
            if date not in records:
                records[date] = {'datetime': date}
            
            records[date][f'{location_name}_{parameter}'] = value
    
# Convert dictionary to dataframe
    df = pd.DataFrame.from_dict(records, orient='index')
    df.reset_index(drop=True, inplace=True)
    
    return df

# Checking in SQL to see if table exists

from dotenv import load_dotenv
load_dotenv()
import os
user = os.getenv('user')
password = os.getenv('password')
my_host = os.getenv('host')

conn = psql.connect(database = "pagila",
                    user = user,
                    host = my_host,
                    password = password,
                    port = 5432
                    )


cur = conn.cursor()
sql_count_rows = """ 
(SELECT count(*) FROM student.bw_air_pollution_data
)
"""
cur.execute(sql_count_rows)
conn.commit()
conn.close

row_count = cur.fetchone()[0]

# If table does not exist, load whole previous year of data

if row_count == 0:
    latest_datetime = pd.to_datetime('2023-07-01T00:00:00+00:00')
else:
    uk_df['datetime'] = pd.to_datetime(uk_df['datetime'])
    # Retrieve the latest datetime
    latest_datetime = uk_df['datetime'].max()

# Setting current datetime to -3 hours

current_time_2 = datetime.now() - pd.Timedelta(hours=3)
current_datetime = current_time_2.strftime('%Y-%m-%dT%H:%M:%S+00:00')

# Function to fetch data for UK locations

def fetch_multiple_locations_data(locations, parameters):
    date_from = latest_datetime
    date_to = current_datetime
    
    dfs = []
    for location_name, location_id in locations.items():
        df = fetch_location_data(location_name, location_id, parameters, date_from, date_to)
        if not df.empty:
            dfs.append(df)
    
# Merge all DataFrames on the 'datetime' column
    
    if dfs:
        merged_df = dfs[0]
        for df in dfs[1:]:
            merged_df = pd.merge(merged_df, df, on='datetime', how='outer')
        
        merged_df.sort_values(by='datetime', ascending=False, inplace=True)
        merged_df.reset_index(drop=True, inplace=True)
        return merged_df
    else:
        return None
        
# Fetch data for UK locations
uk_df = fetch_multiple_locations_data(locations, parameters)

# IMPORTING PM2.5 FOR KARACHI, LIMA, SINGAPORE

# Define the locations and their location_ids
locations = {
    'Lima': 2415,
    'Karachi': 8156,
    'Singapore': 367929
}

# Function with a waiting and retry mechanism when usage limits are exceeded

def fetch_data_with_retry(url, params):
    max_retries = 10
    retries = 0
    
    while retries < max_retries:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()['results']
        elif response.status_code == 429:
            time.sleep(30)
            retries += 1
        else:
            return []
    
    return []

# Function to fetch data for a location and create the dataframe

def fetch_location_data(location_name, location_id, parameter, date_from, date_to):
    url = 'https://api.openaq.org/v2/measurements'

# Set parameters
    params = {
        'date_from': date_from,
        'date_to': date_to,
        'limit': 100000,
        'sort': 'desc',
        'parameter': 'pm25',
        'location_id': location_id,
        'order_by': 'datetime'
    }
    
# Fetch data with retry mechanism
    data = fetch_data_with_retry(url, params)
    
# Parse data and organise into dictionary
    records = {}
    for result in data:
        date = result['date']['utc']
        value = result['value']
        
        if date not in records:
            records[date] = {'date': date, f'{location_name}_pm25': None}
        
        records[date][f'{location_name}_pm25'] = value
    
# Convert dictionary to dataframe
    df = pd.DataFrame.from_dict(records, orient='index')
    df.reset_index(drop=True, inplace=True)
    
    return df

# Function to fetch data for global locations

def fetch_multiple_locations_data(locations):
    date_from = latest_datetime
    date_to = current_datetime
    
    dfs = []
    for location_name, location_id in locations.items():
        df = fetch_location_data(location_name, location_id, 'pm25', date_from, date_to)
        if not df.empty:
            if 'date' in df.columns:
                df.rename(columns={'date': 'datetime'}, inplace=True)  # Rename 'date' to 'datetime'
            dfs.append(df)

# Merge all dataframes on datetime
    if dfs:
        merged_df = dfs[0]
        for df in dfs[1:]:
            merged_df = pd.merge(merged_df, df, on='datetime', how='outer')
        
        merged_df.sort_values(by='datetime', ascending=False, inplace=True)
        merged_df.reset_index(drop=True, inplace=True)
        return merged_df
    else:
        return None
        
# Fetch data for global locations
global_df = fetch_multiple_locations_data(locations)

# Merge datframes on datetime
merged_df = pd.merge(uk_df, global_df, on='datetime', how='outer')

# Sort values
merged_df.sort_values(by='datetime', ascending=False, inplace=True)

# Reset index
merged_df.reset_index(drop=True, inplace=True)

from dotenv import load_dotenv
load_dotenv()
import os
user = os.getenv('user')
password = os.getenv('password')
my_host = os.getenv('host')

conn = psql.connect(database = "pagila",
                    user = user,
                    host = my_host,
                    password = password,
                    port = 5432
                    )

#CREATING TABLE

cur = conn.cursor()

# now creating a new table in SQL to add data to

sql_create_table = """ 
CREATE TABLE IF NOT EXISTS student.bw_air_pollution_data (
    datetime TIMESTAMP,
    Westminster_pm25 FLOAT,
    Westminster_o3 FLOAT,
    Westminster_no2 FLOAT,
    Hillingdon_pm25 FLOAT,
    Hillingdon_o3 FLOAT,
    Hillingdon_no2 FLOAT,
    Manchester_pm25 FLOAT,
    Manchester_o3 FLOAT,
    Manchester_no2 FLOAT,
    Oxford_pm25 FLOAT,
    Oxford_o3 FLOAT,
    Oxford_no2 FLOAT,
    Lima_pm25 FLOAT,
    Karachi_pm25 FLOAT,
    Singapore_pm25 FLOAT
)
"""
cur.execute(sql_create_table)
conn.commit()

cur = conn.cursor()

# Iterate over rows and insert 

for values, row in merged_df.iterrows():
    insert_data = f"""
    INSERT INTO student.bw_air_pollution_data (
        datetime,
        Westminster_pm25, Westminster_o3, Westminster_no2,
        Hillingdon_pm25, Hillingdon_o3, Hillingdon_no2,
        Manchester_pm25, Manchester_o3, Manchester_no2,
        Oxford_pm25, Oxford_o3, Oxford_no2,
        Lima_pm25,
        Karachi_pm25,
        Singapore_pm25
    ) 
    VALUES (
        '{row['datetime']}',
        {row['Westminster_pm25'] if not pd.isnull(row['Westminster_pm25']) else 'NULL'},
        {row['Westminster_o3'] if not pd.isnull(row['Westminster_o3']) else 'NULL'},
        {row['Westminster_no2'] if not pd.isnull(row['Westminster_no2']) else 'NULL'},
        {row['Hillingdon_pm25'] if not pd.isnull(row['Hillingdon_pm25']) else 'NULL'},
        {row['Hillingdon_o3'] if not pd.isnull(row['Hillingdon_o3']) else 'NULL'},
        {row['Hillingdon_no2'] if not pd.isnull(row['Hillingdon_no2']) else 'NULL'},
        {row['Manchester_pm25'] if not pd.isnull(row['Manchester_pm25']) else 'NULL'},
        {row['Manchester_o3'] if not pd.isnull(row['Manchester_o3']) else 'NULL'},
        {row['Manchester_no2'] if not pd.isnull(row['Manchester_no2']) else 'NULL'},
        {row['Oxford_pm25'] if not pd.isnull(row['Oxford_pm25']) else 'NULL'},
        {row['Oxford_o3'] if not pd.isnull(row['Oxford_o3']) else 'NULL'},
        {row['Oxford_no2'] if not pd.isnull(row['Oxford_no2']) else 'NULL'},
        {row['Lima_pm25'] if not pd.isnull(row['Lima_pm25']) else 'NULL'},
        {row['Karachi_pm25'] if not pd.isnull(row['Karachi_pm25']) else 'NULL'},
        {row['Singapore_pm25'] if not pd.isnull(row['Singapore_pm25']) else 'NULL'}
    )
    """
    
    cur.execute(insert_data)


conn.commit()
cur.close()
conn.close()