import requests
from datetime import datetime
import pandas as pd
import sqlite3
import os

# Replace with your OpenWeatherMap API key
api_key = 'Give_your_api' #collect you api from OpenWeatherMap

# List of cities for which you want to fetch weather data
cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 
          'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose']

# Base URL for the OpenWeatherMap API
base_url = 'http://api.openweathermap.org/data/2.5/weather'

# Function to fetch weather data from OpenWeatherMap
def fetch_weather_data(city):
    try:
        endpoint = f'{base_url}?q={city}&appid={api_key}&units=metric'
        response = requests.get(endpoint)
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {city}: {e}")
        return None

# Function to extract relevant weather information
def extract_weather_info(data):
    if data:
        weather_info = {
            'City': data['name'],
            'Weather': data['weather'][0]['description'],
            'Temperature_Celsius': data['main']['temp'],
            'Humidity': data['main']['humidity'],
            'Wind_speed': data['wind']['speed'],
            'Timestamp': datetime.fromtimestamp(data['dt']).strftime('%Y-%m-%d %H:%M:%S')
        }
        return weather_info
    return None

# Main function to orchestrate data extraction and tabulation

weather_data_list = []

for city in cities:
        # Fetch data from OpenWeatherMap
        weather_data = fetch_weather_data(city)

        if weather_data:
            # Extract relevant information
            weather_info = extract_weather_info(weather_data)

            # Append to the list
            if weather_info:
                weather_data_list.append(weather_info)
        else:
            print(f"Failed to fetch weather data for {city}")

# Create a DataFrame from the list of weather information
df_weather = pd.DataFrame(weather_data_list)



# Optional: Save DataFrame to a CSV file for later use
df_weather.to_csv('weather_data.csv', index=False)

df_weather = df_weather.dropna()  # Drop rows with any missing values

# Remove duplicates if any
df_weather = df_weather.drop_duplicates()

# Ensure correct data types
df_weather['Temperature_Celsius'] = df_weather['Temperature_Celsius'].astype(float)
df_weather['Humidity'] = df_weather['Humidity'].astype(int)
df_weather['Wind_speed'] = df_weather['Wind_speed'].astype(float)
df_weather['Timestamp'] = pd.to_datetime(df_weather['Timestamp'])

# Enrich the data
df_weather['Temperature_Fahrenheit'] = df_weather['Temperature_Celsius'] * 9/5 + 32

# Separate the Timestamp into Date, Month, Year, and Time
df_weather['Date'] = df_weather['Timestamp'].dt.day
df_weather['Month'] = df_weather['Timestamp'].dt.strftime('%B')
df_weather['Year'] = df_weather['Timestamp'].dt.year
df_weather['Time'] = df_weather['Timestamp'].dt.strftime('%H:%M:%S')

# Drop the original Timestamp column
df_weather = df_weather.drop(columns=['Timestamp'])

# Normalize the data
df_weather['City'] = df_weather['City'].str.title()  # Capitalize city names
df_weather['Weather'] = df_weather['Weather'].str.capitalize()  # Capitalize weather descriptions


# Function to insert DataFrame into a SQLite database
def insert_into_db(df, db_name='Database/weather_data.db', table_name='weather'):
    # Ensure the database directory exists
    os.makedirs(os.path.dirname(db_name), exist_ok=True)

    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        city TEXT,
        weather TEXT,
        temperature_celsius REAL,
        humidity INTEGER,
        wind_speed REAL,
        temperature_fahrenheit REAL,
        date INTEGER,
        month TEXT,
        year INTEGER,
        time TEXT
    )
    ''')

    # Insert DataFrame into the database
    df.to_sql(table_name, conn, if_exists='append', index=False)

    # Commit changes and close connection
    conn.commit()
    conn.close()

# Insert DataFrame into SQLite database
insert_into_db(df_weather)