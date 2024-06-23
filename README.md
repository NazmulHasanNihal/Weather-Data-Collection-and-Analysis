# Weather Data ETL and Analysis

This project fetches current weather data for a list of cities from the OpenWeatherMap API, processes and transforms the data, and stores it in a SQLite database. The data can then be used for analysis with Power BI.

## Project Structure

- `weather_data.py`: The main Python script that performs the ETL (Extract, Transform, Load) operations.
- `Database/weather_data.db`: The SQLite database where the processed weather data is stored.
- `weather_data.csv`: A CSV file containing the weather data.
- `README.md`: This file.

## Requirements

- Python 3.x
- The following Python libraries:
  - requests
  - pandas
  - sqlite3 (comes with Python standard library)

You can install the required libraries using:
```sh
pip install requests pandas
