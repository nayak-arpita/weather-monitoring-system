import requests
import schedule
import time
import sqlite3
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

# Global API settings
API_KEY = "your_openweathermap_api_key"
CITIES = {
    "Delhi": 1273294,
    "Mumbai": 1275339,
    "Chennai": 1264527,
    "Bangalore": 1277333,
    "Kolkata": 1275004,
    "Hyderabad": 1269843
}
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
ALERT_THRESHOLD = 35  # Example: Alert when temp exceeds 35°C for 2 consecutive updates

# Database setup (SQLite for storing daily summaries)
conn = sqlite3.connect('weather_data.db', check_same_thread=False)
cursor = conn.cursor()

# Create table for weather data storage
cursor.execute('''CREATE TABLE IF NOT EXISTS weather_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    city TEXT,
    date TEXT,
    avg_temp REAL,
    max_temp REAL,
    min_temp REAL,
    dominant_condition TEXT
)''')
conn.commit()


def kelvin_to_celsius(kelvin_temp):
    """Convert Kelvin to Celsius"""
    return kelvin_temp - 273.15


def fetch_weather(city_name, city_id):
    """Fetch weather data from OpenWeatherMap API"""
    params = {
        'id': city_id,
        'appid': API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()

    # Parse and convert temperatures
    temp = kelvin_to_celsius(data['main']['temp'])
    feels_like = kelvin_to_celsius(data['main']['feels_like'])
    weather_condition = data['weather'][0]['main']
    timestamp = data['dt']

    print(f"Weather for {city_name}: {temp:.2f}°C, Feels like {feels_like:.2f}°C, Condition: {weather_condition}")
    return {
        'city': city_name,
        'temp': temp,
        'feels_like': feels_like,
        'condition': weather_condition,
        'timestamp': timestamp
    }


def store_daily_summary(city, date, avg_temp, max_temp, min_temp, dominant_condition):
    """Store daily weather summary in SQLite DB"""
    cursor.execute(
        '''INSERT INTO weather_summary (city, date, avg_temp, max_temp, min_temp, dominant_condition)
           VALUES (?, ?, ?, ?, ?, ?)''',
        (city, date, avg_temp, max_temp, min_temp, dominant_condition)
    )
    conn.commit()


def rollup_and_aggregate(weather_data, date):
    """Calculate daily rollups and aggregates"""
    temps = [entry['temp'] for entry in weather_data]
    avg_temp = sum(temps) / len(temps)
    max_temp = max(temps)
    min_temp = min(temps)

    # Dominant weather condition (condition that occurs most frequently)
    conditions = [entry['condition'] for entry in weather_data]
    dominant_condition = max(set(conditions), key=conditions.count)

    # Store the summary in the database
    store_daily_summary(
        city=weather_data[0]['city'],
        date=date,
        avg_temp=avg_temp,
        max_temp=max_temp,
        min_temp=min_temp,
        dominant_condition=dominant_condition
    )


def check_thresholds(weather_data):
    """Check if any threshold is breached and trigger alerts"""
    if weather_data['temp'] > ALERT_THRESHOLD:
        print(f"Alert: {weather_data['city']} temperature exceeded {ALERT_THRESHOLD}°C: {weather_data['temp']:.2f}°C")


def fetch_and_process_weather():
    """Fetch and process weather data for each city"""
    current_date = datetime.now().strftime('%Y-%m-%d')
    weather_data = []

    for city_name, city_id in CITIES.items():
        data = fetch_weather(city_name, city_id)
        weather_data.append(data)

        # Check for alert thresholds
        check_thresholds(data)

    # Aggregate the daily data for rollups and summaries
    rollup_and_aggregate(weather_data, current_date)


def run_scheduler():
    """Scheduler to fetch weather data every 5 minutes"""
    scheduler = BackgroundScheduler()
    scheduler.add_job(fetch_and_process_weather, 'interval', minutes=5)
    scheduler.start()

    print("Weather monitoring system started. Fetching weather data every 5 minutes.")
    try:
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


if __name__ == "__main__":
    run_scheduler()
