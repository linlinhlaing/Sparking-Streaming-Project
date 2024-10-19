import os
import requests
import json
from kafka import KafkaProducer
import time
import logging
from dotenv import load_dotenv
 
# Load environment variables from .env file
load_dotenv()
 
# Set up logging
logging.basicConfig(level=logging.INFO)
 
# Function to fetch weather data from OpenWeatherMap API
def fetch_weather_data(api_key, city):
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching weather data: {e}")
        return None
 
# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
 
# OpenWeatherMap API key from environment variable and city
api_key = os.getenv('OPENWEATHERMAP_API_KEY')
if not api_key:
    logging.error("API key not found in environment variables. Please set 'OPENWEATHERMAP_API_KEY' in the .env file.")
    exit(1)
 
city = 'New York'
 
try:
    while True:
        weather_data = fetch_weather_data(api_key, city)
       
        if weather_data:
            # Prepare data to send to Kafka
            message = {
                "temperature": weather_data["main"]["temp"],  # Extract temperature
                "humidity": weather_data["main"]["humidity"],  # Extract humidity
                "city": weather_data["name"],                  # Extract city name
                "description": weather_data["weather"][0]["description"],  # Extract weather description
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")  # Generate current timestamp
            }
 
            # Send the weather data to Kafka
            producer.send('weather', message)
            logging.info(f"Weather data sent to Kafka: {message}")
        else:
            logging.warning("Failed to retrieve weather data.")
       
        time.sleep(60)  # Fetch data every 60 seconds
 
except KeyboardInterrupt:
    logging.info("Stopping the producer...")
 
finally:
    # Ensure the Kafka producer is properly closed
    producer.close()
    logging.info("Kafka producer closed.")
 