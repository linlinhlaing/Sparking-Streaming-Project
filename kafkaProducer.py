import pandas as pd
import time
from confluent_kafka import Producer

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Change to your Kafka broker address
    'linger.ms': 5,  # Allow for batching messages
    'acks': 'all',  # Wait for all replicas to acknowledge
    'retries': 5,  # Retry sending messages upon failure
}

# Create Producer instance
producer = Producer(conf)

# Callback function to confirm message delivery
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Function to read CSV and produce messages to Kafka
def produce_csv_to_kafka(csv_file, topic):
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    # Iterate through the DataFrame rows
    for index, row in df.iterrows():
        # Convert row to string or dictionary
        message = row.to_json()  # Convert to JSON string (or use str(row) for string)
        
        # Produce the message
        producer.produce(topic, value=message, callback=delivery_report)
        
        # Introduce a delay to control the production rate
        time.sleep(0.1)  # Adjust the sleep duration as needed
    
    # Wait for any outstanding messages to be delivered and delivery reports
    producer.flush()

# Usage
if __name__ == "__main__":
    csv_file = 'Weather Data (US).csv'  # Replace with your CSV file path
    topic = 'Weather-Data-Topic'  # Replace with your Kafka topic
    produce_csv_to_kafka(csv_file, topic)
