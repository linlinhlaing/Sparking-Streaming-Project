from confluent_kafka import Consumer, KafkaError

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Change to your Kafka broker address
    'group.id': 'my_group',                  # Consumer group ID
    'auto.offset.reset': 'earliest'          # Start reading at the earliest message
}

# Create Consumer instance
consumer = Consumer(conf)

# Subscribe to the topic
topic = 'Weather-Data-Topic'  # Replace with your Kafka topic
consumer.subscribe([topic])

# Poll for messages
try:
    while True:
        msg = consumer.poll(1.0)  # Wait for a message or timeout

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                break

        # Print the message value
        print(f"Received message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    pass
finally:
    # Clean up
    consumer.close()
