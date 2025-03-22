from kafka import KafkaProducer
import json

# Configure the producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Message to send
message = {
    "query": "Hello, this is a query message!",
    "timestamp": "2023-01-01 10:00:00"
}

# Send message to 'query' topic
future = producer.send('query', value=message)

try:
    # Wait for message to be delivered
    record_metadata = future.get(timeout=10)
    print(f"Message sent successfully!")
    print(f"Topic: {record_metadata.topic}")
    print(f"Partition: {record_metadata.partition}")
    print(f"Offset: {record_metadata.offset}")
except Exception as e:
    print(f"Error sending message: {str(e)}")

# Clean up
producer.flush()
producer.close()