from confluent_kafka import Producer
import csv
import json

bootstrap_servers = 'localhost:9092'
topic = 'proje'

# Create a Kafka producer
producer = Producer({'bootstrap.servers': bootstrap_servers})

# CSV file path
csv_file_path = 'C:/housing.csv'

# Read CSV data and produce to Kafka
with open(csv_file_path, 'r') as file:
    # Use csv.DictReader to read rows as dictionaries
    reader = csv.DictReader(file)
    for row in reader:
        # Produce each row as a message to the Kafka topic
        # Encode the row as a JSON string for simplicity
        message_value = json.dumps(row)
        producer.produce(topic, value=message_value.encode('utf-8'))
        producer.flush()

print("Data from housing.csv uploaded to kafka successfully.")