import time
import json
import random
from kafka import KafkaProducer

# Inside the Docker network, we use the service names from docker-compose
# Use the names from your docker-compose.yml
BOOTSTRAP_SERVERS = ['kafka-1:9092', 'kafka-2:9092']
TOPIC_NAME = 'test-topic'

def main():
    print("Connecting to Kafka cluster...")
    # Retry logic in case Kafka is still finishing its KRaft election
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all' # Ensure both nodes receive the data
            )
        except Exception as e:
            print(f"Waiting for Kafka... {e}")
            time.sleep(5)

    print(f"Connected! Sending data to {TOPIC_NAME}...")

    try:
        while True:
            msg = {
                "timestamp": time.time(),
                "sensor_id": "DHT11_01",
                "reading": random.uniform(20.0, 30.0)
            }
            producer.send(TOPIC_NAME, value=msg)
            print(f"Sent: {msg}")
            time.sleep(2)
    except KeyboardInterrupt:
        print("Closing producer.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()