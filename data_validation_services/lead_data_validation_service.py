import asyncio
from gmqtt import Client as mqtt
import uuid
import ssl
from functools import partial
import json
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
import logging
import colorlog
import socket

# --- LOGGING CONFIGURATION ---
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s%(levelname)-8s%(reset)s %(purple)s%(message)s',
    log_colors={
        'DEBUG':    'cyan',
        'INFO':     'green',
        'WARNING':  'yellow',
        'ERROR':    'red',
        'CRITICAL': 'red,bg_white',
    }
))
logger = colorlog.getLogger('validation')
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# --- CONFIGURATION ---
current_status = "IDLE"
current_condition = "NORMAL"

# Kafka Cluster Settings
BOOTSTRAP_SERVERS = 'kafka-1:9092,kafka-2:9092'
INPUT_TOPIC = 'test-topic'       # Where Bridge pushes data
OUTPUT_TOPIC = 'validated-data' # Where valid data goes

# Persistant ID based on Docker Hostname
service_id = socket.gethostname()

# Management Topics
manager_subscribe_topic = {"from_manager": "00989800/to_validation_calls"}
manager_publish_topic = {"to_manager": "00989800/from_validation_calls"}

# Overload/Rate Parameters
data_rate_limit = 12             # Max messages per window
time_for_data_rate_limit = 5      # Window size in seconds
counter = 0
data_flow_start_time = 0
last_overload_time = 0
over_load_cooldown_time = 30     # Don't spam Manager with overload alerts

# --- RATE LIMITER ---

def data_rate_limit_check():
    global counter, data_flow_start_time, data_rate_limit, time_for_data_rate_limit
    
    current_time = asyncio.get_event_loop().time()
    
    if data_flow_start_time == 0:
        data_flow_start_time = current_time
        counter = 1
        return False

    time_passed = current_time - data_flow_start_time

    # If window expired, reset
    if time_passed >= time_for_data_rate_limit:
        data_flow_start_time = current_time
        counter = 1
        return False

    counter += 1
    
    # Check threshold
    if counter > data_rate_limit:
        logger.warning(f"⚠ VALIDATION OVERLOAD DETECTED: {counter} msgs in {time_passed:.2f}s")
        return True
    
    return False

# --- MANAGER CALLBACKS & FUNCTIONS ---

def send_status_response(client, is_status=True):
    """Inform Manager of current state."""
    data = {
        "service_id": service_id,
        "status": current_status,
        "condition": current_condition,
        "is_status": is_status
    }
    client.publish(manager_publish_topic['to_manager'], json.dumps(data))
    logger.info(f"Reported to Manager: Status={current_status}, Condition={current_condition}")

async def respond_to_manager(client, topic, payload, qos, properties):
    global current_status
    
    try:
        data = json.loads(payload.decode('utf-8'))
        # Check if message is for me or for all validation services
        if data.get('service_id') == service_id or data.get('service_id') == "ALL":
            msg = data.get('msg')
            logger.info(f"Command from Manager: {msg}")

            if msg == "START" and current_status == "IDLE":
                current_status = "RUNNING"
                send_status_response(client)
                logger.info(">>> Service switched to RUNNING state")

            elif msg == "IDLE" and current_status == "RUNNING":
                current_status = "IDLE"
                send_status_response(client)
                logger.info(">>> Service switched to IDLE state")

            elif msg == "STATUS":
                send_status_response(client)
    except Exception as e:
        logger.error(f"Error processing Manager message: {e}")

# --- KAFKA WORKER ---

async def validation_worker(mqtt_client):
    global current_status, current_condition, last_overload_time
    
    # 1. Setup Producer (for output)
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks=0,
        max_batch_size=65536,
        linger_ms=10
    )

    # 2. Setup Consumer (for input from Bridge)
    # group_id ensures load balancing across multiple validation instances
    consumer = AIOKafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="validation-service-group",
        auto_offset_reset='latest'
    )

    # Start Kafka clients
    connected = False
    while not connected:
        try:
            await producer.start()
            await consumer.start()
            connected = True
            logger.info("Successfully connected to Kafka cluster.")
        except KafkaConnectionError:
            logger.warning("Kafka cluster not ready. Retrying in 5s...")
            await asyncio.sleep(5)

    

    try:
        async for msg in consumer:
            # Only process if Manager has set status to RUNNING
            if current_status == "RUNNING":
                try:
                    raw_payload = json.loads(msg.value.decode('utf-8'))
                    
                    # 1. Rate Limit Monitoring
                    overload = data_rate_limit_check()
                    current_time = asyncio.get_event_loop().time()

                    # 2. Overload Logic
                    if overload:
                        if current_condition != "OVERLOAD" or (current_time - last_overload_time) > over_load_cooldown_time:
                            last_overload_time = current_time
                            current_condition = "OVERLOAD"
                            send_status_response(mqtt_client, is_status=False) # is_status=False means condition alert
                    else:
                        # Stability Window: Return to Normal if quiet for 10s
                        if current_condition == "OVERLOAD" and (current_time - last_overload_time) > 10.0:
                            current_condition = "NORMAL"
                            send_status_response(mqtt_client)
                            logger.info("--- Condition returned to NORMAL. ---")

                    # 3. Data Validation Logic
                    # Example: Simple check if the payload is a dictionary and contains data
                    if isinstance(raw_payload, dict):
                        # Append metadata
                        raw_payload['validated_by'] = service_id
                        raw_payload['validation_timestamp'] = current_time
                        
                        # Forward to the next Kafka Topic
                        await producer.send_and_wait(OUTPUT_TOPIC, raw_payload)
                        logger.debug("Data validated and forwarded.")
                    else:
                        logger.warning("Received non-dict payload. Discarding.")

                except json.JSONDecodeError:
                    logger.error("Failed to decode Kafka message payload.")
            else:
                # If IDLE, yield control to prevent CPU spin
                await asyncio.sleep(0.5)

    except Exception as e:
        logger.error(f"Fatal Worker Error: {e}")
    finally:
        await producer.stop()
        await consumer.stop()

# --- MAIN ---

def on_connect(client, flags, rc, properties):
    for _, topic in manager_subscribe_topic.items():
        client.subscribe(topic)
    logger.info(f"Connected to MQTT. Subscribed to validation commands.")
    send_status_response(client)

async def main():
    host = "31d09ce8b7fa4a92aafc62ae06187541.s1.eu.hivemq.cloud"
    port = 8883
    ssl_ctx = ssl.create_default_context()

    # MQTT Client Setup
    mqtt_client = mqtt(service_id)
    mqtt_client.set_auth_credentials("Snappp", "Snap00989800")
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = respond_to_manager
    
    await mqtt_client.connect(host, port, ssl=ssl_ctx, keepalive=300)

    # Start the Kafka Validation Loop
    await validation_worker(mqtt_client)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Validation service manually stopped.")