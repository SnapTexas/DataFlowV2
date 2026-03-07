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
    '%(log_color)s%(levelname)-8s%(reset)s %(purple)s%(message)s'
))
logger = colorlog.getLogger('validation')
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# --- CONFIGURATION ---
current_status = "IDLE"
current_condition = "NORMAL"
BOOTSTRAP_SERVERS = 'kafka-1:9092,kafka-2:9092'
INPUT_TOPIC = 'test-topic'
OUTPUT_TOPIC = 'validated-data'
service_id = socket.gethostname()

manager_subscribe_topic = {"from_manager": "00989800/to_validation_calls"}
manager_publish_topic = {"to_manager": "00989800/from_validation_calls"}

data_rate_limit = 12
time_for_data_rate_limit = 5
counter = 0
data_flow_start_time = 0
last_overload_time = 0

# --- RATE LIMITER ---
def data_rate_limit_check():
    global counter, data_flow_start_time
    current_time = asyncio.get_event_loop().time()
    if data_flow_start_time == 0:
        data_flow_start_time = current_time
        counter = 1
        return False
    if (current_time - data_flow_start_time) >= time_for_data_rate_limit:
        data_flow_start_time = current_time
        counter = 1
        return False
    counter += 1
    return counter > data_rate_limit

# --- MANAGER FUNCTIONS ---
def send_status_response(client):
    """The only way the Manager finds out about state."""
    global service_id, current_status, current_condition
    data = {
        "service_id": service_id,
        "status": current_status,
        "condition": current_condition, # Flipped by worker locally
        "is_status": True
    }
    client.publish(manager_publish_topic['to_manager'], json.dumps(data))
    logger.info(f"Heartbeat Sent: {current_status} | {current_condition}")

async def respond_to_manager(client, topic, payload, qos, properties):
    global current_status
    data = json.loads(payload.decode('utf-8'))
    if data.get('service_id') == service_id or data.get('service_id') == "ALL":
        msg = data.get('msg')
        if msg == "START" and current_status == "IDLE":
            current_status = "RUNNING"
            send_status_response(client)
        elif msg == "IDLE" and current_status == "RUNNING":
            current_status = "IDLE"
            send_status_response(client)
        elif msg == "STATUS":
            # REACTIVE: Only report when the Manager pulses
            send_status_response(client)

# --- KAFKA WORKER ---
async def validation_worker(mqtt_client):
    global current_status, current_condition, last_overload_time
    
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    consumer = AIOKafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="validation-service-group"
    )

    await producer.start()
    await consumer.start()
    logger.info("Kafka Clients Started.")

    try:
        async for msg in consumer:
            if current_status == "RUNNING":
                try:
                    raw_payload = json.loads(msg.value.decode('utf-8'))
                    
                    # 1. Update State LOCALLY only
                    overload = data_rate_limit_check()
                    current_time = asyncio.get_event_loop().time()

                    if overload:
                        if current_condition != "OVERLOAD":
                            current_condition = "OVERLOAD"
                            last_overload_time = current_time
                            logger.warning("Local State: OVERLOAD (Silent)")
                    else:
                        # 10s Stability Window
                        if current_condition == "OVERLOAD" and (current_time - last_overload_time) > 10.0:
                            current_condition = "NORMAL"
                            logger.info("Local State: NORMAL")

                    # 2. Logic (Forwarding valid dicts)
                    if isinstance(raw_payload, dict):
                        raw_payload['validated_by'] = service_id
                        await producer.send_and_wait(OUTPUT_TOPIC, raw_payload)

                except Exception as e:
                    logger.error(f"Processing Error: {e}")
            else:
                await asyncio.sleep(0.5)
    finally:
        await producer.stop()
        await consumer.stop()

# --- MAIN ---
def on_connect(client, flags, rc, properties):
    for _, topic in manager_subscribe_topic.items():
        client.subscribe(topic)
    send_status_response(client)

async def main():
    host, port = "31d09ce8b7fa4a92aafc62ae06187541.s1.eu.hivemq.cloud", 8883
    ssl_ctx = ssl.create_default_context()

    mqtt_client = mqtt(service_id)
    mqtt_client.set_auth_credentials("Snappp", "Snap00989800")
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = respond_to_manager
    
    await mqtt_client.connect(host, port, ssl=ssl_ctx)
    await validation_worker(mqtt_client)

if __name__ == '__main__':
    asyncio.run(main())