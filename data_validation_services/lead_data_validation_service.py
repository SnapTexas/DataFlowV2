import asyncio
import json
import ssl
import os
import socket
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
import logging
import colorlog

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

# --- THROUGHPUT & RATE TRACKING ---
data_rate_limit = 12
time_for_data_rate_limit = 5
counter = 0
total_published = 0
data_flow_start_time = 0
last_overload_time = 0
current_msg_rate = 0.0

def data_rate_limit_check():
    """Calculates msg/sec and checks for overload."""
    global counter, data_flow_start_time, current_msg_rate
    current_time = asyncio.get_event_loop().time()
    
    if data_flow_start_time == 0:
        data_flow_start_time = current_time
        counter = 1
        return False
    
    time_passed = current_time - data_flow_start_time
    
    # Update rate every second for internal tracking
    if time_passed >= 1.0:
        current_msg_rate = counter / time_passed
    
    # Window expired → Reset statistics for the next 5s block
    if time_passed >= time_for_data_rate_limit:
        logger.info(f"📊 [WINDOW RESET] Avg Rate: {current_msg_rate:.2f} msg/sec | Total: {total_published}")
        data_flow_start_time = current_time
        counter = 1
        return False
    
    counter += 1
    return counter > data_rate_limit

# --- MANAGER FUNCTIONS ---
def send_status_response(client):
    """Reports state and current throughput to the Manager."""
    global service_id, current_status, current_condition, current_msg_rate
    data = {
        "service_id": service_id,
        "status": current_status,
        "condition": current_condition,
        "rate": round(current_msg_rate, 2), # Now reporting actual speed
        "is_status": True
    }
    client.publish(manager_publish_topic['to_manager'], json.dumps(data))
    logger.info(f"💓 Heartbeat: {current_status} | {current_condition} | {current_msg_rate:.1f} msg/s")

async def respond_to_manager(client, topic, payload, qos, properties):
    global current_status
    try:
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
                send_status_response(client)
    except Exception as e:
        logger.error(f"Manager Msg Error: {e}")

# --- KAFKA WORKER ---
async def validation_worker(mqtt_client):
    global current_status, current_condition, last_overload_time, total_published
    
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    consumer = AIOKafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="validation-service-group",
        auto_offset_reset='earliest'
    )

    # Robust Kafka Connection Loop
    kafka_connected = False
    while not kafka_connected:
        try:
            await producer.start()
            await consumer.start()
            kafka_connected = True
            logger.info("✅ Kafka Clients Started Successfully.")
        except KafkaConnectionError:
            logger.warning("⏳ Kafka not ready. Retrying in 5s...")
            await asyncio.sleep(5)

    try:
        async for msg in consumer:
            if current_status == "RUNNING":
                try:
                    raw_payload = json.loads(msg.value.decode('utf-8'))
                    
                    # 1. Update State & Throughput
                    overload = data_rate_limit_check()
                    current_time = asyncio.get_event_loop().time()

                    if overload:
                        if current_condition != "OVERLOAD":
                            current_condition = "OVERLOAD"
                            last_overload_time = current_time
                            logger.warning(f"⚠️ OVERLOAD! Current rate: {current_msg_rate:.1f} msg/s")
                    else:
                        if current_condition == "OVERLOAD" and (current_time - last_overload_time) > 10.0:
                            current_condition = "NORMAL"
                            logger.info("✅ Recovered: State NORMAL")

                    # 2. Logic: Process and Forward
                    if isinstance(raw_payload, dict):
                        raw_payload['validated_by'] = service_id
                        await producer.send_and_wait(OUTPUT_TOPIC, raw_payload)
                        total_published += 1
                        
                        if total_published % 50 == 0:
                            logger.info(f"📤 Published {total_published} validated messages total.")

                except Exception as e:
                    logger.error(f"Processing Error: {e}")
            else:
                # If IDLE, just sleep a bit to not peg the CPU
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

    from gmqtt import Client as MQTTClient
    mqtt_c = MQTTClient(service_id)
    mqtt_c.set_auth_credentials("Snappp", "Snap00989800")
    mqtt_c.on_connect = on_connect
    mqtt_c.on_message = respond_to_manager
    
    try:
        await mqtt_c.connect(host, port, ssl=ssl_ctx)
        await validation_worker(mqtt_c)
    except Exception as e:
        logger.error(f"Fatal Error: {e}")

if __name__ == '__main__':
    asyncio.run(main())