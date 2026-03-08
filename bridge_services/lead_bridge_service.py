import asyncio
from gmqtt import Client as mqtt
import uuid
import ssl
from functools import partial
import json
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError, RequestTimedOutError, ProducerClosed
import logging
import colorlog
import socket

# --- LOGGING CONFIGURATION ---
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s%(levelname)-8s%(reset)s %(blue)s%(message)s',
    log_colors={
        'DEBUG': 'cyan', 'INFO': 'green', 'WARNING': 'yellow',
        'ERROR': 'red', 'CRITICAL': 'red,bg_white',
    }
))
logger = colorlog.getLogger('bridge')
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# --- STATE TRACKING ---
current_status = "IDLE"
current_condition = "NORMAL"
service_id = socket.gethostname()
service_id_data = str(uuid.uuid4()) # Unique ID for the data channel

# Kafka Config
BOOTSTRAP_SERVERS = 'kafka-1:9092,kafka-2:9092'
TOPIC_NAME = 'test-topic'
producer = None

# Scaling & Rate Limiting
data_rate_limit = 10
time_for_data_rate_limit = 5
counter = 0
total_pushed = 0
current_msg_rate = 0.0
data_flow_start_time = 0
last_overload_time = 0

# MQTT Topics
manager_sub_topic = "00989800/to_bridge_calls"
manager_pub_topic = "00989800/from_bridge_calls"
data_sub_topic = "$share/iot-data-pipeline-v2/00989800/#"

# --- MANAGER FUNCTIONS ---

def send_status_response(client):
    """Reports state and metrics to the Manager."""
    global current_status, current_condition, current_msg_rate
    data = {
        "service_id": service_id,
        "status": current_status,
        "condition": current_condition,
        "rate": round(current_msg_rate, 2),
        "is_status": True
    }
    client.publish(manager_pub_topic, json.dumps(data))
    logger.info(f"💓 Heartbeat Sent: {current_status} | {current_condition} | {current_msg_rate} msg/s")

async def respond_to_manager(client, topic, payload, qos, properties, data_client):
    global current_status
    try:
        data = json.loads(payload.decode('utf-8'))
        target = data.get('service_id')
        
        if target == service_id or target == "ALL":
            msg = data.get('msg')
            logger.info(f"📩 Manager Command: {msg}")
            
            if msg == "START" and current_status == "IDLE":
                await start_service(data_client)
                send_status_response(client)
            elif msg == "IDLE" and current_status == "RUNNING":
                await go_idle(data_client)
                send_status_response(client)
            elif msg == "STATUS":
                send_status_response(client)
    except Exception as e:
        logger.error(f"Manager parsing error: {e}")

# --- KAFKA PRODUCER LOGIC ---

async def start_producer():
    global producer
    if producer: return
    
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks=0, linger_ms=10
    )
    
    connected = False
    while not connected:
        try:
            await producer.start()
            connected = True
            logger.info("✅ Kafka Connection Established")
        except KafkaConnectionError:
            logger.warning("⏳ Kafka brokers not ready. Retrying in 5s...")
            await asyncio.sleep(5)

async def push_data(msg):
    global total_pushed
    if producer:
        try:
            await producer.send_and_wait(TOPIC_NAME, msg)
            total_pushed += 1
        except Exception as e:
            logger.error(f"Kafka Push Failed: {e}")

# --- SERVICE STATE CONTROL ---

async def start_service(data_client):
    global current_status
    logger.info("🚀 Booting Service: Subscribing to Data & Starting Kafka...")
    data_client.subscribe(data_sub_topic)
    await start_producer()
    current_status = "RUNNING"

async def go_idle(data_client):
    global current_status, producer
    logger.info("📉 Scaling Down: Unsubscribing & Closing Kafka...")
    data_client.unsubscribe(data_sub_topic)
    if producer:
        await producer.stop()
        producer = None
    current_status = "IDLE"

# --- RATE LIMITING ---

def perform_rate_check():
    global counter, data_flow_start_time, current_msg_rate, current_condition, last_overload_time
    now = asyncio.get_event_loop().time()
    
    if data_flow_start_time == 0:
        data_flow_start_time = now
        return

    time_passed = now - data_flow_start_time
    counter += 1
    
    if time_passed >= 1.0:
        current_msg_rate = round(counter / time_passed, 2)
        
    # Overload Logic
    if counter > data_rate_limit:
        if current_condition != "OVERLOAD":
            logger.warning(f"⚠️ OVERLOAD: {current_msg_rate} msg/s exceeds limit!")
            current_condition = "OVERLOAD"
            last_overload_time = now
            
    # Window Reset
    if time_passed >= time_for_data_rate_limit:
        # Check for recovery (10s stability window)
        if current_condition == "OVERLOAD" and (now - last_overload_time) > 10:
            current_condition = "NORMAL"
            logger.info("✅ Cluster condition stabilized to NORMAL")
            
        data_flow_start_time = now
        counter = 0

# --- CORE WORKER ---

async def worker(data_queue):
    logger.info("Worker Task Initialized")
    while True:
        data = await data_queue.get()
        if current_status == "RUNNING":
            perform_rate_check()
            await push_data(data)
            if total_pushed % 100 == 0:
                logger.info(f"📊 Total processed: {total_pushed} units")
        data_queue.task_done()

async def on_data_received(client, topic, payload, qos, properties, data_queue):
    """Filters data from manager calls and puts valid readings in queue."""
    if topic == manager_sub_topic:
        return # Ignore manager calls reaching data client
    
    try:
        data = json.loads(payload.decode('utf-8'))
        await data_queue.put(data)
    except:
        pass

# --- MAIN ---

async def main():
    logger.info(f"Starting Bridge ID: {service_id}")
    
    host, port = "31d09ce8b7fa4a92aafc62ae06187541.s1.eu.hivemq.cloud", 8883
    ssl_ctx = ssl.create_default_context()
    data_queue = asyncio.Queue()

    # Manager Control Client
    client = mqtt(service_id)
    client.set_auth_credentials("Snappp", "Snap00989800")
    client.on_connect = lambda c, f, r, p: (c.subscribe(manager_sub_topic), send_status_response(c))
    client.on_message = partial(respond_to_manager, data_client=None) # placeholder
    await client.connect(host, port, ssl=ssl_ctx)

    # Data Pipeline Client
    data_client = mqtt(service_id_data)
    data_client.set_auth_credentials("Snappp", "Snap00989800")
    data_client.on_message = partial(on_data_received, data_queue=data_queue)
    await data_client.connect(host, port, ssl=ssl_ctx)

    # Link clients for control
    client.on_message = partial(respond_to_manager, data_client=data_client)

    # Background Tasks
    asyncio.create_task(worker(data_queue))
    
    await asyncio.Event().wait()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Service Offline.")