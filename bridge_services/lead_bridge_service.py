import asyncio
from gmqtt import Client as mqtt
import uuid
import ssl
from functools import partial
import json
from aiokafka import AIOKafkaProducer
from aiokafka.errors import (KafkaConnectionError, RequestTimedOutError, NodeNotReadyError, ProducerClosed)
import logging
import colorlog
import socket

# --- LOGGING CONFIGURATION ---
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s%(levelname)-8s%(reset)s %(blue)s%(message)s',
    log_colors={
        'DEBUG':    'cyan',
        'INFO':     'green',
        'WARNING':  'yellow',
        'ERROR':    'red',
        'CRITICAL': 'red,bg_white',
    }
))
logger = colorlog.getLogger('bridge')
logger.addHandler(handler)
logger.setLevel(logging.INFO)
# -----------------------------

current_status="IDLE"
current_condition="NORMAL"

# Internal Docker names
BOOTSTRAP_SERVERS = 'kafka-1:9092,kafka-2:9092'
TOPIC_NAME = 'test-topic'

#service_id for taling to manger and service_id2 for tallking with data client
service_id = socket.gethostname()

logger.info(f"My Persistent Unique ID is: {service_id}")#"46b01876-f798-46b9-904e-c33623f0b593"# for now setting it to const
service_id2=str(uuid.uuid4())
logger.info(f"For Manager: {service_id}")
logger.info(f"For Worker: {service_id2}")

producer=None
measure_time=False
data_flow_start_time=0
data_rate_limit=10
time_for_data_rate_limit=5
counter=1
over_load_cooldown_time=30
last_overload_time = 0
manager_subscribe_topic={
                "from_manager":"00989800/to_bridge_calls" 
                }

manager_publish_topic={
                "to_manager":"00989800/from_bridge_calls"
            }

data_subscribe_topic={
    "data_topic":"$share/iot-data-pipeline-v2/00989800/#"
}

ID_FILE_PATH = "/app/data/container_id.txt"

# _______ manager callbacks ______________

def on_connect(client,flags, rc, properties):# subscribe to manager
    global manager_subscribe_topic
    for i,j in manager_subscribe_topic.items():
        client.subscribe(j)
        logger.info(f"subscribed to {i}:{j}")
    send_status_response(client=client)
    logger.info("Sent status on start")

def on_subscribe(client, mid, qos, properties):#conifrm subscription
    logger.info("subscribed to manager topic!!!")

def on_disconnet(client, packet, exc=None):# diconnected INFO
    logger.warning("Disconnected from Broker!!!")
# ________________________________

#________________data client callbacks_________

# Assign the wrapper, not the async function directly

async def subscribe_to_data_topic(client,flags, rc, properties):
    global data_subscribe_topic
    
    data_topic=data_subscribe_topic['data_topic']
    client.subscribe(data_topic)
    logger.info(f"Subscribed to topic {data_topic}")

def start_sub_task(client, flags, rc, properties):
    # This 'schedules' the async work so it actually runs
    logger.info("Connected to the  data Client!! not subscribed")
    #asyncio.create_task(subscribe_to_data_topic(client, flags, rc, properties))

def on_disconnet_data_client(client, packet, exc=None):
    logger.warning("Disconnected from data client")

def on_subscribe_data_client(client, mid, qos, properties):
    logger.info("Subscribed to data client topic")

# ________________________________

# manager functions ______________
def send_status_response(client):
    global service_id,current_status,manager_publish_topic
    data={"service_id":service_id,"status":current_status,"condition":current_condition,"is_status":True}
    logger.info(f"Called manager with msg :{data}")
    call_manager(client,msg=data)

async def respond_to_manager(client, topic, payload, qos, properties,data_client):
    global current_status

    msg_from_manager=payload.decode('utf-8')
    data=json.loads(msg_from_manager)
    if data.get('service_id')==service_id or data.get('service_id')=="ALL":
        logger.info("Got msg from manager!!!")
        msg=data.get('msg')
        logger.info(f"Msg from manager is {msg}")
        logger.info(f"Current Status is {current_status}")
        if msg=="START" and current_status=="IDLE":
            await start_service(data_client)
            send_status_response(client)
            logger.info("Updated manager service started")
            logger.info("Started Service called")
        elif msg=="IDLE" and current_status=="RUNNING":
            await go_idle(data_client)
            logger.info(" Service go idle called")
        elif msg=="STATUS":
            send_status_response(client)
        
def call_manager(client,msg):
    msg=json.dumps(msg)
    if not client.is_connected:
        logger.error("NOT CONNECTED TO BROKER")
        return 
    client.publish(manager_publish_topic['to_manager'],msg)

def service_under_load_call(client):
    global current_condition,service_id,current_status
    data={"service_id":service_id,"status":current_status,"condition":current_condition,"is_status":False}
    logger.warning("called manager to inform service under load")
    call_manager(client,msg=data)
 
#_________________________________

# data client functions _________
async def worker(client,data_queue):
    global current_condition
    global last_overload_time
    global over_load_cooldown_time
    try:
        while True:
            data=await data_queue.get()
            
            overload=data_rate_limit_check()
            current_time=asyncio.get_event_loop().time()
            
            await push_data(data)
            logger.info("Pushing data in Kafka")
            data_queue.task_done()
            if overload :
                if current_condition!="OVERLOAD" or (current_time - last_overload_time) > over_load_cooldown_time:
                    last_overload_time=current_time
                    current_condition="OVERLOAD"
                    service_under_load_call(client=client)
            else:
                if current_condition == "OVERLOAD":
                    # Wait for a "Stability Window" (e.g., 10 seconds of Normal traffic)
                    if (current_time - last_overload_time) > 10.0:
                        current_condition = "NORMAL"
                        send_status_response(client) # Inform Manager: I am okay now!
                        logger.info("--- Condition returned to NORMAL. Informing Manager. ---")
    except ProducerClosed:
        logger.warning("Producer was closed, worker stopping push.")
        
    except Exception as e:
            logger.error(f"Worker Error: {e}")

async def disconnect_kafka():
    global producer
    logger.info("Initiating graceful shutdown...")
    await producer.stop() 
    logger.info("Producer stopped. All buffers flushed.")

async def go_idle(client,data_topic=None):
    global current_status
    if current_status=="IDLE":
        logger.info("Already IDLE")
        return
    if data_topic is None:
        data_topic=data_subscribe_topic['data_topic']

    client.unsubscribe(data_topic)
    await disconnect_kafka()
    current_status="IDLE"
    logger.info(f"Going Idle Just Unsubscribed to the data topic :{data_topic}")
    logger.info("Disconnecting from kafka")

async def start_service(client,data_topic=None):
    global current_status

    if data_topic is None:
        data_topic=data_subscribe_topic['data_topic']
    try:
        logger.info("Subscribing to data topic...")
        client.subscribe(data_topic)
        logger.info("Starting kafka")
        await start_producer()
        current_status="RUNNING"
        
    except Exception as e:
        logger.error(f"Exception happened in start service: {e}") 
    logger.info(f"Subscribed to data Topic:{data_topic}")
    logger.info("Connected to Kafka too ")

def data_rate_limit_check():
    global counter
    global current_status
    global data_rate_limit
    global data_flow_start_time
    global time_for_data_rate_limit

    loop = asyncio.get_running_loop()
    current_time = loop.time()

    # First call initialization
    if data_flow_start_time == 0:
        data_flow_start_time = current_time
        counter = 1
        logger.debug("[INIT] Rate limiter started")
        return False

    time_passed = current_time - data_flow_start_time

    # Window expired → reset
    if time_passed >= time_for_data_rate_limit:
        final_rate = counter / max(time_passed, 0.0001)
        logger.info(f"[WINDOW RESET] Final rate: {final_rate:.2f} msg/sec")

        data_flow_start_time = current_time
        counter = 1
        return False

    # Still inside window
    counter += 1

    # 🔥 Only calculate rate after 1 second minimum
    if time_passed >= 1:
        current_rate = counter / time_passed
        logger.info(f"[RATE] {current_rate:.2f} msg/sec | Count: {counter} | Time: {time_passed:.2f}s")

    if counter > data_rate_limit:
        logger.warning("⚠ OVERLOAD DETECTED")
        return True
    
    return False

async def get_data(client, topic, payload, qos, properties,data_queue):#inputs str in queue
    global manager_subscribe_topic,data_flow_start_time,measure_time
    if topic!=manager_subscribe_topic['from_manager']:
        data=payload.decode('utf-8')
        data=json.loads(data)
        await data_queue.put(data)
        logger.debug("Data Inserted InQueue")

async def start_producer():
    global producer
    
    # Check if a producer already exists and is running
    if producer is not None:
        logger.info("Producer already exists. Skipping initialization.")
        return
    logger.info("Creating producer object...")
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # 'all' ensures data is replicated to both brokers before confirming
        acks='all',
        # Retry settings for internal library resilience
        retry_backoff_ms=500 
    )

    # --- CONNECTION LOGIC ---
    connected = False
    while not connected:
        try:
            logger.info(f"Attempting to connect to KRaft cluster at {BOOTSTRAP_SERVERS}...")
            await producer.start()
            connected = True
            logger.info("Successfully connected to Kafka!")
        except KafkaConnectionError:
            logger.warning("Broker not available yet. Retrying in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            await asyncio.sleep(5)

async def push_data(msg):
    global producer 
    if producer is not None:

        try:
            await producer.send_and_wait(TOPIC_NAME, msg)
        except KafkaConnectionError:
            logger.error("Connection lost! The broker is unreachable.")
            await start_producer()
        except RequestTimedOutError:
            logger.error("The leader died and no new leader was elected in time.")
    else:
        logger.warning("Producer is None. Starting Producer...")
        await start_producer()
#_____________________________

async def main():

    #Global things
    host="31d09ce8b7fa4a92aafc62ae06187541.s1.eu.hivemq.cloud"
    port=8883
    username="Snappp"
    password="Snap00989800"
    ssl_ctx = ssl.create_default_context()

    # both clients
    client=mqtt(service_id)
    data_client=mqtt(service_id2)

    #
    client.set_auth_credentials(username=username, password=password)
    client.on_connect= on_connect
    client.on_message = partial(respond_to_manager,data_client=data_client)
    client.on_disconnect = on_disconnet
    client.on_subscribe= on_subscribe
    
    await client.connect(host=host, port=port, ssl=ssl_ctx, keepalive=300)

        # data client Things 
    data_queue=asyncio.Queue()

    data_client.set_auth_credentials(username, password)
    data_client.on_connect = start_sub_task
    data_client.on_disconnect = on_disconnet_data_client
    data_client.on_subscribe= on_subscribe_data_client
    data_client.on_message= partial(get_data,data_queue=data_queue)

    await data_client.connect(host, port, ssl=ssl_ctx, keepalive=300)

    asyncio.create_task(worker(client=client,data_queue=data_queue))
    await asyncio.Event().wait()

if __name__=='__main__':
    asyncio.run(main())