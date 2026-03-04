import asyncio
from gmqtt import Client as mqtt
import uuid
import ssl
from functools import partial
import json
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError, RequestTimedOutError, NodeNotReadyError


import os
import dotenv
current_status="IDLE"
current_condition="NORMAL"

# Internal Docker names
BOOTSTRAP_SERVERS = 'kafka-1:9092,kafka-2:9092'
TOPIC_NAME = 'test-topic'

#service_id for taling to manger and service_id2 for tallking with data client
service_id="46b01876-f798-46b9-904e-c33623f0b593"# for now setting it to const
service_id2=str(uuid.uuid4())
print("For Manager",service_id)
print("For Worker",service_id2)

producer=None

manager_subscribe_topic={
                "from_manager":"00989800/to_bridge_calls" 
                }

manager_publish_topic={
                "to_manager":"00989800/from_bridge_calls"
            }

data_subscribe_topic={
    "data_topic":"$share/iot-data-pipeline-v2/00989800/#"
}
# _______ manager callbacks ______________

def on_connect(client,flags, rc, properties):# subscribe to manager
    global manager_subscribe_topic
    for i,j in manager_subscribe_topic.items():
        client.subscribe(j)
        print(f"subscribed to {i}:{j}")
        print()

def on_subscribe(client, mid, qos, properties):#conifrm subscription
    print("subscribed to manager topic!!!")
    print()

def on_disconnet():# diconnected INFO
    print("Diconnected from Broker!!!")
    print()
# ________________________________

#________________data client callbacks_________
# needs partial
async def subscribe_to_data_topic(client,flags, rc, properties,data_topic=None):
    if data_topic is None:
        data_topic=data_subscribe_topic['data_topic']
    client.subscribe(data_topic)
    print(f"Subscribed to topic {data_topic}")


def on_disconnet_data_client():
    print("Disconnected ffrom data client")

def on_subscribe_data_client(client, mid, qos, properties):
    print("Subscribed to data client topic")

# ________________________________

# manager functions ______________
async def send_status_response(client):
    global service_id,current_status,manager_publish_topic
    data={"service_id":service_id,"status":current_status,"condition":current_condition,"is_status":True}
    print(f"Called manager with msg :{data}")
    call_manager(client,msg=data)

async def respond_to_manager(client, topic, payload, qos, properties):
    global current_status
    msg_from_manager=payload.decode('utf-8')
    data=json.loads(msg_from_manager)
    if data.get('service_id')==service_id:
        print("Got msg from manager!!!")
        msg=data.get('msg')
        print(msg,current_status)
        if msg=="START" and current_status=="IDLE":
            await start_service(client)
            print("Started Service called")
        elif msg=="IDLE" and current_status=="RUNNING":
            await go_idle(client)
            print(" Service go idle called")
        elif msg=="STATUS":
            await send_status_response(client)
        
def call_manager(client,msg):
    msg=json.dumps(msg)
    client.publish(manager_publish_topic['to_manager'],msg)

def service_under_load_call(client):
    global current_condition,service_id,current_status
    data={"service_id":service_id,"status":current_status,"condition":current_condition,"is_status":False}
    print("called manager to inform service under laod")
    call_manager(client,msg=data)
 
#_________________________________


# data client functions _________
async def worker(data_queue):
    while True:
        data=await data_queue.get()
        await push_data(data)
        print("Pushing data in Kafka ")
        data_queue.task_done()

async def disconnect_kafka():
    global producer
    print("Initiating graceful shutdown...")
    await producer.stop() 
    print("Producer stopped. All buffers flushed.")

async def go_idle(client,data_topic=None):
    if data_topic is None:
        data_topic=data_subscribe_topic['data_topic']

    await client.unsubscribe(data_topic)
    disconnect_kafka()
    print(f"Going Idle Just Unsubrcibed to the data topic :{data_topic}")
    print()
    print("Disconnecting from kafka")

async def start_service(client,data_topic=None):
    global current_status

    if data_topic is None:
        data_topic=data_subscribe_topic['data_topic']
    try:
        
        client.subscribe(data_topic)
        await start_producer()
        current_status="RUNNING"
    except Exception as e:
        print("Exception happended in start service ")
        print()
        print(e) 
    print(f"Subscribed to data Topic:{data_topic}")
    print("Connected to Kafka too ")

def check_if_service_load():
    pass

async def get_data(client, topic, payload, qos, properties,data_queue):#inputs str in queue
    data=payload.decode('utf-8')
    data=json.loads(data)
    await data_queue.put(data)
    print("Data Inserted InQueue")

async def start_producer():
    global producer
    
    # Check if a producer already exists and is running
    if producer is not None:
        print("Producer already exists. Skipping initialization.")
        return
    
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
            print(f"Attempting to connect to KRaft cluster at {BOOTSTRAP_SERVERS}...")
            await producer.start()
            connected = True
            print("Successfully connected to Kafka!")
        except KafkaConnectionError:
            print("Broker not available yet. Retrying in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"Unexpected error: {e}")
            await asyncio.sleep(5)

async def push_data(msg):
    global producer 
    if producer is not None:

        try:
            await producer.send_and_wait(TOPIC_NAME, msg)
        except KafkaConnectionError:
            print("Connection lost! The broker is unreachable.")
            await start_producer()
        except RequestTimedOutError:
            print("The leader died and no new leader was elected in time.")
    else:
        await start_producer()
        print("producer is None")
        print("Starting Producer")
#_____________________________




async def main():



    #Global things
    host="31d09ce8b7fa4a92aafc62ae06187541.s1.eu.hivemq.cloud"
    port=8883
    username="Snappp"
    password="Snap00989800"
    ssl_ctx = ssl.create_default_context()
    # manager Things
    client=mqtt(service_id)
    client.set_auth_credentials(username=username, password=password)
    client.on_connect= on_connect
    client.on_message = respond_to_manager
    client.on_disconnect = on_disconnet
    client.on_subscribe= on_subscribe
    await client.connect(host=host, port=port, ssl=ssl_ctx)



    # data client Things 
    data_queue=asyncio.Queue()

   
    data_client=mqtt(service_id2)

    data_client.set_auth_credentials(username, password)
    data_client.on_connect = subscribe_to_data_topic
    data_client.on_disconnect = on_disconnet_data_client
    data_client.on_subscribe= on_subscribe_data_client
    data_client.on_message= partial(get_data,data_queue=data_queue)


    await data_client.connect(host, port, ssl=ssl_ctx)


    asyncio.create_task(worker(data_queue))
    await asyncio.Event().wait()

if __name__=='__main__':
    asyncio.run(main())