import asyncio
from gmqtt import Client as mqtt
import uuid
import ssl
from functools import partial
import json
import os
import dotenv
current_status="RUNNING"
current_condition="NORMAL"
service_id=str(uuid.uuid4())
print(service_id)
subscribe_topics={
                "bridge_call_topic":"00989800/to_bridge_calls" 
                }

publish_topics={
                "bridge_call_recv_topic":"00989800/from_bridge_calls"
            }
# _______ callbacks ______________

def on_connect(client,flags, rc, properties):
    global subscribe_topics
    
    client.subscribe(subscribe_topics['bridge_call_topic'])
    print(f"subscribed to topic:{subscribe_topics['bridge_call_topic']}")
    print()



def update_health_check(publish_topics):
    global service_id,current_status
    data={"service_id":service_id,"status":current_status,"condition":current_condition}

def get_data():
    pass

def push_data():
    pass

def respond_to_manager():
    pass

def call_manager(msg):
    pass

def service_under_load():
    pass

def start_service():
    pass

def go_idle():
    pass

async def main():
    
    
    host="31d09ce8b7fa4a92aafc62ae06187541.s1.eu.hivemq.cloud"
    port=8883
    username="Snappp"
    password="Snap00989800"
    ssl_ctx = ssl.create_default_context()

    client=mqtt(service_id)
    client.set_auth_credentials(username=username, password=password)

    client.on_connect=on_connect
    client.on_message = partial(on_message,incomming_calls_queue=incomming_calls_queue)
    client.on_disconnect = on_disconnect
    client.on_subscribe=on_subscribe

    await client.connect(host=host, port=port, ssl=ssl_ctx)

if __name__=='__main__':
    asyncio.run(main())