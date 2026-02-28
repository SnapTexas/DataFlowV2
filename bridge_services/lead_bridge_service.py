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


manager_subscribe_topic={
                "from_manager":"00989800/to_bridge_calls" 
                }

manager_publish_topic={
                "to_manager":"00989800/from_bridge_calls"
            }

data_subscribe_topic={
    "data_topic":"$share/iot-data-pipeline-v2/00989800/#"
}
# _______ callbacks ______________

def on_connect(client,flags, rc, properties):# subscribe to manager
    global manager_subscribe_topic
    for i,j in manager_subscribe_topic.items():
        client.subscribe(j)
        print(f"subscribed to {i}:{j}")
        print()

def on_subscribe():#conifrm subscription
    print("subscription confirmed!!!")
    print()

def on_disconnet():# diconnected INFO
    print("Diconnected from Broker!!!")
    print()
# ________________________________

async def send_status_response(client):
    global service_id,current_status,manager_publish_topic
    data={"service_id":service_id,"status":current_status,"condition":current_condition,"is_status":True}
    print(f"Called manager with msg :{data}")
    call_manager(client,msg=data)

def get_data():
    pass

def push_data():
    pass

def respond_to_manager(client, topic, payload, qos, properties):
    msg_from_manager=payload.decode('utf-8')
    data=json.loads(msg_from_manager)
    if data.get('service_id')==service_id:
        print("Got msg from manager!!!")
        msg=data.get('msg')
        if msg=="START":
            start_service(client)
        elif msg=="IDLE":
            go_idle(client)
        elif msg=="STATUS":
            send_status_response(client)
        
        
    

def call_manager(client,msg):
    msg=json.dumps(msg)
    client.publish(manager_publish_topic['to_manager'],msg)

def service_under_load():
    global current_condition,service_id,current_status
    data={"service_id":service_id,"status":current_status,"condition":current_condition,"is_status":False}
    print("called manager to inform service under laod")
    call_manager(msg=data)
 
def start_service():
    pass

def go_idle(client):
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
    client.on_message = respond_to_manager
    client.on_disconnect = on_disconnect
    client.on_subscribe=

    await client.connect(host=host, port=port, ssl=ssl_ctx)

if __name__=='__main__':
    asyncio.run(main())