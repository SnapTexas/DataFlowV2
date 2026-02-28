import  uuid
from gmqtt import Client as mqtt_client
from functools import partial
import asyncio

# call backs 
def client_subscribe_to_all_topics(call_from_bridge_topic,call_from_validation_topic,client, flags, rc, properties):   
    client.subscribe(call_from_bridge_topic)
    print(f"Subscribed to topic : {call_from_bridge_topic}")
    client.subscribe(call_from_validation_topic)

def disconnected_from_mqtt(client, packet, exc=None):
    print("Disconnected from MQtt")

def respond_to_service_calls(client, topic, payload, qos, properties,call_to_bridge_topic=None,call_to_validation_topic=None):
    if call_to_bridge_topic!=None and call_to_validation_topic ==None:
        print("calling some bridge service")
    elif call_to_validation_topic!=None and call_to_bridge_topic==None:
        print("calling some data validation service")
    else:
        raise Exception("Cannot have both field empty or Filled")
    

def get_list_idle_bridge_services(msg):
    pass

def get_list_idle_data_validation_service():
    pass

async def main():
    print("Running...")
    broker="broker.hivemq.com"
    port=1883
    call_from_bridge_topic="00989800/call_from_bridge_topic"
    call_to_bridge_topic="/00989800/call_to_bridge_topic"
    call_from_validation_topic=""
    client_id=str(uuid.uuid4())

    client=mqtt_client(client_id=client_id)

    client.on_message = respond_to_service_calls
    client.on_disconnect = disconnected_from_mqtt
    client.on_connect = partial(client_subscribe_to_all_topics,call_from_bridge_topic,call_from_validation_topic)


    await asyncio.Event().wait()

if __name__=="__main__":
    asyncio.run(main())