import  uuid
from gmqtt import Client as mqtt_client
from functools import partial
import asyncio

# call backs 
def client_subscribe_to_all_topics(client, flags, rc, properties,call_from_bridge_topic,call_from_validation_topic): 
    print("connected ")  
    #print(call_from_bridge_topic,call_from_validation_topic)
    client.subscribe(call_from_bridge_topic)
    print(f"Subscribed to topic : {call_from_bridge_topic}")
    client.subscribe(call_from_validation_topic)
    print(f"Subscribed to topic : {call_from_validation_topic}")

def disconnected_from_mqtt(client, packet, exc=None):
    print("Disconnected from MQtt")

def listen_to_service_calls(client, topic, payload, qos, properties,call_from_bridge_topic,call_from_validation_topic):
    if topic == call_from_bridge_topic:
        print(f"call came from bridge service : topic:{topic} msg:{payload.decode('utf-8')}")
    if topic == call_from_validation_topic:
        print(f"call came from validation service : topic:{topic} msg:{payload.decode('utf-8')}")

def respond_to_service_calls(client,topic,msg):
    client.publish(topic,msg.encode('utf-8'))
    

def get_list_idle_bridge_services(msg):
    pass

def get_list_idle_data_validation_service():
    pass

async def main():
    print("Running...")
    broker="broker.hivemq.com"
    port=1883
    call_from_bridge_topic="00989800/call-from-bridge-topic"
    call_to_bridge_topic="00989800/call-to-bridge-topic"
    call_from_validation_topic="00989800/call-from-validation-topic"
    client_id=str(uuid.uuid4())

    client=mqtt_client(client_id=client_id)
    client.on_connect = partial(client_subscribe_to_all_topics,
                                call_from_bridge_topic=call_from_bridge_topic,
                                call_from_validation_topic=call_from_validation_topic)
    client.on_message = partial(listen_to_service_calls,
                                call_from_bridge_topic=call_from_bridge_topic,
                                call_from_validation_topic=call_from_validation_topic)
    client.on_disconnect = disconnected_from_mqtt
    

    await client.connect(host=broker,port=port)

    await asyncio.Event().wait()

if __name__=="__main__":
    asyncio.run(main())