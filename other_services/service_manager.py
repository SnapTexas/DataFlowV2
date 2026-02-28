import asyncio
from gmqtt import Client as mqtt
import uuid
import ssl
from functools import partial
sub_topics=["00989800/bridge_calls_recv","00989800/validation_calls_recv"]


#needs partial
def on_connect(client, flags, rc, properties,subscribe_topics:list):
    for i in subscribe_topics:
        client.subscribe(i)

#needs partial
async def on_message(client, topic, payload, qos, properties,incomming_calls_queue):
    #put the data in the calls queue for worker to process
    data={"topic":topic,
          "data":payload.decode('utf-8')}
    await incomming_calls_queue.put_nowait(data)

async def get_bridge_services_status():
    pass

def choose_service(list_of_services):
    #from the list of bridge services status return the first service id which is idle
    for service in list_of_services:
        if service["status"] == "IDLE":
            return service["id"]
            #activate the first one for now
            


async def start_bridge_service(client,service_id):
    #Command to start processing messages
    start_service_topic=f"00989800/call_to_bridge_service/{service_id}"
    await client.publish(start_service_topic,"START")
    
async def stop_bridge_service(client,service_id):
    #Stop means to go idle
    start_service_topic=f"00989800/call_to_bridge_service/{service_id}"
    await client.publish(start_service_topic,"IDLE")

async def verify_bridge_service_call_response(calls_to_bridge_services):
    pass
    # look at the queue who were messaged did they respond successfully
    # if yess inform if no inform or ping them to see if they are alive else till dead service

async def respond_msg(client,topic,msg):
    if topic==sub_topics[0]:
        #Bridge service response
        if msg=="UNDERLOAD":
            bridge_services_status_list = await get_bridge_services_status()
            chosen_service_id=choose_service(bridge_services_status_list)
            await start_bridge_service(client,chosen_service_id)  

    elif topic==sub_topics[1]:
        #Validations service response
        pass
    else:
        pass
    #await client.publish(topic,msg)

async def worker(client,queue):
    while True:
        msg = await queue.get()   # waits without blocking
        print(f"Recieved Msg:{msg["data"]} from topic :{msg["topic"]}")
        await respond_msg(client,msg["topic"],msg["data"])        # slow work here
        queue.task_done()

def on_disconnect(client, packet, exc=None):
    print("Disconnected ...")
    print()

def on_subscribe(client, mid, qos, properties):
    print("subscribed to topics")
    print()

async def main():
    #store calls from services
    incomming_calls_queue=asyncio.Queue()

    host="31d09ce8b7fa4a92aafc62ae06187541.s1.eu.hivemq.cloud"
    port=8883
    username=""
    password=""
    client_id=str(uuid.uuid4())
    client=mqtt(client_id)

    client.set_auth_credentials(username=username, password=password)

    ssl_ctx = ssl.create_default_context()
    client.on_connect=on_connect
    client.on_message = partial(on_message,incomming_calls_queue=incomming_calls_queue)
    client.on_disconnect = on_disconnect
    client.on_subscribe=on_subscribe

    await client.connect(host=host,
                         port=port,
                         ssl=ssl_ctx)
    asyncio.create_task(worker(client=client,queue=incomming_calls_queue))

    await asyncio.Event().wait()

if __name__=="__main__":
    asyncio.run(main())