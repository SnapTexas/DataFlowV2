import asyncio
from gmqtt import Client as mqtt
import uuid
import ssl
from functools import partial
import json
subscribe_topics={
                  "bridge_call_recv_topic":"00989800/from_bridge_calls",
                  "validation_call_recv_topic":"00989800/from_validation_calls"
                  }
publish_topics={
                "bridge_call_topic":"00989800/to_bridge_calls",
                "validation_call_topic":"00989800/to_validation_calls"
                }
thresh_bridge_services=1# how many bridge services should run on default
bridge_status_response_calls=[]
collect_bridge_status_reponse=False
active_bridge_service_set = set()
#needs partial

def on_connect(client, flags, rc, properties):
    global subscribe_topics
    for i,j in subscribe_topics.items():
        client.subscribe(j)
        print(f"subscribed to {i}")

async def on_message(client, topic, payload, qos, properties,incomming_calls_queue):# when we get a msg put it in queue for worker to process
    #put the data in the calls queue for worker to process
    service_data={"topic":topic,
          "data":payload.decode('utf-8')}
    service_data=json.dumps(service_data)
    await incomming_calls_queue.put(service_data)

def get_active_bridge_services(service_data):# gets or updates the result active bridge services set 
    global active_bridge_service_set
    service_data=json.loads(service_data)
    if service_data['status']=="RUNNING" :
        #Adding to set of running services 
        active_bridge_service_set.add(service_data['service_id'])
    elif service_data['status']=="IDLE" and service_data['service_id'] in active_bridge_service_set:
        #Removing idle services from set of running services 
        active_bridge_service_set.remove(service_data['service_id'])

async def get_bridge_services_status(client,call_to_service_topic):# send a msg to all bridge services to respond with status which is collected by worker
    global active_bridge_service_set,thresh_bridge_services,bridge_status_response_calls,collect_bridge_status_reponse
    service_data={"service_id":"ALL","msg":"STATUS","condition":None}
    service_data=json.dumps(service_data)
    client.publish(call_to_service_topic,service_data)
    #For now simply sleep for 10s and then collect whatever the response was 
    flush_bridge_status_list()
    collect_bridge_status_reponse=True
    await asyncio.sleep(10)
    #See collected result from the worker assuming the worker processed the calls queue and we have count of how many responded with what updates the active set of bridge services
    collect_bridge_status_reponse=False

def collect_bridge_service_status_response(data):
    global bridge_status_response_calls
    bridge_status_response_calls.append(data)

def flush_bridge_status_list():
    global bridge_status_response_calls
    bridge_status_response_calls.clear()



def choose_service_start(list_of_services):# takes a list of services and returns the first idle if of the first one if there are any else None
    if list_of_services:
        for service in list_of_services:
                if service['status'] == "IDLE":
                    return service['id']
                    #activate the first one for now
    else:
        return None
            
def choose_service_stop(list_of_services):# takes a list of services and returns the first running service if there are more than one else None
     #from the list of bridge services status return the first service id which is running
    if len(list_of_services)>0:
        for service in list_of_services:
            if service['status'] == "RUNNING":
                return service['id']
                #activate the first one for now
    else:
        return None 

async def start_bridge_service(client,service_id): # starts the service whose id is given (Commands to start)
    #Command to start processing messages
    start_service_topic=f"00989800/call_to_bridge_service/{service_id}"
    client.publish(start_service_topic,"START")
    
async def stop_bridge_service(client,service_id):# stops the service whose id is given (Commands to start)
    #Stop means to go idle
    start_service_topic=f"00989800/call_to_bridge_service"
    service_data={"service_id":service_id,
          "msg":"IDLE",
          "condition":None}
    service_data=json.dumps(service_data)
    client.publish(start_service_topic,service_data)

async def verify_bridge_service_call_response(calls_to_bridge_services):# will verify the result of calls made to bridge services 
    pass
    # look at the queue who were messaged did they respond successfully
    # if yess inform if no inform or ping them to see if they are alive else till dead service

async def respond_msg(client,topic:str,msg:str):
    global active_bridge_service_set,bridge_status_response_calls
    # 1. Parse once at the top
    try:
        data = json.loads(msg)
    except: return

    if data.get("msg") == "STATUS":
        return
    if topic==subscribe_topics['bridge_call_recv_topic']:
        #Bridge service response
        service_data=json.loads(msg)
        if service_data['condition']=="UNDERLOAD":
            await get_bridge_services_status(client=client,call_to_service_topic=publish_topics['bridge_call_topic'])
            chosen_service_id=choose_service_start(bridge_status_response_calls)
            await start_bridge_service(client,chosen_service_id)  
        elif service_data['condition']=="NORMAL" and service_data['status']!="IDLE":# check if the running services are normal
            #Command One of the service to shut down if there are more than one service running on that topic
            #get list of active services on that topic if there are more than one services active on that topic stop one
            await get_bridge_services_status(client=client,call_to_bridge_service_topic=publish_topics['bridge_call_topic'])
            chosen_service_id=choose_service_stop(bridge_status_response_calls)
            await stop_bridge_service(client=client,service_id=chosen_service_id)
            
    elif topic==subscribe_topics['validation_call_recv_topic']:
        #Validations service response
        pass
    else:
        pass
    #client.publish(topic,msg)

async def worker(client,queue):# checks the queue for incomming calls and sends them to be processed and maintains the set of active bridge services
    while True:
        
        msg = await queue.get()   # waits without blocking
        msg=json.loads(msg)
        print(f"Recieved Msg:{msg['data']} from topic :{msg['topic']}")
        get_active_bridge_services(msg['data'])
        if collect_bridge_status_reponse:
            try:
                # Convert the string to a DICT so choose_service functions can read it
                decoded_data = json.loads(msg['data'])
                collect_bridge_service_status_response(decoded_data)
            except:
                pass
        await respond_msg(client,msg['topic'],msg['data'])        # slow work here
        queue.task_done()

def on_disconnect(client, packet, exc=None):# just prints diconnected used to see when disconnected
    print("Disconnected ...")
    print()

def on_subscribe(client, mid, qos, properties):# just prints subscribed used to see when disconnected
    print("subscribed to topics")
    print()

async def main():
    #store calls from services
    incomming_calls_queue=asyncio.Queue()

    host="31d09ce8b7fa4a92aafc62ae06187541.s1.eu.hivemq.cloud"
    port=8883
    username="Snappp"
    password="Snap00989800"
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