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

# Separate response lists for Bridge and Validation
bridge_status_response_calls=[]
validation_status_response_calls=[]

collect_bridge_status_reponse=False
active_bridge_service_set = set()
active_validation_service_set = set() # Track validation services separately

#needs partial
def on_connect(bridge_client, flags, rc, properties):
    global subscribe_topics
    bridge_client.subscribe(subscribe_topics['bridge_call_recv_topic'])
    print(f"Bridge client subscribed to bridge_call_recv_topic")

def on_connect_validation(validation_client, flags, rc, properties):
    global subscribe_topics
    validation_client.subscribe(subscribe_topics['validation_call_recv_topic'])
    print(f"Validation client subscribed to validation_call_recv_topic")

async def on_message(bridge_client, topic, payload, qos, properties,incomming_calls_queue):# when we get a msg put it in queue for worker to process
    #put the data in the calls queue for worker to process
    service_data={"topic":topic,
          "data":payload.decode('utf-8')}
    service_data=json.dumps(service_data)
    await incomming_calls_queue.put(service_data)

async def on_message_validation(validation_client, topic, payload, qos, properties,incomming_calls_queue):
    #put the data in the calls queue for worker to process
    service_data={"topic":topic,
          "data":payload.decode('utf-8')}
    service_data=json.dumps(service_data)
    await incomming_calls_queue.put(service_data)

def get_active_bridge_services(service_data):# gets or updates the result active bridge services set 
    global active_bridge_service_set
    service_data=json.loads(service_data)
    if service_data.get('status')=="RUNNING" :
        #Adding to set of running services 
        active_bridge_service_set.add(service_data.get('service_id'))
    elif service_data.get('status')=="IDLE" and service_data.get('service_id') in active_bridge_service_set:
        #Removing idle services from set of running services 
        active_bridge_service_set.remove(service_data.get('service_id'))

def get_active_validation_services(service_data):# gets or updates the result active bridge services set 
    global active_validation_service_set
    service_data=json.loads(service_data)
    if service_data.get('status')=="RUNNING" :
        #Adding to set of running services 
        active_validation_service_set.add(service_data.get('service_id'))
    elif service_data.get('status')=="IDLE" and service_data.get('service_id') in active_validation_service_set:
        #Removing idle services from set of running services 
        active_validation_service_set.remove(service_data.get('service_id'))

async def get_bridge_services_status(bridge_client,call_to_service_topic):# send a msg to all bridge services to respond with status which is collected by worker
    global active_bridge_service_set,thresh_bridge_services,bridge_status_response_calls,collect_bridge_status_reponse
    service_data={"service_id":"ALL","msg":"STATUS","condition":None}
    service_data=json.dumps(service_data)
    bridge_client.publish(call_to_service_topic,service_data)
    #For now simply sleep for 10s and then collect whatever the response was 
    flush_bridge_status_list()
    collect_bridge_status_reponse=True
    await asyncio.sleep(10)
    #See collected result from the worker assuming the worker processed the calls queue and we have count of how many responded with what updates the active set of bridge services
    collect_bridge_status_reponse=False

# Helper for Validation status check
async def get_validation_services_status(validation_client,call_to_service_topic):
    global validation_status_response_calls,collect_bridge_status_reponse
    service_data={"service_id":"ALL","msg":"STATUS","condition":None}
    service_data=json.dumps(service_data)
    validation_client.publish(call_to_service_topic,service_data)
    validation_status_response_calls.clear()
    collect_bridge_status_reponse=True
    await asyncio.sleep(10)
    collect_bridge_status_reponse=False

def collect_bridge_service_status_response(data):
    global bridge_status_response_calls
    bridge_status_response_calls.append(data)

def collect_validation_service_status_response(data):
    global validation_status_response_calls
    validation_status_response_calls.append(data)

def flush_bridge_status_list():
    global bridge_status_response_calls
    bridge_status_response_calls.clear()

def choose_service_start(list_of_services):# takes a list of services and returns the first idle if of the first one if there are any else None
    if list_of_services:
        for service in list_of_services:
                if service.get('status') == "IDLE":
                    return service.get('service_id') or service.get('id')
                    #activate the first one for now
    return None
            
def choose_service_stop(list_of_services):# takes a list of services and returns the first running service if there are more than one else None
     #from the list of bridge services status return the first service id which is running
    if len(list_of_services)>0:
        for service in list_of_services:
            if service.get('status') == "RUNNING":
                return service.get('service_id') or service.get('id')
                #activate the first one for now
    return None 

async def start_bridge_service(bridge_client,service_id): # starts the service whose id is given (Commands to start)
    #Command to start processing messages
    print(f"start bridge service called for {service_id}")
    start_service_topic=f"00989800/call_to_bridge_service/{service_id}"
    bridge_client.publish(start_service_topic,"START")
    
async def stop_bridge_service(bridge_client,service_id):# stops the service whose id is given (Commands to start)
    #Stop means to go idle
    print(f"stop bridge service called for {service_id}")
    start_service_topic=f"00989800/call_to_bridge_service"
    service_data={"service_id":service_id,
          "msg":"IDLE",
          "condition":None}
    service_data=json.dumps(service_data)
    bridge_client.publish(start_service_topic,service_data)

async def respond_msg(bridge_client, validation_client, topic:str,msg:str):
    global active_bridge_service_set,bridge_status_response_calls, validation_status_response_calls
    # 1. Parse once at the top
    try:
        data = json.loads(msg)
    except: return

    if data.get("msg") == "STATUS":
        return
    if topic==subscribe_topics['bridge_call_recv_topic']:
        #Bridge service response
        if data.get('condition')=="UNDERLOAD":
            await get_bridge_services_status(bridge_client=bridge_client,call_to_service_topic=publish_topics['bridge_call_topic'])
            chosen_service_id=choose_service_start(bridge_status_response_calls)
            await start_bridge_service(bridge_client,chosen_service_id)  
        elif data.get('condition')=="NORMAL" and data.get('status')!="IDLE":# check if the running services are normal
            #Command One of the service to shut down if there are more than one service running on that topic
            #get list of active services on that topic if there are more than one services active on that topic stop one
            await get_bridge_services_status(bridge_client=bridge_client,call_to_service_topic=publish_topics['bridge_call_topic'])
            chosen_service_id=choose_service_stop(bridge_status_response_calls)
            await stop_bridge_service(bridge_client=bridge_client,service_id=chosen_service_id)
            
    elif topic==subscribe_topics['validation_call_recv_topic']:
        #Validations service response
        if data.get('condition')=="UNDERLOAD":
            print("Validation Underload detected!")
            await get_validation_services_status(validation_client, publish_topics['validation_call_topic'])
            chosen_val_id = choose_service_start(validation_status_response_calls)
            if chosen_val_id:
                val_topic = f"00989800/call_to_validation_service/{chosen_val_id}"
                validation_client.publish(val_topic, "START")

async def worker(bridge_client, validation_client, queue):# checks the queue for incomming calls and sends them to be processed and maintains the set of active bridge services
    while True:
        msg = await queue.get()   # waits without blocking
        msg=json.loads(msg)
        print(f"Recieved Msg:{msg['data']} from topic :{msg['topic']}")
        
        # Route to correct active set management
        if msg['topic'] == subscribe_topics['bridge_call_recv_topic']:
            get_active_bridge_services(msg['data'])
        else:
            get_active_validation_services(msg['data'])

        if collect_bridge_status_reponse:
            try:
                # Convert the string to a DICT so choose_service functions can read it
                decoded_data = json.loads(msg['data'])
                # Route to correct collection list
                if msg['topic'] == subscribe_topics['bridge_call_recv_topic']:
                    collect_bridge_service_status_response(decoded_data)
                else:
                    collect_validation_service_status_response(decoded_data)
            except:
                pass
        await respond_msg(bridge_client, validation_client, msg['topic'],msg['data'])         # slow work here
        queue.task_done()

def on_disconnect(client, packet, exc=None):# just prints diconnected used to see when disconnected
    print("Disconnected ...")

def on_subscribe(client, mid, qos, properties):# just prints subscribed used to see when disconnected
    print("subscribed to topics")

async def main():
    #store calls from services
    incomming_calls_queue=asyncio.Queue()

    host="31d09ce8b7fa4a92aafc62ae06187541.s1.eu.hivemq.cloud"
    port=8883
    username="Snappp"
    password="Snap00989800"
    ssl_ctx = ssl.create_default_context()

    # --- Client 1: Bridge Manager ---
    bridge_client=mqtt(str(uuid.uuid4()))
    bridge_client.set_auth_credentials(username=username, password=password)
    bridge_client.on_connect=on_connect
    bridge_client.on_message = partial(on_message,incomming_calls_queue=incomming_calls_queue)
    bridge_client.on_disconnect = on_disconnect
    bridge_client.on_subscribe=on_subscribe
    await bridge_client.connect(host=host, port=port, ssl=ssl_ctx)

    # --- Client 2: Validation Manager ---
    validation_client=mqtt(str(uuid.uuid4()))
    validation_client.set_auth_credentials(username=username, password=password)
    validation_client.on_connect= on_connect_validation
    validation_client.on_message = partial(on_message_validation,incomming_calls_queue=incomming_calls_queue)
    validation_client.on_disconnect = on_disconnect
    validation_client.on_subscribe=on_subscribe
    await validation_client.connect(host=host, port=port, ssl=ssl_ctx)

    asyncio.create_task(worker(bridge_client=bridge_client, validation_client=validation_client, queue=incomming_calls_queue))

    await asyncio.Event().wait()

if __name__=="__main__":
    asyncio.run(main())