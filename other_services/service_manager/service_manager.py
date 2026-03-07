import asyncio
from gmqtt import Client as mqtt
import uuid
import ssl
from functools import partial
import json
import time

# --- CONFIGURATION ---
subscribe_topics = {
    "bridge_call_recv_topic": "00989800/from_bridge_calls",
    "validation_call_recv_topic": "00989800/from_validation_calls"
}
publish_topics = {
    "bridge_call_topic": "00989800/to_bridge_calls",
    "validation_call_topic": "00989800/to_validation_calls"
}

thresh_bridge_services = 1
thresh_validation_services = 1

bridge_type = "bridge"
validation_type = "validation"




overload_service={bridge_type:False,validation_type:False}
over_load_cooldown=10
last_overload_change_time = 0 
last_seen_state_of_service={}
service_expiry_time=30# after 30s if we dont get a response we mark it as dead
collect_bridge_status_reponse=False
collect_validation_status_response = False
active_bridge_service_set = set()
active_validation_service_set = set() # Track validation services separately
idle_bridge_services_set =set()
idle_validation_services_set=set()
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
          "data":json.loads(payload.decode('utf-8'))}
    service_data=json.dumps(service_data)
    await incomming_calls_queue.put(service_data)

def assign_stuff_based_on_type(type):
    global thresh_bridge_services,thresh_validation_services
    global active_bridge_service_set,active_validation_service_set
    global idle_bridge_services_set,idle_validation_services_set
    global publish_topics,subscribe_topics
    if type==bridge_type:

        return {"active_set":active_bridge_service_set,
          "idle_set":idle_bridge_services_set,
          "threshold":thresh_bridge_services,
          "call_topic":publish_topics['bridge_call_topic'],
          "call_recv_topic":subscribe_topics['bridge_call_recv_topic']}

    elif type==validation_type:
        return {"active_set":active_validation_service_set,
          "idle_set":idle_validation_services_set,
          "threshold":thresh_validation_services,
          "call_topic":publish_topics['validation_call_topic'],
          "call_recv_topic":subscribe_topics['validation_call_recv_topic']}
    else:
        raise Exception("Invalid Type assign_stuuf fun")
    
    

def update_services_status(service_data,type):# gets or updates the result active bridge services set #WORKING HERE CONTINUE CREATE A FUN FOR ASSIGNING BASED ON TYPE AND MAKE 2 FUNCTIONS IN ONE
    data=assign_stuff_based_on_type(type=type)
    active_set=data['active_set']
    idle_set=data['idle_set']
    print(f"Update active {type} service was called")
    try:
        
        #service_data=json.loads(service_data)
        status = service_data.get("status")
        
        service_id = service_data.get("service_id")
        condition= service_data.get('condition')
        if status=="RUNNING" and condition=="NORMAL":
            #Adding to set of running services 
            active_set.add(service_id)
            idle_set.discard(service_id)
            print("Added a service")
            print(f"Active bridge set updated cuurent active service :{len(active_set)}")
        

        elif status=="IDLE" :
            #Removing idle services from set of running services
            active_set.discard(service_id)
            idle_set.add(service_id)
            print("Removed a service")
            print(f"Active bridge set updated cuurent active service :{len(active_set)}")
            print()
            print(f"Added to idle bridge services set : {len(idle_set)}")
    except Exception as e:
        print("update active bridge service failed",e)

async def get_services_status(client,type):# send a msg to all bridge services to respond with status which is collected by worker
    
    data=assign_stuff_based_on_type(type=type)
    call_topic=data['call_topic']
    service_data={"service_id":"ALL","msg":"STATUS","condition":None,"expected_reponse":True}
    service_data=json.dumps(service_data)
    client.publish(call_topic,service_data)
    #For now simply sleep for 10s and then collect whatever the response was 
    

    

# Helper for Validation status check
async def get_validation_services_status(validation_client):#DONE
    pass


def collect_validation_service_status_response(data):
    global validation_status_response_calls
    validation_status_response_calls.append(data)



def choose_service_start(idle_services_set,type):# takes a list of services and returns the first idle if of the first one if there are any else None
    if not idle_services_set: # Check if set is empty
        return None
    try:
        return idle_services_set.pop()
    except Exception as e:
        print("Exception Happend in choose start service",e)
        print("Number of services in idle ", type,"set:",len(idle_bridge_services_set))
        return None
def choose_service_stop(active_services_set):# takes a list of services and returns the first running service if there are more than one else None
    try:
        if not active_services_set: 
            return None
        return active_services_set.pop()
    except Exception as e:
        print("Exception Happend in choose stop service",e)
        print("Number of services in idle ", type,"set:",len(active_services_set))
        return None
    
def start_service(client,service_id,type): # starts the service whose id is given (Commands to start)
    #Command to start processing messages
    print(f"start bridge service called for {service_id} ,type={type}")
    
    data={"service_id":service_id,
          "msg":"START",
          "condition":None,
          "expected_reponse":False
          }
    data=json.dumps(data)
    if type==bridge_type:
        client.publish(publish_topics['bridge_call_topic'],data)
    else:
        client.publish(publish_topics['validation_call_topic'],data)

 


def stop_service_procedure(bridge_client,validation_client,type):
    global overload_service
    try:
        data=assign_stuff_based_on_type(type=type)
        active_set=data.get('active_set')
        threshold=data.get('threshold')
        call_topic=data.get('call_topic')

        
        if len(active_set)>threshold and  overload_service[type]==False:
        #await get_services_status(bridge_client=bridge_client,type=bridge_type)
            chosen_service_id=choose_service_stop(active_services_set=active_set)
            if chosen_service_id is None:
                raise Exception("Choose Service ID was None")
            else:
                if type==bridge_type:
                    stop_service(client=bridge_client,service_id=chosen_service_id,call_topic=call_topic)
                else:
                    stop_service(client=validation_client,service_id=chosen_service_id,call_topic=call_topic)
    except Exception as e:
        print("Exception happend IN stopping bridge service")

def stop_service(client,service_id,call_topic):# stops the service whose id is given (Commands to start)
    #Stop means to go idle
    print(f"stop service called for {service_id} on topic {call_topic}")
    
    service_data={"service_id":service_id,
          "msg":"IDLE",
          "condition":None,
          "expected_reponse":False}
    service_data=json.dumps(service_data)
    client.publish(call_topic,service_data)



def check_overload(type):
    global overload_service
    return overload_service[type]


def process_service_heartbeat(service_id,active_set,idle_set):
    global last_seen_state_of_service,service_expiry_time
    if  service_id in last_seen_state_of_service.keys():
        past_data=last_seen_state_of_service[service_id]
        last_seen_time=past_data.get('last_seen')
        current_time=asyncio.get_event_loop().time()
        #type_service=past_data.get('service_type')
        service_expired=past_data['is_expired']
        if service_expired==True:
            idle_set.add(service_id)
        if current_time-last_seen_time>service_expiry_time :
            print(f"Service of service_id:{service_id} has expired and being removed from active and idle sets ")
            active_set.discard(service_id)
            idle_set.discard(service_id)
            past_data['is_expired']=True

            #stop_service(service_id=service_id)
        else:
            past_data['is_expired']=False
            past_data['last_seen']=current_time



async def respond_msg(bridge_client, validation_client, topic:str,msg:str):

    print("Respond to msg called by worker")
    # 1. Parse once at the top
    type =  bridge_type if topic==subscribe_topics['bridge_call_recv_topic'] else validation_type    
    data=assign_stuff_based_on_type(type=type)
    active_set=data.get('active_set')
    idle_set=data.get('idle_set')
    threshold=data.get('threshold')
    service_id=msg.get('service_id')
    current_state=msg.get('status')
    current_condition=msg.get('condition')
    
    process_service_heartbeat(service_id=service_id,
                              active_set=active_set,
                              idle_set=idle_set)
    update_overload_status(current_condition=current_condition,
                            current_status=current_state,
                            s_type=type)
    await start_procedure(bridge_client=bridge_client,validation_client=validation_client)
    stop_service_procedure(bridge_client=bridge_client,validation_client=validation_client,type=type)
    
# Make sure this is initialized at the top of your script


def update_overload_status(current_condition, current_status, s_type):
    global overload_service, last_overload_change_time
    
    current_time = asyncio.get_event_loop().time()
    key = f"{s_type}_overload"
    
    # 1. Cooldown Check: If we changed state too recently, do nothing
    if (current_time - last_overload_change_time) < over_load_cooldown:
        return

    # 2. Determine the "Vote": Is this specific service reporting trouble?
    is_reporting_overload = (current_condition == "OVERLOAD" and current_status == "RUNNING")

    # 3. State Transition Logic
    # Transition to TRUE: Service reports overload and we are currently NORMAL
    if is_reporting_overload and not overload_service[key]:
        print(f"ALARM: {s_type} cluster moved to OVERLOAD. Scaling allowed.")
        overload_service[key] = True
        last_overload_change_time = current_time

    # Transition to FALSE: Service reports normal and we are currently in OVERLOAD
    elif not is_reporting_overload and overload_service[key]:
        print(f"INFO: {s_type} cluster stabilized. Returning to NORMAL.")
        overload_service[key] = False
        last_overload_change_time = current_time        
    

def update_last_seen(msg,last_seen:dict):
    topic=msg.get('topic')
    data=msg.get('data')
    service_id=data.get('service_id')
    current_state=data.get('status')
    current_condition=data.get('condition')
    service_type= bridge_type if topic==publish_topics['bridge_call_topic'] else validation_type
    last_seen[service_id]={"last_seen":asyncio.get_event_loop().time(),
                           "last_state":current_state,
                           "last_condition":current_condition,
                           "service_type":service_type,
                           "is_expired":False}

    
   
async def heartbeat_publisher(bridge_client,validation_client):
    """0
    Background task that pulses the Manager every 30 seconds.
    """
    global current_condition # The state updated by your worker
    
    print(f"Heartbeat was sent")
    
    while True:
        try:
            # Prepare the "One-Stage" heartbeat payload
            heartbeat_msg = {
                "service_id": "ALL",
                "msg": "STATUS"
            }
            
            # Publish to the Manager
            bridge_client.publish(publish_topics['bridge_call_topic'], heartbeat_msg)
            validation_client.publish(publish_topics['validation_call_topic'], heartbeat_msg)
            # logger.debug(f"Heartbeat sent: {current_condition}")
            
            # Sleep for exactly 30 seconds
            await asyncio.sleep(30)
            
        except Exception as e:
            print(f"Heartbeat failed: {e}")
            await asyncio.sleep(5) # Short sleep before retry if network fails





async def worker(bridge_client, validation_client, queue):#DONE# checks the queue for incomming calls and sends them to be processed and maintains the set of active bridge services
    print("Worker Called")
    global active_bridge_service_set,idle_bridge_services_set
    global active_validation_service_set,idle_validation_services_set
    global last_seen_state_of_service
    while True:
        
        try:
            msg = await queue.get()   # waits without blocking
            msg=json.loads(msg)
            data=msg['data']
            topic=msg['topic']
            
            print(f"Recieved Msg:{data} from topic :{topic}")
            await respond_msg(bridge_client, validation_client, topic,data)  
            print("Current number of active bridge_services:",len(active_bridge_service_set))
            print("Current number of idle bridge_services:",len(idle_bridge_services_set))
            print("Current number of active validation_services:",len(active_validation_service_set))
            print("Current number of idle validation_services:",len(idle_validation_services_set))
            update_last_seen(msg,last_seen_state_of_service)
        except Exception as e:
                print("Exception happend in Worker ", e)
               # slow work here
        queue.task_done()

def on_disconnect(client, packet, exc=None):# just prints diconnected used to see when disconnected
    print("Disconnected ...")

def on_subscribe(client, mid, qos, properties,type):# just prints subscribed used to see when disconnected
    print(f"subscribed to topics of type {type}")#DONE

async def start_procedure(bridge_client,validation_client):#Done
    global active_bridge_service_set,active_validation_service_set
    global idle_bridge_services_set,idle_validation_services_set

    minimal_bridge=True if len(active_bridge_service_set)<thresh_bridge_services else False

    minimal_validation=True if len(active_validation_service_set)<thresh_validation_services else False
    
    bridge_found,val_found=len(active_bridge_service_set)+len(idle_bridge_services_set),len(active_validation_service_set)+len(idle_validation_services_set)
    
    bridge_overload=overload_service[bridge_type]

    validation_overload=overload_service[validation_type]

    if minimal_bridge ==True or bridge_overload:
        chosen_id_bridge = choose_service_start(idle_bridge_services_set,type=bridge_type)
        if chosen_id_bridge is not None :
            print("called some bridge service to start")
            start_service(client=bridge_client, service_id=chosen_id_bridge,type=bridge_type)
        else:
            if len(idle_bridge_services_set)==0:
                print("NO idle bridge services found, chosen_id was None")

    if minimal_validation==True or validation_overload:
        chosen_id_validation =choose_service_start(idle_validation_services_set,type=validation_type)
        if  chosen_id_validation is not None:
            start_service(client=validation_client,service_id=chosen_id_validation,type=validation_type)
            print("called some validation service to start")
        else:
            if len(idle_bridge_services_set)==0:
                print("NO idle bridge services found, chosen_id was None")

    if bridge_found == 0 or val_found==0:
        print("WARNING: Check connections.")
        print(f"validation services Needed:{True if val_found<thresh_validation_services else False}")
        print(f"bridge services Needed:{True if bridge_found<thresh_bridge_services else False}")
    else:
        print("INFO: All Bridges and Validation are currently busy or no IDLE service available.")
        pass
    print(f"STILL WAITING: Bridges: {bridge_found}/{thresh_bridge_services}, "
            f"Validations: {val_found}/{thresh_validation_services}. Retrying in 5s...")
    await asyncio.sleep(5)

# --- MQTT HANDLERS ---

async def on_message_handler(client, topic, payload, qos, properties, queue):
    data = json.loads(payload.decode())
    await queue.put({"topic": topic, "data": data})

async def worker(queue):
    """Updates shared state based on incoming messages."""
    while True:
        # 1. Trigger fresh status sweep
        await asyncio.gather(
            get_services_status(bridge_client,type=bridge_type),
            get_services_status(validation_client,type=validation_type)
        )
        
        # Check how many services total (IDLE or RUNNING) responded
        bridge_found = len(active_bridge_service_set)
        print("Active Bridge Services set:",active_bridge_service_set)
        print("IDLE Brridge services set:",idle_bridge_services_set)
        val_found = len(active_validation_service_set)

        # We wait until we see at least the minimum required services online
        if bridge_found >= thresh_bridge_services  and val_found >= thresh_validation_services:
            print(f"SUCCESS: Infrastructure ready. Found {bridge_found} Bridges and {val_found} Validations.")
            break
        else:
            print(f"Number of Validation Service:{val_found}")
            print(f"Number of Bridge Service: {bridge_found}")
            print("We dont have required amount of them so trying to start more ")
            
            await start_procedure(bridge_client=bridge_client,
                            validation_client=validation_client
                        )
    # --- Initial Scaling Phase ---
    print("--- Initial Scaling: Booting minimum required services ---")

async def main():
    q = asyncio.Queue()
    host, port = "31d09ce8b7fa4a92aafc62ae06187541.s1.eu.hivemq.cloud", 8883
    username, password = "Snappp", "Snap00989800"
    ssl_ctx = ssl.create_default_context()

    bc = mqtt(str(uuid.uuid4()))
    bc.set_auth_credentials(username, password)
    bc.on_connect = partial(on_connect, t_key="bridge_call_recv_topic")
    bc.on_message = partial(on_message_handler, queue=q)

    vc = mqtt(str(uuid.uuid4()))
    vc.set_auth_credentials(username, password)
    vc.on_connect = partial(on_connect, t_key="validation_call_recv_topic")
    vc.on_message = partial(on_message_handler, queue=q)

    await bc.connect(host, port, ssl=ssl_ctx)
    await vc.connect(host, port, ssl=ssl_ctx)

    
    asyncio.create_task(worker(bridge_client=bridge_client, validation_client=validation_client, queue=incomming_calls_queue))
    
    await sync_infrastructure_and_boot(bridge_client, validation_client)
    
    asyncio.create_task(heartbeat_publisher(bridge_client=bridge_client,validation_client=validation_client))
    

    print(f"Discovery Complete. Active Bridges: {len(active_bridge_service_set)}, Active Validations: {len(active_validation_service_set)}")

    # 3. Optional: Initial Scaling Check
    # If 0 services are running but you need 1 (thresh_bridge_services), start one now.
    # --- Bridge Scaling Check ---
    

    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())