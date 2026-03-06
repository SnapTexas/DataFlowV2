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
thresh_validation_services=1

bridge_type="bridge"
validation_type="validation"



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
        threshold=thresh_bridge_services
        active_set=active_bridge_service_set
        idle_set=idle_bridge_services_set
        call_topic=publish_topics["bridge_call_topic"]
        call_recv_topic=subscribe_topics['bridge_call_recv_topic']

    elif type==validation_type:
        threshold=thresh_validation_services
        active_set=active_validation_service_set
        idle_set=idle_validation_services_set
        call_topic=publish_topics["validation_call_topic"]
        call_recv_topic=subscribe_topics['validation_call_recv_topic']
    else:
        raise Exception("Invalid Type assign_stuuf fun")
    
    data={"active_set":active_set,
          "idle_set":idle_set,
          "threshold":threshold,
          "call_topic":call_topic,
          "call_recv_topic":call_recv_topic}
    return data
    

def update_active_services(service_data,type):# gets or updates the result active bridge services set #WORKING HERE CONTINUE CREATE A FUN FOR ASSIGNING BASED ON TYPE AND MAKE 2 FUNCTIONS IN ONE
    data=assign_stuff_based_on_type(type=type)
    active_set=data['active_set']
    idle_set=data['idle_set']
    print(f"Update active {type} service was called")
    try:
        
        #service_data=json.loads(service_data)
        status = service_data.get("status")
        is_status = service_data.get("is_status")
        service_id = service_data.get("service_id")
        condition= service_data.get('condition')
        if status=="RUNNING" and is_status==True:
            #Adding to set of running services 
            active_set.add(service_id)
            idle_set.discard(service_id)
            print("Added a service")
            print(f"Active bridge set updated cuurent active service :{len(active_set)}")
        

        elif status=="IDLE" and is_status==True:
            #Removing idle services from set of running services
            active_set.discard(service_id)
            idle_set.add(service_id)
            print("Removed a service")
            print(f"Active bridge set updated cuurent active service :{len(active_set)}")
            print()
            print(f"Added to idle bridge services set : {len(idle_set)}")
    except Exception as e:
        print("update active bridge service failed",e)

async def get_bridge_services_status(bridge_client,type):# send a msg to all bridge services to respond with status which is collected by worker
    
    global active_bridge_service_set,thresh_bridge_services,bridge_status_response_calls,collect_bridge_status_reponse
    service_data={"service_id":"ALL","msg":"STATUS","condition":None,"expected_reponse":True}
    service_data=json.dumps(service_data)
    bridge_client.publish(publish_topics['bridge_call_topic'],service_data)
    #For now simply sleep for 10s and then collect whatever the response was 
    

    

# Helper for Validation status check
async def get_validation_services_status(validation_client):#DONE
    global validation_status_response_calls,collect_validation_status_response
    service_data={"service_id":"ALL","msg":"STATUS","condition":None,"expected_reponse":True}
    service_data=json.dumps(service_data)
    validation_client.publish(publish_topics['validation_call_topic'],service_data)




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

 


async def stop_service_procedure(client,type):
    
    try:
        data=assign_stuff_based_on_type(type=type)
        active_set=data.get('active_set')
        threshold=data.get('threshold')
        call_topic=data.get('call_topic')


        if len(active_set)>threshold:
        #await get_bridge_services_status(bridge_client=bridge_client)
            chosen_service_id=choose_service_stop(active_services_set=active_set)
            if chosen_service_id is None:
                raise Exception("Choose Service ID was None")
            else:
                await stop_service(client=client,service_id=chosen_service_id,call_topic=call_topic)
    except Exception as e:
        print("Exception happend IN stopping bridge service")

async def stop_service(client,service_id,call_topic):# stops the service whose id is given (Commands to start)
    #Stop means to go idle
    print(f"stop service called for {service_id} on topic {call_topic}")
    
    service_data={"service_id":service_id,
          "msg":"IDLE",
          "condition":None,
          "expected_reponse":False}
    service_data=json.dumps(service_data)
    client.publish(call_topic,service_data)



def check_service_is_ok_now(status,condition,type):
    try:
        data=assign_stuff_based_on_type(type=type)
        active_set=data['active_set']
        threshold=data['threshold']
    
        if  status=="RUNNING" and condition =="NORMAL" :
            if len(active_set)>threshold:
                return "OK"
    except Exception as e:
        print("Hey exception heppend when checking service is ok")
        print("Exception:",e)
        return None

async def status_response(client,msg,topic):#DONE
    
    if msg.get('is_status') == True:
        status=msg.get('status')
        condition=msg.get('condition')
        print("Status response recieved ",end='')
        # Route to correct active set management
        if topic == subscribe_topics['bridge_call_recv_topic']:
            service_status_call_back=check_service_is_ok_now(status,condition,type=bridge_type)
            if service_status_call_back =="OK":
                print("SToppping BRIDGE SERVICE called from status_response")
                await stop_service_procedure(client,type=bridge_type)
            else:
                update_active_services(msg,type=bridge_type)
                print("from Bridge service")
                print("Update active bridge service called with msg",msg)
            
        else:
                #needs tobe  updated
            service_status_call_back=check_service_is_ok_now(status,condition,type=validation_type)
            if service_status_call_back =="OK":
                print("SToppping Validation SERVICE called from status_response")
                await stop_service_procedure(client,type=validation_type)
            else:
                update_active_services(msg,type=validation_type)
                print("from validation service")
                print("Update active validation service called with msg",msg)
        return True


async def respond_msg(bridge_client, validation_client, topic:str,msg:str):
    global active_bridge_service_set,  idle_bridge_services_set
    global active_validation_service_set, idle_validation_services_set
    global thresh_bridge_services,thresh_validation_services
    print("Respond to msg called by worker")
    # 1. Parse once at the top
    

    
    if topic==subscribe_topics['bridge_call_recv_topic']:
        is_status_response=await status_response(bridge_client,msg=msg,topic=topic)
        if is_status_response==True:
            print("Respond_msg has responded to status response and returned")
            return 
        #Bridge service response
        if msg.get('condition')=="OVERLOAD" :
            print("Underload safety triggered")
            await get_bridge_services_status(bridge_client=bridge_client)
            chosen_service_id=choose_service_start(idle_services_set=idle_bridge_services_set,type=bridge_type)
            if chosen_service_id is not None:
                start_service(bridge_client,chosen_service_id,type=bridge_type)
            else:
                print("NO CHOSEN SERVICE ID DUE TO EMPTY IDLE LIST ")

        elif msg.get('condition')=="NORMAL" and msg.get('status')!="IDLE" and len(active_bridge_service_set)>thresh_bridge_services:# check if the running services are normal
            #Command One of the service to shut down if there are more than one service running on that topic
            #get list of active services on that topic if there are more than one services active on that topic stop one
            await stop_service_procedure(bridge_client,type=bridge_type)
           
    elif topic==subscribe_topics['validation_call_recv_topic']:
        #Validations service response
        is_status_response=await status_response(validation_client,msg=msg,topic=topic)
        if is_status_response==True:
            return 
        if msg.get('condition')=="OVERLOAD":
            print("Validation Underload detected!")
            await get_validation_services_status(validation_client)
            chosen_val_id = choose_service_start(idle_services_set=idle_validation_services_set,type=validation_type)
            if chosen_val_id is not None:
              
                start_service(validation_client,service_id=chosen_val_id,type=validation_type)
            else:
                print("NO CHOSEN SERVICE ID DUE TO EMPTY IDLE LIST ")
        #Maybe we can remove this part Ill see
        elif msg.get('condition')=="NORMAL" and msg.get('status')!="IDLE" and len(active_validation_service_set)>thresh_validation_services:# check if the running services are normal
            #Command One of the service to shut down if there are more than one service running on that topic
            #get list of active services on that topic if there are more than one services active on that topic stop one
            await stop_service_procedure(bridge_client,type=validation_type)

async def worker(bridge_client, validation_client, queue):#DONE# checks the queue for incomming calls and sends them to be processed and maintains the set of active bridge services
    print("Worker Called")
    global active_bridge_service_set,idle_bridge_services_set
    global active_validation_service_set,idle_validation_services_set
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
        except Exception as e:
                print("Exception happend in Worker ", e)
               # slow work here
        queue.task_done()

def on_disconnect(client, packet, exc=None):# just prints diconnected used to see when disconnected
    print("Disconnected ...")

def on_subscribe(client, mid, qos, properties,type):# just prints subscribed used to see when disconnected
    print(f"subscribed to topics of type {type}")#DONE

async def start_procedure(bridge_client,validation_client,bridge_found,val_found):#Done
    chosen_id_bridge = choose_service_start(idle_bridge_services_set,type=bridge_type)
    chosen_id_validation =choose_service_start(idle_validation_services_set,type=validation_type)
    if chosen_id_bridge is not None and chosen_id_validation is not None:
        print("called some bridge service to start")
        start_service(client=bridge_client, service_id=chosen_id_bridge,type=bridge_type)
        start_service(client=validation_client,service_id=chosen_id_validation,type=validation_type)

        print("Commanded both service to stop")
    elif bridge_found == 0 or val_found==0:
        print("WARNING: Check connections.")
        print("NUmber of bridge services found",bridge_found)
        print("NUmber of validation services found",val_found)
        print(f"validation services Needed:{True if val_found<thresh_validation_services else False}")
        print(f"bridge services Needed:{True if bridge_found<thresh_bridge_services else False}")
    else:
        print("INFO: All Bridges and Validation are currently busy or no IDLE service available.")
        pass
    print(f"STILL WAITING: Bridges: {bridge_found}/{thresh_bridge_services}, "
            f"Validations: {val_found}/{thresh_validation_services}. Retrying in 5s...")
    await asyncio.sleep(5)


async def sync_infrastructure_and_boot(bridge_client, validation_client):#DONE
    print("--- Infrastructure Sync: Waiting for services to check in ---")
    global active_bridge_service_set,idle_bridge_services_set
    global active_validation_service_set,idle_validation_services_set
    while True:
        # 1. Trigger fresh status sweep
        await asyncio.gather(
            get_bridge_services_status(bridge_client),
            get_validation_services_status(validation_client)
        )
        
        # Check how many services total (IDLE or RUNNING) responded
        bridge_found = len(active_bridge_service_set)
        print("Active Bridge Services set:",active_validation_service_set)
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
                            validation_client=validation_client,
                            bridge_found=bridge_found,
                            val_found=val_found)
    # --- Initial Scaling Phase ---
    print("--- Initial Scaling: Booting minimum required services ---")

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
    bridge_client.on_subscribe=partial(on_subscribe,type=bridge_type)
    await bridge_client.connect(host=host, port=port, ssl=ssl_ctx)

    # --- Client 2: Validation Manager ---
    validation_client=mqtt(str(uuid.uuid4()))
    validation_client.set_auth_credentials(username=username, password=password)
    validation_client.on_connect= on_connect_validation
    validation_client.on_message = partial(on_message,incomming_calls_queue=incomming_calls_queue)
    validation_client.on_disconnect = on_disconnect
    validation_client.on_subscribe=partial(on_subscribe,type=validation_type)
    await validation_client.connect(host=host, port=port, ssl=ssl_ctx)

    
    asyncio.create_task(worker(bridge_client=bridge_client, validation_client=validation_client, queue=incomming_calls_queue))

    await sync_infrastructure_and_boot(bridge_client, validation_client)

    print(f"Discovery Complete. Active Bridges: {len(active_bridge_service_set)}, Active Validations: {len(active_validation_service_set)}")

    # 3. Optional: Initial Scaling Check
    # If 0 services are running but you need 1 (thresh_bridge_services), start one now.
    # --- Bridge Scaling Check ---
    

    await asyncio.Event().wait()


if __name__=="__main__":
    asyncio.run(main())