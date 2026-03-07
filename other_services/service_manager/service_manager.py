import asyncio
from gmqtt import Client as mqtt
import uuid
import ssl
from functools import partial
import json

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

# --- STATE TRACKING ---
overload_service = {bridge_type: False, validation_type: False}
over_load_cooldown = 10
last_overload_change_time = 0 
last_seen_state_of_service = {}
service_expiry_time = 30  # TTL for dead services

active_bridge_service_set = set()
active_validation_service_set = set()
idle_bridge_services_set = set()
idle_validation_services_set = set()

# --- MQTT CALLBACKS ---
def on_connect(client, flags, rc, properties):
    client.subscribe(subscribe_topics['bridge_call_recv_topic'])
    print(f"Connected to Bridge Topics")

def on_connect_validation(client, flags, rc, properties):
    client.subscribe(subscribe_topics['validation_call_recv_topic'])
    print(f"Connected to Validation Topics")

async def on_message(client, topic, payload, qos, properties, incomming_calls_queue):
    service_data = {
        "topic": topic,
        "data": json.loads(payload.decode('utf-8'))
    }
    await incomming_calls_queue.put(json.dumps(service_data))

# --- LOGIC HELPERS ---
def assign_stuff_based_on_type(type):
    if type == bridge_type:
        return {
            "active_set": active_bridge_service_set,
            "idle_set": idle_bridge_services_set,
            "threshold": thresh_bridge_services,
            "call_topic": publish_topics['bridge_call_topic']
        }
    elif type == validation_type:
        return {
            "active_set": active_validation_service_set,
            "idle_set": idle_validation_services_set,
            "threshold": thresh_validation_services,
            "call_topic": publish_topics['validation_call_topic']
        }
    raise Exception("Invalid Service Type")

def update_services_status(service_data, s_type):
    data = assign_stuff_based_on_type(s_type)
    active_set = data['active_set']
    idle_set = data['idle_set']
    
    s_id = service_data.get("service_id")
    status = service_data.get("status")
    condition = service_data.get('condition')

    if status == "RUNNING" and condition == "NORMAL":
        active_set.add(s_id)
        idle_set.discard(s_id)
    elif status == "IDLE":
        active_set.discard(s_id)
        idle_set.add(s_id)
    elif condition == "OVERLOAD":
        active_set.add(s_id) # Still active if overloaded
        idle_set.discard(s_id)

async def heartbeat_publisher(bridge_client, validation_client):
    """Broadcasts STATUS requests and REAPS dead services."""
    while True:
        try:
            # 1. Broadast Pulse
            heartbeat_msg = json.dumps({"service_id": "ALL", "msg": "STATUS"})
            bridge_client.publish(publish_topics['bridge_call_topic'], heartbeat_msg)
            validation_client.publish(publish_topics['validation_call_topic'], heartbeat_msg)
            
            # 2. THE REAPER: Audit liveness
            now = asyncio.get_event_loop().time()
            for s_id in list(last_seen_state_of_service.keys()):
                info = last_seen_state_of_service[s_id]
                if (now - info['last_seen']) > service_expiry_time:
                    if not info.get('is_expired', False):
                        print(f"💀 REAPER: Service {s_id} timed out. Evicting.")
                        active_bridge_service_set.discard(s_id)
                        idle_bridge_services_set.discard(s_id)
                        active_validation_service_set.discard(s_id)
                        idle_validation_services_set.discard(s_id)
                        info['is_expired'] = True

            await asyncio.sleep(30)
        except Exception as e:
            print(f"Heartbeat Error: {e}")
            await asyncio.sleep(5)

def update_last_seen(msg_wrapper, last_seen_dict):
    topic = msg_wrapper.get('topic')
    data = msg_wrapper.get('data')
    s_id = data.get('service_id')
    s_type = bridge_type if "bridge" in topic else validation_type
    
    last_seen_dict[s_id] = {
        "last_seen": asyncio.get_event_loop().time(),
        "service_type": s_type,
        "is_expired": False
    }

def update_overload_status(current_condition, current_status, s_type):
    global overload_service, last_overload_change_time
    now = asyncio.get_event_loop().time()
    
    if (now - last_overload_change_time) < over_load_cooldown:
        return

    is_reporting_overload = (current_condition == "OVERLOAD" and current_status == "RUNNING")

    if is_reporting_overload and not overload_service[s_type]:
        print(f"⚠️ ALARM: {s_type} cluster OVERLOADED.")
        overload_service[s_type] = True
        last_overload_change_time = now
    elif not is_reporting_overload and overload_service[s_type]:
        print(f"✅ INFO: {s_type} cluster NORMAL.")
        overload_service[s_type] = False
        last_overload_change_time = now

# --- SCALING PROCEDURES ---
def start_service(client, service_id, s_type):
    data = json.dumps({"service_id": service_id, "msg": "START"})
    topic = publish_topics['bridge_call_topic'] if s_type == bridge_type else publish_topics['validation_call_topic']
    client.publish(topic, data)
    print(f"🚀 Sent START to {s_type}: {service_id}")

async def start_procedure(bc, vc):
    # Bridge Scaling
    if len(active_bridge_service_set) < thresh_bridge_services or overload_service[bridge_type]:
        chosen = choose_service_start(idle_bridge_services_set, bridge_type)
        if chosen: start_service(bc, chosen, bridge_type)

    # Validation Scaling
    if len(active_validation_service_set) < thresh_validation_services or overload_service[validation_type]:
        chosen = choose_service_start(idle_validation_services_set, validation_type)
        if chosen: start_service(vc, chosen, validation_type)

def choose_service_start(idle_set, s_type):
    if not idle_set: return None
    return idle_set.pop()

async def respond_msg(bc, vc, topic, data):
    s_type = bridge_type if topic == subscribe_topics['bridge_call_recv_topic'] else validation_type
    
    update_overload_status(data.get('condition'), data.get('status'), s_type)
    await start_procedure(bc, vc)

# --- CORE LOOPS ---
async def worker(bc, vc, queue):
    while True:
        try:
            raw = await queue.get()
            msg = json.loads(raw)
            data = msg['data']
            topic = msg['topic']
            
            # Update registry FIRST
            update_last_seen(msg, last_seen_state_of_service)
            s_type = bridge_type if "bridge" in topic else validation_type
            update_services_status(data, s_type)
            
            await respond_msg(bc, vc, topic, data)
        except Exception as e:
            print(f"Worker Error: {e}")
        finally:
            queue.task_done()
async def sync_infrastructure_and_boot(bc, vc):
    print("--- Initializing Infrastructure ---")
    while True:
        # 1. Ask for current status to fill the sets
        await asyncio.gather(
            get_services_status(bc, type=bridge_type),
            get_services_status(vc, type=validation_type)
        )
        
        # Give the worker a small moment (2s) to process the incoming MQTT replies
        await asyncio.sleep(2)

        b_count = len(active_bridge_service_set)
        v_count = len(active_validation_service_set)

        # 2. THE EXIT CONDITION: If targets are met, STOP the loop and move to steady state
        if b_count >= thresh_bridge_services and v_count >= thresh_validation_services:
            print(f"✅ INFRASTRUCTURE READY: Bridges({b_count}) Validations({v_count})")
            break # <--- This will finally stop the loop!
        
        # 3. Only if we are NOT ready, we try to start more
        print(f"🔄 Booting... Current: B({b_count}/{thresh_bridge_services}) V({v_count}/{thresh_validation_services})")
        await start_procedure(bc, vc)
async def get_services_status(client, type):
    """
    Sends a broadcast STATUS request to a specific cluster type.
    """
    data = assign_stuff_based_on_type(type)
    call_topic = data['call_topic']
    
    # Message to all services of this type
    status_request = {
        "service_id": "ALL",
        "msg": "STATUS"
    }
    
    client.publish(call_topic, json.dumps(status_request))
async def main():
    calls_q = asyncio.Queue()
    host, port = "31d09ce8b7fa4a92aafc62ae06187541.s1.eu.hivemq.cloud", 8883
    ssl_ctx = ssl.create_default_context()

    bc = mqtt(str(uuid.uuid4()))
    bc.set_auth_credentials("Snappp", "Snap00989800")
    bc.on_connect = on_connect
    bc.on_message = partial(on_message, incomming_calls_queue=calls_q)
    await bc.connect(host, port, ssl=ssl_ctx)

    vc = mqtt(str(uuid.uuid4()))
    vc.set_auth_credentials("Snappp", "Snap00989800")
    vc.on_connect = on_connect_validation
    vc.on_message = partial(on_message, incomming_calls_queue=calls_q)
    await vc.connect(host, port, ssl=ssl_ctx)

    asyncio.create_task(worker(bc, vc, calls_q))
    await sync_infrastructure_and_boot(bc, vc)
    asyncio.create_task(heartbeat_publisher(bc, vc))

    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())