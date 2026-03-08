import asyncio
from gmqtt import Client as mqtt
import uuid
import ssl
from functools import partial
import json
import os
from dotenv import load_dotenv

load_dotenv()
# --- CONFIGURATION FROM ENV ---
MQTT_HOST = os.getenv("MQTT_HOST", "31d09ce8b7fa4a92aafc62ae06187541.s1.eu.hivemq.cloud")
MQTT_PORT = int(os.getenv("MQTT_PORT", 8883))
MQTT_USER = os.getenv("MQTT_USER", "Snappp")
MQTT_PASS = os.getenv("MQTT_PASS", "Snap00989800")

# Topic Configuration
BRIDGE_RECV = os.getenv("TOPIC_BRIDGE_RECV", "00989800/from_bridge_calls")
VAL_RECV = os.getenv("TOPIC_VAL_RECV", "00989800/from_validation_calls")
BRIDGE_SEND = os.getenv("TOPIC_BRIDGE_SEND", "00989800/to_bridge_calls")
VAL_SEND = os.getenv("TOPIC_VAL_SEND", "00989800/to_validation_calls")

# Thresholds
THRESH_BRIDGE = int(os.getenv("THRESH_BRIDGE", 1))
THRESH_VAL = int(os.getenv("THRESH_VAL", 1))
subscribe_topics = {
    "bridge_call_recv_topic": BRIDGE_RECV,
    "validation_call_recv_topic": VAL_RECV
}
publish_topics = {
    "bridge_call_topic": BRIDGE_SEND,
    "validation_call_topic": VAL_SEND
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

# --- DASHBOARD ---
def print_cluster_status():
    """Prints a clean view of the current network state."""
    print("\n" + "═"*50)
    print(f"📡 CLUSTER MONITOR | Time: {int(asyncio.get_event_loop().time())}")
    print(f"  BRIDGE:     Active={list(active_bridge_service_set)} | Idle={list(idle_bridge_services_set)}")
    print(f"  VALIDATION: Active={list(active_validation_service_set)} | Idle={list(idle_validation_services_set)}")
    print("═"*50 + "\n")

# --- MQTT CALLBACKS ---
def on_connect(client, flags, rc, properties):
    client.subscribe(subscribe_topics['bridge_call_recv_topic'])
    print(f"✅ Bridge Channel Connected")

def on_connect_validation(client, flags, rc, properties):
    client.subscribe(subscribe_topics['validation_call_recv_topic'])
    print(f"✅ Validation Channel Connected")

async def on_message(client, topic, payload, qos, properties, incomming_calls_queue):
    try:
        service_data = {
            "topic": topic,
            "data": json.loads(payload.decode('utf-8'))
        }
        await incomming_calls_queue.put(json.dumps(service_data))
    except Exception as e:
        print(f"Incoming Msg Error: {e}")

# --- LOGIC HELPERS ---
def assign_stuff_based_on_type(s_type):
    if s_type == bridge_type:
        return {
            "active_set": active_bridge_service_set,
            "idle_set": idle_bridge_services_set,
            "threshold": thresh_bridge_services,
            "call_topic": publish_topics['bridge_call_topic']
        }
    elif s_type == validation_type:
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

    if status == "RUNNING":
        active_set.add(s_id)
        idle_set.discard(s_id)
    elif status == "IDLE":
        active_set.discard(s_id)
        idle_set.add(s_id)

async def get_services_status(client, s_type):
    data = assign_stuff_based_on_type(s_type)
    msg = json.dumps({"service_id": "ALL", "msg": "STATUS"})
    client.publish(data['call_topic'], msg)

def update_last_seen(msg_wrapper, last_seen_dict):
    data = msg_wrapper.get('data')
    s_id = data.get('service_id')
    last_seen_dict[s_id] = {
        "last_seen": asyncio.get_event_loop().time(),
        "is_expired": False
    }

def update_overload_status(current_condition, current_status, s_type):
    global overload_service, last_overload_change_time
    now = asyncio.get_event_loop().time()
    
    if (now - last_overload_change_time) < over_load_cooldown:
        return

    is_reporting_overload = (current_condition == "OVERLOAD" and current_status == "RUNNING")

    if is_reporting_overload and not overload_service[s_type]:
        print(f"⚠️ ALARM: {s_type} cluster moved to OVERLOAD.")
        overload_service[s_type] = True
        last_overload_change_time = now
    elif not is_reporting_overload and overload_service[s_type]:
        print(f"ℹ️ INFO: {s_type} cluster returned to NORMAL.")
        overload_service[s_type] = False
        last_overload_change_time = now

def scaling_down(s_type, client):
    """Checks if we have too many active services and sends them to IDLE."""
    global overload_service
    data = assign_stuff_based_on_type(s_type)
    active_set = data.get('active_set')
    threshold = data.get('threshold')
    call_topic = data.get('call_topic')
    
    # Scale down only if cluster is NOT overloaded and we are above threshold
    if not overload_service[s_type] and len(active_set) > threshold:
        # Get a service ID from active set
        service_id = list(active_set)[0] 
        send_data = json.dumps({
            "service_id": service_id,
            "msg": "IDLE"
        })
        client.publish(call_topic, send_data)
        print(f"📉 Scaling: {s_type} [{service_id}] requested to go IDLE.")

# --- SCALING & REAPER ---
def start_service(client, service_id, s_type):
    msg = json.dumps({"service_id": service_id, "msg": "START"})
    topic = publish_topics['bridge_call_topic'] if s_type == bridge_type else publish_topics['validation_call_topic']
    client.publish(topic, msg)
    print(f"🚀 Scaling: Starting {s_type} [{service_id}]")

async def start_procedure(bc, vc):
    """The central scaling decision logic."""
    # Bridge scaling
    if len(active_bridge_service_set) < thresh_bridge_services or overload_service[bridge_type]:
        chosen = list(idle_bridge_services_set)[0] if idle_bridge_services_set else None
        if chosen: start_service(bc, chosen, bridge_type)

    # Validation scaling
    if len(active_validation_service_set) < thresh_validation_services or overload_service[validation_type]:
        chosen = list(idle_validation_services_set)[0] if idle_validation_services_set else None
        if chosen: start_service(vc, chosen, validation_type)

async def heartbeat_publisher(bc, vc):
    while True:
        try:
            msg = json.dumps({"service_id": "ALL", "msg": "STATUS"})
            bc.publish(publish_topics['bridge_call_topic'], msg)
            vc.publish(publish_topics['validation_call_topic'], msg)
            
            now = asyncio.get_event_loop().time()
            for s_id in list(last_seen_state_of_service.keys()):
                info = last_seen_state_of_service[s_id]
                if (now - info['last_seen']) > service_expiry_time:
                    if not info.get('is_expired', False):
                        print(f"💀 REAPER: Evicting stale service {s_id}")
                        active_bridge_service_set.discard(s_id)
                        idle_bridge_services_set.discard(s_id)
                        active_validation_service_set.discard(s_id)
                        idle_validation_services_set.discard(s_id)
                        info['is_expired'] = True
            
            await start_procedure(bc, vc)
            await asyncio.sleep(30)
        except Exception as e:
            print(f"Heartbeat Error: {e}")
            await asyncio.sleep(5)

# --- CORE LOOPS ---
async def worker(bc, vc, queue):
    while True:
        try:
            raw = await queue.get()
            try:
                msg = json.loads(raw)
                data = msg['data']
                topic = msg['topic']
                s_type = bridge_type if "bridge" in topic else validation_type
                client = bc if s_type == bridge_type else vc
                
                update_last_seen(msg, last_seen_state_of_service)
                update_services_status(data, s_type)
                update_overload_status(data.get('condition'), data.get('status'), s_type)
                
                print_cluster_status()
                
                # Try scaling down if cluster is healthy
                scaling_down(s_type=s_type, client=client)
                
                # Ensure minimum services are running
                await start_procedure(bc, vc)
            finally:
                queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Worker Error: {e}")

async def sync_infrastructure_and_boot(bc, vc):
    print("--- 🛠 Initializing Infrastructure 🛠 ---")
    while True:
        await get_services_status(bc, bridge_type)
        await get_services_status(vc, validation_type)
        await asyncio.sleep(2) 

        b_count = len(active_bridge_service_set)
        v_count = len(active_validation_service_set)

        if b_count >= thresh_bridge_services and v_count >= thresh_validation_services:
            print(f"✨ ALL SYSTEMS GO: Bridges={b_count}, Validations={v_count}")
            break
        
        print(f"⏳ Waiting for nodes... (B:{b_count}/{thresh_bridge_services} V:{v_count}/{thresh_validation_services})")
        await start_procedure(bc, vc)
        await asyncio.sleep(3)

async def main():
    calls_q = asyncio.Queue()
    host, port = MQTT_HOST,MQTT_PORT
    ssl_ctx = ssl.create_default_context()

    bc = mqtt(str(uuid.uuid4()))
    bc.set_auth_credentials(MQTT_USER, MQTT_PASS)
    bc.on_connect = on_connect
    bc.on_message = partial(on_message, incomming_calls_queue=calls_q)
    await bc.connect(host, port, ssl=ssl_ctx)

    vc = mqtt(str(uuid.uuid4()))
    vc.set_auth_credentials(MQTT_USER, MQTT_PASS)
    vc.on_connect = on_connect_validation
    vc.on_message = partial(on_message, incomming_calls_queue=calls_q)
    await vc.connect(host, port, ssl=ssl_ctx)

    asyncio.create_task(worker(bc, vc, calls_q))
    await sync_infrastructure_and_boot(bc, vc)
    asyncio.create_task(heartbeat_publisher(bc, vc))

    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Orchestrator stopped.")