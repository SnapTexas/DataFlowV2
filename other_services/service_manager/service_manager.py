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

# --- SHARED STATE ---
active_bridge_service_set = set()
idle_bridge_services_set = set()
active_validation_service_set = set()
idle_validation_services_set = set()

service_last_seen = {} 
scale_up_needed = {"bridge": False, "validation": False}

# --- HELPERS ---

def assign_stuff_based_on_type(service_type):
    if service_type == bridge_type:
        return {
            "active_set": active_bridge_service_set,
            "idle_set": idle_bridge_services_set,
            "threshold": thresh_bridge_services,
            "call_topic": publish_topics["bridge_call_topic"]
        }
    else:
        return {
            "active_set": active_validation_service_set,
            "idle_set": idle_validation_services_set,
            "threshold": thresh_validation_services,
            "call_topic": publish_topics["validation_call_topic"]
        }

def update_active_services(service_data, service_type):
    data = assign_stuff_based_on_type(service_type)
    active_set, idle_set = data['active_set'], data['idle_set']
    
    s_id = service_data.get("service_id")
    status = service_data.get("status")
    condition = service_data.get("condition")

    service_last_seen[s_id] = time.time()

    if status == "RUNNING":
        scale_up_needed[service_type] = False

    if condition == "OVERLOAD" or status == "RUNNING":
        active_set.add(s_id)
        idle_set.discard(s_id)
    elif status == "IDLE":
        active_set.discard(s_id)
        idle_set.add(s_id)

def start_service(client, service_id, service_type, call_topic):
    msg = json.dumps({"service_id": service_id, "msg": "START", "condition": None, "expected_reponse": False})
    client.publish(call_topic, msg)
    print(f"🚀 [ACTION] Scaling UP {service_type}: Starting {service_id}")

# --- ENGINES ---

async def orchestration_decision_engine(bridge_client, validation_client):
    """The Master Controller: Acts based on current known state."""
    while True:
        try:
            for s_type in [bridge_type, validation_type]:
                info = assign_stuff_based_on_type(s_type)
                client = bridge_client if s_type == bridge_type else validation_client
                
                # Maintain Minimum (Self-Healing)
                if len(info['active_set']) < info['threshold']:
                    if info['idle_set']:
                        chosen = info['idle_set'].pop()
                        start_service(client, chosen, s_type, info['call_topic'])
                
                # Handle Reactive Scale Request
                elif scale_up_needed[s_type]:
                    if info['idle_set']:
                        chosen = info['idle_set'].pop()
                        start_service(client, chosen, s_type, info['call_topic'])
                        scale_up_needed[s_type] = False 
        except Exception as e:
            print(f"❌ Engine Error: {e}")
        await asyncio.sleep(2)

async def infrastructure_heartbeat(bridge_client, validation_client):
    """Zombie Hunter with Countdown: Wipes data at start of cycle and pings."""
    while True:
        try:
            print(f"\n🔄 [NEW CYCLE] Clearing old data and sending status ping...")
            
            # 1. Clear old messages/sets to only show fresh responses this cycle
            active_bridge_service_set.clear()
            idle_bridge_services_set.clear()
            active_validation_service_set.clear()
            idle_validation_services_set.clear()
            service_last_seen.clear()

            # 2. Trigger fresh sweep
            ping = json.dumps({"service_id": "ALL", "msg": "STATUS", "condition": None, "expected_reponse": True})
            bridge_client.publish(publish_topics["bridge_call_topic"], ping)
            validation_client.publish(publish_topics["validation_call_topic"], ping)

            # 3. Countdown loop (30 seconds)
            for remaining in range(30, 0, -1):
                # We print on one line to keep logs clean
                print(f"⏳ Time to next ping: {remaining}s | Active Bridges: {len(active_bridge_service_set)} | Active Val: {len(active_validation_service_set)}", end='\r')
                await asyncio.sleep(1)
            print() # Move to next line after countdown

        except Exception as e:
            print(f"❌ Heartbeat Error: {e}")
            await asyncio.sleep(5)

# --- MQTT HANDLERS ---

async def on_message_handler(client, topic, payload, qos, properties, queue):
    data = json.loads(payload.decode())
    await queue.put({"topic": topic, "data": data})

async def worker(queue):
    """Updates shared state based on incoming messages."""
    while True:
        item = await queue.get()
        topic, data = item['topic'], item['data']
        is_bridge = (topic == subscribe_topics['bridge_call_recv_topic'])
        m_type = bridge_type if is_bridge else validation_type
        
        update_active_services(data, m_type)
        
        if data.get('condition') == "OVERLOAD":
            scale_up_needed[m_type] = True
        
        queue.task_done()

# --- MAIN ---

def on_connect(client, flags, rc, properties, t_key):
    client.subscribe(subscribe_topics[t_key])
    print(f"✅ Connected to {t_key}")

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

    asyncio.create_task(worker(q))
    asyncio.create_task(infrastructure_heartbeat(bc, vc))
    asyncio.create_task(orchestration_decision_engine(bc, vc))

    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())