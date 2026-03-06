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

# Timestamps to detect crashed containers
service_last_seen = {} 

# Scaling "Switches" - Flipped by messages, consumed by the Engine
scale_up_needed = {"bridge": False, "validation": False}

# --- HELPERS ---

def assign_stuff_based_on_type(service_type):
    if service_type == bridge_type:
        return {
            "active_set": active_bridge_service_set,
            "idle_set": idle_bridge_services_set,
            "threshold": thresh_bridge_services,
            "call_topic": publish_topics["bridge_call_topic"],
            "client_key": "bridge"
        }
    else:
        return {
            "active_set": active_validation_service_set,
            "idle_set": idle_validation_services_set,
            "threshold": thresh_validation_services,
            "call_topic": publish_topics["validation_call_topic"],
            "client_key": "validation"
        }

def update_active_services(service_data, service_type):
    global service_last_seen
    data = assign_stuff_based_on_type(service_type)
    active_set, idle_set = data['active_set'], data['idle_set']
    
    s_id = service_data.get("service_id")
    status = service_data.get("status")
    condition = service_data.get("condition")

    # Mark as alive
    service_last_seen[s_id] = time.time()

    # If it's running, we definitely don't need a scale-up anymore
    if status == "RUNNING":
        scale_up_needed[service_type] = False

    # Logical Mapping
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

def stop_service(client, service_id, call_topic):
    msg = json.dumps({"service_id": service_id, "msg": "IDLE", "condition": None, "expected_reponse": False})
    client.publish(call_topic, msg)
    print(f"💤 [ACTION] Scaling DOWN: Sending {service_id} to IDLE")

# --- CONTROL ENGINES ---

async def orchestration_decision_engine(bridge_client, validation_client):
    """
    The Master Controller. 
    Checks thresholds and scale-up requests every 2 seconds.
    """
    print("🧠 Orchestration Engine Online (Control Loop Active)")
    while True:
        try:
            for s_type in [bridge_type, validation_type]:
                info = assign_stuff_based_on_type(s_type)
                client = bridge_client if s_type == bridge_type else validation_client
                
                active_count = len(info['active_set'])
                
                # 1. Maintain Minimum (Self-Healing)
                if active_count < info['threshold']:
                    if info['idle_set']:
                        chosen = info['idle_set'].pop()
                        start_service(client, chosen, s_type, info['call_topic'])
                
                # 2. Handle Reactive Scale Request (Overload)
                elif scale_up_needed[s_type]:
                    if info['idle_set']:
                        chosen = info['idle_set'].pop()
                        start_service(client, chosen, s_type, info['call_topic'])
                        # Turn off switch so we don't start a second one immediately
                        scale_up_needed[s_type] = False 
                
                # 3. Handle Scale Down (Optional: Only if condition is NORMAL for all)
                # We keep this conservative to avoid flickering.

        except Exception as e:
            print(f"❌ Engine Error: {e}")
        await asyncio.sleep(2)

async def infrastructure_heartbeat(bridge_client, validation_client):
    """Zombie Hunter: Removes services that crashed (stopped talking)."""
    while True:
        try:
            now = time.time()
            dead_ids = [s_id for s_id, last_t in service_last_seen.items() if (now - last_t) > 45]
            
            for s_id in dead_ids:
                print(f"💀 [PRUNE] Service {s_id} timed out. Removing from Sets.")
                for s in [active_bridge_service_set, idle_bridge_services_set, 
                          active_validation_service_set, idle_validation_services_set]:
                    s.discard(s_id)
                service_last_seen.pop(s_id)

            # Refresh Status Ping
            ping = json.dumps({"service_id": "ALL", "msg": "STATUS", "condition": None, "expected_reponse": True})
            bridge_client.publish(publish_topics["bridge_call_topic"], ping)
            validation_client.publish(publish_topics["validation_call_topic"], ping)
            
        except Exception as e:
            print(f"❌ Heartbeat Error: {e}")
        await asyncio.sleep(30)

# --- MQTT CALLBACKS ---

async def on_message_handler(client, topic, payload, qos, properties, queue):
    """Simply puts messages into the queue. No decisions here."""
    data = json.loads(payload.decode())
    await queue.put({"topic": topic, "data": data})

async def worker(queue):
    """Processes the queue to update the SHARED STATE only."""
    while True:
        item = await queue.get()
        topic, data = item['topic'], item['data']
        
        is_bridge = (topic == subscribe_topics['bridge_call_recv_topic'])
        m_type = bridge_type if is_bridge else validation_type
        
        # 1. Update our memory of the world
        update_active_services(data, m_type)
        
        # 2. If Overload, flip the switch for the Decision Engine
        if data.get('condition') == "OVERLOAD":
            scale_up_needed[m_type] = True

        # 3. Print Dashboard log
        print(f"📊 [STATE] Bridges: {len(active_bridge_service_set)}A/{len(idle_bridge_services_set)}I | "
              f"Val: {len(active_validation_service_set)}A/{len(idle_validation_services_set)}I")
        
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

    # Start independent tasks
    asyncio.create_task(worker(q))
    asyncio.create_task(infrastructure_heartbeat(bc, vc))
    asyncio.create_task(orchestration_decision_engine(bc, vc))

    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())