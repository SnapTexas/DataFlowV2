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

service_last_seen = {}  # Tracks {service_id: timestamp} for pruning

# --- HELPERS ---

def assign_stuff_based_on_type(service_type):
    if service_type == bridge_type:
        return {
            "active_set": active_bridge_service_set,
            "idle_set": idle_bridge_services_set,
            "threshold": thresh_bridge_services,
            "call_topic": publish_topics["bridge_call_topic"]
        }
    elif service_type == validation_type:
        return {
            "active_set": active_validation_service_set,
            "idle_set": idle_validation_services_set,
            "threshold": thresh_validation_services,
            "call_topic": publish_topics["validation_call_topic"]
        }
    raise Exception(f"Invalid service type: {service_type}")

def update_active_services(service_data, service_type):
    global service_last_seen
    data = assign_stuff_based_on_type(service_type)
    active_set, idle_set = data['active_set'], data['idle_set']
    
    s_id = service_data.get("service_id")
    status = service_data.get("status")
    condition = service_data.get("condition")

    # Mark as seen
    service_last_seen[s_id] = time.time()

    # Rule: Overload or Running always belongs in Active Set
    if condition == "OVERLOAD" or status == "RUNNING":
        active_set.add(s_id)
        idle_set.discard(s_id)
    elif status == "IDLE":
        active_set.discard(s_id)
        idle_set.add(s_id)

def choose_service_start(idle_set, service_type):
    if not idle_set: return None
    return idle_set.pop()

def choose_service_stop(active_set):
    if not active_set: return None
    # Professional touch: return the most recently seen (or random) to keep logic simple
    return next(iter(active_set))

# --- ACTIONS ---

def start_service(client, service_id, service_type):
    data = assign_stuff_based_on_type(service_type)
    msg = json.dumps({"service_id": service_id, "msg": "START", "condition": None, "expected_reponse": False})
    client.publish(data['call_topic'], msg)
    print(f"🚀 [ACTION] Sent START to {service_type}: {service_id}")

async def stop_service_procedure(client, service_type):
    data = assign_stuff_based_on_type(service_type)
    active_set = data['active_set']
    if len(active_set) > data['threshold']:
        chosen_id = choose_service_stop(active_set)
        if chosen_id:
            msg = json.dumps({"service_id": chosen_id, "msg": "IDLE", "condition": None, "expected_reponse": False})
            client.publish(data['call_topic'], msg)
            print(f"💤 [ACTION] Sent IDLE to {service_type}: {chosen_id}")

# --- ENGINES (The Core Logic) ---

async def orchestration_decision_engine(bridge_client, validation_client):
    """The Control Loop: Constantly ensures Current State matches Desired State."""
    print("🧠 Orchestration Decision Engine is online...")
    while True:
        try:
            # Check Bridge Minimums
            if len(active_bridge_service_set) < thresh_bridge_services:
                chosen = choose_service_start(idle_bridge_services_set, bridge_type)
                if chosen: start_service(bridge_client, chosen, bridge_type)

            # Check Validation Minimums
            if len(active_validation_service_set) < thresh_validation_services:
                chosen = choose_service_start(idle_validation_services_set, validation_type)
                if chosen: start_service(validation_client, chosen, validation_type)

        except Exception as e:
            print(f"❌ Decision Engine Error: {e}")
        await asyncio.sleep(2)  # High-frequency check

async def infrastructure_heartbeat(bridge_client, validation_client):
    """Health Check: Sends status pings and prunes dead (Zombie) containers."""
    while True:
        try:
            now = time.time()
            # Prune services not seen for 45 seconds (Zombie detection)
            dead_ids = [s_id for s_id, last_t in service_last_seen.items() if (now - last_t) > 45]
            for s_id in dead_ids:
                print(f"💀 [PRUNE] Removing crashed service: {s_id}")
                for s in [active_bridge_service_set, idle_bridge_services_set, 
                          active_validation_service_set, idle_validation_services_set]:
                    s.discard(s_id)
                service_last_seen.pop(s_id)

            # Trigger fresh sweep
            payload = json.dumps({"service_id": "ALL", "msg": "STATUS", "condition": None, "expected_reponse": True})
            bridge_client.publish(publish_topics["bridge_call_topic"], payload)
            validation_client.publish(publish_topics["validation_call_topic"], payload)
            
        except Exception as e:
            print(f"❌ Heartbeat Error: {e}")
        await asyncio.sleep(30)

# --- MQTT HANDLERS ---

async def respond_msg(bridge_client, validation_client, topic, data):
    is_bridge = (topic == subscribe_topics['bridge_call_recv_topic'])
    m_type = bridge_type if is_bridge else validation_type
    m_client = bridge_client if is_bridge else validation_client
    
    # 1. Update State
    update_active_services(data, m_type)

    # 2. Reactive Scaling (Speed)
    if data.get('condition') == "OVERLOAD":
        target_idle = idle_bridge_services_set if is_bridge else idle_validation_services_set
        chosen = choose_service_start(target_idle, m_type)
        if chosen: start_service(m_client, chosen, m_type)
        
    elif data.get('condition') == "NORMAL" and data.get('status') == "RUNNING":
        await stop_service_procedure(m_client, m_type)

async def worker(bridge_client, validation_client, queue):
    while True:
        try:
            raw_msg = await queue.get()
            msg = json.loads(raw_msg)
            await respond_msg(bridge_client, validation_client, msg['topic'], msg['data'])
            
            # Simplified Log for Dashboard
            print(f"📊 Bridges: {len(active_bridge_service_set)}A/{len(idle_bridge_services_set)}I | "
                  f"Validations: {len(active_validation_service_set)}A/{len(idle_validation_services_set)}I")
        except Exception as e:
            print(f"❌ Worker Error: {e}")
        finally:
            queue.task_done()

# --- CONNECTION CALLBACKS ---

def on_connect(client, flags, rc, properties, topic_key):
    client.subscribe(subscribe_topics[topic_key])
    print(f"✅ Manager connected to {topic_key}")

async def on_message(client, topic, payload, qos, properties, queue):
    await queue.put(json.dumps({"topic": topic, "data": json.loads(payload.decode())}))

# --- MAIN ---

async def sync_infrastructure_and_boot(bc, vc):
    print("⏳ Synchronizing Infrastructure...")
    await asyncio.sleep(5)  # Give time for initial check-ins

async def main():
    incomming_calls_queue = asyncio.Queue()
    host, port = "31d09ce8b7fa4a92aafc62ae06187541.s1.eu.hivemq.cloud", 8883
    username, password = "Snappp", "Snap00989800"
    ssl_ctx = ssl.create_default_context()

    # Bridge Client
    bridge_client = mqtt(str(uuid.uuid4()))
    bridge_client.set_auth_credentials(username, password)
    bridge_client.on_connect = partial(on_connect, topic_key="bridge_call_recv_topic")
    bridge_client.on_message = partial(on_message, queue=incomming_calls_queue)
    await bridge_client.connect(host, port, ssl=ssl_ctx)

    # Validation Client
    validation_client = mqtt(str(uuid.uuid4()))
    validation_client.set_auth_credentials(username, password)
    validation_client.on_connect = partial(on_connect, topic_key="validation_call_recv_topic")
    validation_client.on_message = partial(on_message, queue=incomming_calls_queue)
    await validation_client.connect(host, port, ssl=ssl_ctx)

    # Start Tasks
    asyncio.create_task(worker(bridge_client, validation_client, incomming_calls_queue))
    asyncio.create_task(infrastructure_heartbeat(bridge_client, validation_client))
    asyncio.create_task(orchestration_decision_engine(bridge_client, validation_client))

    await sync_infrastructure_and_boot(bridge_client, validation_client)
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())