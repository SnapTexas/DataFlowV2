import asyncio
import json
import ssl
import os
import socket
import uuid
import re
from aiokafka import AIOKafkaConsumer
from gmqtt import Client as mqtt_client
import google.generativeai as genai

# --- CONFIGURATION ---
GEMINI_API_KEY = "AIzaSyCnOMeh7iof_MO2mm03Sed28VGi-bij6qk"
BOOTSTRAP_SERVERS = 'kafka-1:9092,kafka-2:9092'
INPUT_TOPIC = 'validated-data'
ACTIONS_TOPIC = "00989800/actions"
SERVICE_ID = f"ML_BRAIN_{socket.gethostname()}"

# --- SPAM & QUOTA PROTECTION ---
GEMINI_COOLDOWN = 60  # Base cooldown
MIN_BATCH_SIZE = 40   # Trigger threshold
MAX_BUFFER_SIZE = 200 # Prevent bloat during 429 errors

# Initialize Gemini
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.0-flash')

# This will now track the "Next Allowed Call Time"
next_allowed_call_time = 0

def compact_data(data_batch):
    """Reduces token usage by removing redundant sequential readings."""
    if not data_batch: return []
    compacted = [data_batch[0]]
    for i in range(1, len(data_batch)):
        if data_batch[i] != data_batch[i-1]:
            compacted.append(data_batch[i])
    return compacted

async def ask_gemini_and_act(data_batch, mqtt_c):
    """Sends sensor data to Gemini with dynamic retry-after handling."""
    global next_allowed_call_time
    
    clean_data = compact_data(data_batch)
    print(f"🤖 [AI ANALYSIS] Sending {len(clean_data)} readings to Gemini...")
    
    prompt = f"""
    Analyze this batch of IoT sensor data: {clean_data}
    Rules:
    - If abnormal/dangerous, reply ONLY: Alert, Task1, or Task2.
    - If normal, reply ONLY: NONE.
    - Strictly one word only.
    """
    
    try:
        response = await asyncio.to_thread(model.generate_content, prompt)
        action = response.text.strip().replace('*', '').replace('.', '')
        
        if action in ["Alert", "Task1", "Task2"]:
            print(f"🚨 [AI DECISION] {action} required!")
            mqtt_c.publish(ACTIONS_TOPIC, action)
        else:
            print(f"✅ [AI OBSERVATION] Status: Normal")
            
    except Exception as e:
        error_msg = str(e)
        if "429" in error_msg:
            # Parse Google's 'retry in X seconds' message
            wait_time = GEMINI_COOLDOWN
            match = re.search(r"retry in (\d+)", error_msg)
            if match:
                wait_time = int(match.group(1)) + 5
            
            print(f"⛔ [RATE LIMIT] Google requested {wait_time}s pause.")
            next_allowed_call_time = asyncio.get_event_loop().time() + wait_time
        else:
            print(f"❌ [GEMINI ERROR] {e}")

async def ml_worker(mqtt_c):
    """Consumes Kafka data and manages time-gated batching with anti-bloat."""
    global next_allowed_call_time
    data_buffer = []
    unique_group = f"ml-brain-{uuid.uuid4().hex[:6]}"

    while True:
        try:
            consumer = AIOKafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id=unique_group,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest' # Ignore old backlog to avoid instant 429s
            )
            
            await consumer.start()
            print(f"✅ [KAFKA] Connected. Watching: {INPUT_TOPIC}")
            
            async for msg in consumer:
                data_buffer.append(msg.value)
                now = asyncio.get_event_loop().time()

                # Trigger Logic
                if len(data_buffer) >= MIN_BATCH_SIZE:
                    if now >= next_allowed_call_time:
                        print(f"🚀 [TRIGGER] Batch size {len(data_buffer)} reached. Calling AI.")
                        batch_to_send = list(data_buffer)
                        data_buffer.clear() # Clear immediately to prevent re-sending
                        
                        # Set next cooldown
                        next_allowed_call_time = now + GEMINI_COOLDOWN
                        asyncio.create_task(ask_gemini_and_act(batch_to_send, mqtt_c))
                    else:
                        # ANTI-BLOAT: If API is on cooldown, don't let buffer grow forever
                        if len(data_buffer) > MAX_BUFFER_SIZE:
                            print(f"🗑️ [BUFFER FULL] API on cooldown. Dropping old data. Buffer: {len(data_buffer)}")
                            data_buffer = data_buffer[-50:] # Keep only the latest 50
                        
                        if len(data_buffer) % 20 == 0:
                            wait_rem = int(next_allowed_call_time - now)
                            print(f"⏳ [COOLDOWN] Waiting {wait_rem}s more. Buffer: {len(data_buffer)}")

        except Exception as e:
            print(f"⚠️ [KAFKA ERROR] {e}. Retrying in 10s...")
            await asyncio.sleep(10)
        finally:
            await consumer.stop()

async def main():
    print(f"🚀 Starting ML Brain Service: {SERVICE_ID}")
    
    ssl_ctx = ssl.create_default_context()
    mqtt_c = mqtt_client(SERVICE_ID)
    mqtt_c.set_auth_credentials("Snappp", "Snap00989800")
    
    try:
        print("🔗 [MQTT] Connecting to HiveMQ Cloud...")
        await mqtt_c.connect("31d09ce8b7fa4a92aafc62ae06187541.s1.eu.hivemq.cloud", 8883, ssl=ssl_ctx)
        await ml_worker(mqtt_c)
    except Exception as e:
        print(f"💀 [FATAL ERROR] {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 ML Service stopped.")