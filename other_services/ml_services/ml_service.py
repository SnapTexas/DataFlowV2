import asyncio
import json
import ssl
import socket
import uuid
import re
import os
from aiokafka import AIOKafkaConsumer
from gmqtt import Client as mqtt_client
import google.generativeai as genai
from dotenv import load_dotenv

# Load .env for local development
load_dotenv()

# --- CONFIGURATION FROM ENV ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092")
INPUT_TOPIC = os.getenv("TOPIC_VALIDATED", "validated-data")
ACTIONS_TOPIC = os.getenv("TOPIC_ACTIONS", "00989800/actions")

MQTT_HOST = os.getenv("MQTT_HOST", "31d09ce8b7fa4a92aafc62ae06187541.s1.eu.hivemq.cloud")
MQTT_USER = os.getenv("MQTT_USER", "Snappp")
MQTT_PASS = os.getenv("MQTT_PASS", "Snap00989800")

# Service Identity
SERVICE_ID = f"ML_BRAIN_{socket.gethostname()}_{uuid.uuid4().hex[:4]}"

# --- AI CONFIGURATION ---
GEMINI_COOLDOWN = int(os.getenv("GEMINI_COOLDOWN", 90))
MIN_BATCH_SIZE = int(os.getenv("MIN_BATCH_SIZE", 100))
MAX_BUFFER_SIZE = int(os.getenv("MAX_BUFFER_SIZE", 300))

# Initialize Gemini
if not GEMINI_API_KEY:
    raise ValueError("❌ GEMINI_API_KEY not found. Check your .env file.")

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.0-flash')

# Global state
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
    """Sends sensor data to Gemini and processes the response."""
    global next_allowed_call_time
    
    clean_data = compact_data(data_batch)
    print(f"🤖 [AI ANALYSIS] Sending {len(clean_data)} unique readings to Gemini...")
    
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
            wait_time = GEMINI_COOLDOWN
            match = re.search(r"retry in (\d+)", error_msg)
            if match:
                wait_time = int(match.group(1)) + 5
            print(f"⛔ [RATE LIMIT] Pausing for {wait_time}s.")
            next_allowed_call_time = asyncio.get_event_loop().time() + wait_time
        else:
            print(f"❌ [GEMINI ERROR] {e}")

async def ml_worker(mqtt_c):
    """Consumes Kafka data and manages time-gated batching."""
    global next_allowed_call_time
    data_buffer = []
    unique_group = f"ml-brain-{uuid.uuid4().hex[:6]}"

    while True:
        try:
            consumer = AIOKafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BROKERS,
                group_id=unique_group,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest' 
            )
            
            await consumer.start()
            print(f"✅ [KAFKA] Listening on {INPUT_TOPIC} via {KAFKA_BROKERS}")
            
            async for msg in consumer:
                data_buffer.append(msg.value)
                now = asyncio.get_event_loop().time()

                if len(data_buffer) >= MIN_BATCH_SIZE and now >= next_allowed_call_time:
                    print(f"🚀 [TRIGGER] Processing batch...")
                    batch_to_send = list(data_buffer)
                    data_buffer.clear() 
                    await ask_gemini_and_act(batch_to_send, mqtt_c)
                    next_allowed_call_time = asyncio.get_event_loop().time() + GEMINI_COOLDOWN
                
                elif len(data_buffer) > MAX_BUFFER_SIZE:
                    print(f"🗑️ [BUFFER FULL] API on cooldown. Dropping old data.")
                    data_buffer = data_buffer[-50:] 

        except Exception as e:
            print(f"⚠️ [KAFKA ERROR] {e}. Retrying in 10s...")
            await asyncio.sleep(10)
        finally:
            await consumer.stop()

async def main():
    print(f"🚀 Starting ML Brain Service: {SERVICE_ID}")
    
    ssl_ctx = ssl.create_default_context()
    mqtt_c = mqtt_client(SERVICE_ID)
    mqtt_c.set_auth_credentials(MQTT_USER, MQTT_PASS)
    
    try:
        print(f"🔗 [MQTT] Connecting to {MQTT_HOST}...")
        await mqtt_c.connect(MQTT_HOST, 8883, ssl=ssl_ctx)
        await ml_worker(mqtt_c)
    except Exception as e:
        print(f"💀 [FATAL ERROR] {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 ML Service stopped.")