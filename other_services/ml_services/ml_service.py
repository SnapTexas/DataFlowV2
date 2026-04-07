import asyncio
import json
import ssl
import socket
import uuid
import re
import os
import time
from aiokafka import AIOKafkaConsumer
from gmqtt import Client as mqtt_client
import google.generativeai as genai
from dotenv import load_dotenv

# --- LOAD ENV ---
load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092")
INPUT_TOPIC = os.getenv("TOPIC_VALIDATED", "validated-data")
ACTIONS_TOPIC = os.getenv("TOPIC_ACTIONS", "00989800/actions")

MQTT_HOST = os.getenv("MQTT_HOST")
MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASS = os.getenv("MQTT_PASS")

# --- SERVICE ID ---
SERVICE_ID = f"ML_BRAIN_{socket.gethostname()}"

# --- AI CONFIG ---
MIN_BATCH_SIZE = 30
MAX_BUFFER_SIZE = 300

# Strong rate limiting
MIN_API_INTERVAL = 120  # seconds (2 min hard gap)
last_api_call_time = 0
next_allowed_call_time = 0

# --- INIT GEMINI ---
if not GEMINI_API_KEY:
    raise ValueError("❌ GEMINI_API_KEY missing")

genai.configure(api_key=GEMINI_API_KEY)

for m in genai.list_models():
    if "generateContent" in m.supported_generation_methods:
        print(m.name)

#model = genai.GenerativeModel('gemini-2.0-flash')
model = genai.GenerativeModel('gemini-2.5-flash')


# -------------------------------
# 🔧 UTIL FUNCTIONS
# -------------------------------
def compact_data(data_batch):
    """Remove duplicate consecutive readings."""
    if not data_batch:
        return []
    compacted = [data_batch[0]]
    for i in range(1, len(data_batch)):
        if data_batch[i] != data_batch[i - 1]:
            compacted.append(data_batch[i])
    return compacted


def is_data_significant(data_batch):
    """Avoid calling AI if data has little variation."""
    unique = len(set(map(str, data_batch)))
    return unique > 10  # threshold


# -------------------------------
# 🤖 GEMINI CALL
# -------------------------------
async def ask_gemini_and_act(data_batch, mqtt_c):
    global last_api_call_time, next_allowed_call_time

    now = time.time()

    # HARD RATE LIMIT
    if now - last_api_call_time < MIN_API_INTERVAL:
        print("⏳ [SKIP] API interval not reached")
        return

    clean_data = compact_data(data_batch)
    print(len(clean_data))
    clean_data=clean_data[:30]
    if not is_data_significant(clean_data):
        print(len(clean_data))
        print("⚠️ [SKIP] Data not significant")
        return

    print(f"🤖 [AI] Sending {len(clean_data)} readings")
    print(len(clean_data))
    prompt = f"""
    Analyze this batch of IoT sensor data: {clean_data}
    Alert Notifies the user 
    Task1 raises temprature
    Task2 reduces temprature
    Normal Temp (1 < temp < 100)
    Rules:
    - If abnormal/dangerous → reply ONLY: Alert, Task1, or Task2  
    - If normal → reply ONLY: NONE
    - Strictly one word
    """

    try:
        print(f"📡 [API CALL] {time.strftime('%X')}")
        # response = await asyncio.to_thread(
        # model.generate_content,
        # "Say OK"
        # )
        
        response = await asyncio.to_thread(model.generate_content, prompt)
        action = response.text.strip().replace('*', '').replace('.', '')
        print("RESPONSE:", response.text)
        last_api_call_time = time.time()

        if action in ["Alert", "Task1", "Task2"]:
            print(f"🚨 ACTION: {action}")
            mqtt_c.publish(ACTIONS_TOPIC, action)
        else:
            print("✅ NORMAL")

    except Exception as e:
        next_allowed_call_time = time.time() + 60
        error_msg = str(e)

        if "429" in error_msg:
            wait_time = 120
            match = re.search(r"retry in (\d+)", error_msg)

            if match:
                #wait_time = int(match.group(1)) + 5
                pass
            print(f"⛔ RATE LIMITED: waiting {wait_time}s")
            #next_allowed_call_time = time.time() + wait_time

        else:
            print(f"❌ GEMINI ERROR: {e}")


# -------------------------------
# 📥 KAFKA WORKER
# -------------------------------
async def ml_worker(mqtt_c):
    global next_allowed_call_time

    data_buffer = []

    consumer = AIOKafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        group_id="ml-brain-group",  # ✅ FIXED GROUP
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )

    await consumer.start()
    print(f"✅ Kafka connected: {INPUT_TOPIC}")

    try:
        async for msg in consumer:
            data_buffer.append(msg.value)

            now = time.time()

            # Trigger condition
            if (
                len(data_buffer) >= MIN_BATCH_SIZE
                and now >= next_allowed_call_time
            ):
                print("🚀 Processing batch")

                batch = list(data_buffer)
                data_buffer.clear()

                await ask_gemini_and_act(batch, mqtt_c)

            # Prevent memory overflow
            if len(data_buffer) > MAX_BUFFER_SIZE:
                print("🗑️ Buffer overflow → trimming")
                data_buffer = data_buffer[-50:]

    except Exception as e:
        print(f"⚠️ Kafka error: {e}")

    finally:
        await consumer.stop()


# -------------------------------
# 🔗 MAIN
# -------------------------------
async def main():
    print(f"🚀 ML Brain Started: {SERVICE_ID}")

    ssl_ctx = ssl.create_default_context()

    mqtt_c = mqtt_client(SERVICE_ID)
    mqtt_c.set_auth_credentials(MQTT_USER, MQTT_PASS)

    try:
        print("🔗 Connecting MQTT...")
        await mqtt_c.connect(MQTT_HOST, 8883, ssl=ssl_ctx)

        await ml_worker(mqtt_c)

    except Exception as e:
        print(f"💀 FATAL ERROR: {e}")


# -------------------------------
# ▶ RUN
# -------------------------------
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Stopped")