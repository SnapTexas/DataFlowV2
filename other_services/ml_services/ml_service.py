import asyncio
import json
import ssl
import os
import socket
from aiokafka import AIOKafkaConsumer
from gmqtt import Client as mqtt_client
import google.generativeai as genai

# --- CONFIGURATION ---
# IMPORTANT: Delete this key from Google AI Studio and generate a new one after this!
GEMINI_API_KEY = "AIzaSyCnOMeh7iof_MO2mm03Sed28VGi-bij6qk"
BOOTSTRAP_SERVERS = 'kafka-1:9092,kafka-2:9092'
INPUT_TOPIC = 'validated-data'
ACTIONS_TOPIC = "00989800/actions"
SERVICE_ID = f"ML_BRAIN_{socket.gethostname()}"

# SPAM PROTECTION SETTINGS
GEMINI_COOLDOWN = 20  # Minimum seconds between AI calls
MIN_BATCH_SIZE = 30   # Minimum messages needed to trigger analysis

# Initialize Gemini
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.0-flash')

# Registry to track the last time we asked the AI
last_gemini_call_time = 0

async def heartbeat():
    """Background task to show the service is alive in the logs."""
    while True:
        print(f"💓 [HEARTBEAT] {SERVICE_ID} monitoring Kafka...")
        await asyncio.sleep(60)

async def ask_gemini_and_act(data_batch, mqtt_c):
    """Sends a batch of sensor data to Gemini and publishes the resulting action."""
    print(f"🤖 [AI ANALYSIS] Processing batch of {len(data_batch)} readings...")
    
    prompt = f"""
    You are an IoT System Controller. Analyze this batch of validated sensor data:
    {data_batch}

    Rules:
    1. If values are abnormal or dangerous, choose ONE action: [Alert, Task1, Task2].
    2. If everything is within normal operating parameters, reply 'NONE'.
    3. Reply ONLY with the single word (no punctuation or explanation).
    """
    
    try:
        # to_thread keeps the internal asyncio loop from blocking during the API request
        response = await asyncio.to_thread(model.generate_content, prompt)
        
        # Clean and validate response
        action = response.text.strip().replace('*', '')
        allowed_actions = ["Alert", "Task1", "Task2"]
        
        if action in allowed_actions:
            print(f"🚨 [AI DECISION] Action Required: {action}. Publishing to {ACTIONS_TOPIC}")
            mqtt_c.publish(ACTIONS_TOPIC, action)
        else:
            print(f"✅ [AI OBSERVATION] Status: Normal (Gemini said: {action})")
            
    except Exception as e:
        print(f"❌ [GEMINI ERROR] API call failed: {e}")

async def ml_worker(mqtt_c):
    """Consumes data from Kafka and manages the time-gated batching logic."""
    global last_gemini_call_time
    asyncio.create_task(heartbeat())
    
    data_buffer = []

    while True:
        try:
            consumer = AIOKafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id="ml-brain-group",
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest'
            )
            
            await consumer.start()
            print(f"✅ [KAFKA] Connected. Watching topic: {INPUT_TOPIC}")
            
            async for msg in consumer:
                data_buffer.append(msg.value)
                
                current_time = asyncio.get_event_loop().time()
                time_since_last_call = current_time - last_gemini_call_time
                
                # Check if we meet both Volume (30 msgs) and Time (20s) requirements
                if len(data_buffer) >= MIN_BATCH_SIZE:
                    if time_since_last_call >= GEMINI_COOLDOWN:
                        # TRIGGER AI
                        last_gemini_call_time = current_time
                        
                        # Create task with a copy of the buffer and clear the original
                        asyncio.create_task(ask_gemini_and_act(list(data_buffer), mqtt_c))
                        data_buffer.clear()
                    else:
                        # Log status occasionally so we know it's still working
                        if len(data_buffer) % 50 == 0:
                            wait_remaining = int(GEMINI_COOLDOWN - time_since_last_call)
                            print(f"⏳ [BUFFERING] {len(data_buffer)} msgs in queue. Cooldown: {wait_remaining}s left.")

        except Exception as e:
            print(f"⚠️ [KAFKA ERROR] {e}. Retrying in 10s...")
            await asyncio.sleep(10)
        finally:
            # Ensure consumer is closed before attempting restart
            try:
                await consumer.stop()
            except:
                pass

async def main():
    """Main entry point: Connects to MQTT and starts the ML worker."""
    print(f"🚀 Starting ML Brain Service: {SERVICE_ID}")
    
    ssl_ctx = ssl.create_default_context()
    mqtt_c = mqtt_client(SERVICE_ID)
    mqtt_c.set_auth_credentials("Snappp", "Snap00989800")
    
    try:
        print("🔗 [MQTT] Connecting to HiveMQ Cloud...")
        await mqtt_c.connect(
            "31d09ce8b7fa4a92aafc62ae06187541.s1.eu.hivemq.cloud", 
            8883, 
            ssl=ssl_ctx
        )
        
        # Start worker
        await ml_worker(mqtt_c)
        
    except Exception as e:
        print(f"💀 [FATAL ERROR] {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 ML Service stopped.")