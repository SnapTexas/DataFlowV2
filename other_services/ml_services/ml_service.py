import asyncio
import json
import ssl
import os
import socket
from aiokafka import AIOKafkaConsumer
from gmqtt import Client as mqtt_client
import google.generativeai as genai
# Load API Key from .env file

GEMINI_API_KEY = "AIzaSyCnOMeh7iof_MO2mm03Sed28VGi-bij6qk"

# --- CONFIGURATION ---
BOOTSTRAP_SERVERS = 'kafka-1:9092,kafka-2:9092'
INPUT_TOPIC = 'validated-data'
ACTIONS_TOPIC = "00989800/actions"
SERVICE_ID = f"ML_BRAIN_{socket.gethostname()}"

# Initialize Gemini
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.0-flash')

async def heartbeat():
    """Background task to show the service is alive in the logs."""
    while True:
        print(f"💓 [HEARTBEAT] {SERVICE_ID} is active and monitoring Kafka...")
        await asyncio.sleep(60)

async def ask_gemini_and_act(data_batch, mqtt_c):
    """Sends a batch of sensor data to Gemini and publishes the resulting action."""
    prompt = f"""
    You are an IoT System Controller. Analyze this batch of validated sensor data:
    {data_batch}

    Rules:
    1. If values are abnormal or dangerous, choose ONE action: [Alert, Task1, Task2].
    2. If everything is within normal operating parameters, reply 'NONE'.
    3. Reply ONLY with the single word (no punctuation or explanation).
    """
    
    try:
        # Run synchronous Gemini call in a separate thread to keep the loop moving
        response = await asyncio.to_thread(model.generate_content, prompt)
        
        # Clean the response (removes potential markdown like **Alert**)
        action = response.text.strip().replace('*', '')
        
        allowed_actions = ["Alert", "Task1", "Task2"]
        
        if action in allowed_actions:
            print(f"🧠 [AI DECISION] Publishing {action} to {ACTIONS_TOPIC}")
            print(f"🧠 [AI DECISION] Action Required: {action}")
            mqtt_c.publish(ACTIONS_TOPIC, action)
        else:
            print(f"☁️ [AI OBSERVATION] Status: Normal (Gemini said: {action})")
            
    except Exception as e:
        print(f"❌ [GEMINI ERROR] Failed to get AI decision: {e}")

async def ml_worker(mqtt_c):
    """Consumes data from Kafka and manages the batching logic."""
    # Start the heartbeat in the background
    asyncio.create_task(heartbeat())
    
    while True: # Persistence loop for Kafka connection
        try:
            consumer = AIOKafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id="ml-brain-group",
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest'
            )
            
            await consumer.start()
            print(f"✅ [KAFKA] Connected to cluster. Watching topic: {INPUT_TOPIC}")
            
            data_buffer = []
            
            async for msg in consumer:
                data_buffer.append(msg.value)
                
                # Process every 5 messages
                if len(data_buffer) >= 30:
                    print(f"🤖 [BATCH READY] Sending 5 messages to Gemini for analysis...")
                    # Create task for AI so the consumer can keep receiving new data
                    asyncio.create_task(ask_gemini_and_act(list(data_buffer), mqtt_c))
                    data_buffer.clear()

        except Exception as e:
            print(f"⚠️ [KAFKA ERROR] Connection lost: {e}. Retrying in 10s...")
            await asyncio.sleep(10)
        finally:
            await consumer.stop()

async def main():
    """Main entry point: Connects to MQTT and starts the ML worker."""
    print(f"🚀 Starting ML Brain Service: {SERVICE_ID}")
    
    # MQTT Setup (for sending commands to Actions Service)
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
        
        # Start the worker (this function blocks, keeping the service alive)
        await ml_worker(mqtt_c)
        
    except Exception as e:
        print(f"💀 [FATAL ERROR] Service crashed: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 ML Service stopped manually by user.")