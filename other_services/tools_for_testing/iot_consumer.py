import asyncio
import uuid
import json
from gmqtt import Client as MQTTClient
from functools import partial

# Topic Structure: $share/GROUP_NAME/TOPIC
# The broker will distribute messages among all scripts using this group name
TOPIC_TO_CONSUME = "iot-data-pipeline-v2/00989800/ml-report"

def on_connect(client, flags, rc, properties):
    print(f"✅ Consumer Connected (RC: {rc})")
    client.subscribe(TOPIC_TO_CONSUME, qos=1)
    print(f"📡 Ingesting from: {TOPIC_TO_CONSUME}")

async def on_message(client, topic, payload, qos, properties):
    try:
        # 1. Decode payload
        raw_data = payload.decode('utf-8')
        
        # 2. Parse if JSON (optional, but common for IoT)
        try:
            data = json.loads(raw_data)
        except json.JSONDecodeError:
            data = raw_data
            
        # 3. Handle your data here (save to DB, process, etc.)
        print(f"📥 Processed Data: {data}")

    except Exception as e:
        print(f"⚠️ Ingestion Error: {e}")

async def main():
    # Use a persistent ID so the broker can track session if needed
    client = MQTTClient(f"consumer-{uuid.uuid4().hex[:4]}")

    client.on_connect = on_connect
    client.on_message = on_message

    # Connect to HiveMQ public broker
    await client.connect("broker.hivemq.com", port=1883)

    # Infinite loop to keep consuming
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nConsumer stopped.")