import asyncio
import uuid
import ssl
import json
import os
import random
from gmqtt import Client as mqtt_client
from dotenv import load_dotenv  # Import dotenv

# Load variables from .env into system environment
load_dotenv() 

# -------- CONFIGURATION FROM ENV --------
# Now os.getenv will look in your .env file first
BROKER = os.getenv("MQTT_BROKER")
PORT = int(os.getenv("MQTT_PORT", 8883))
USERNAME = os.getenv("MQTT_USER")
PASSWORD = os.getenv("MQTT_PASS")

MANAGER_TOPIC = os.getenv("TOPIC_MANAGER")
DATA_TOPIC = os.getenv("TOPIC_DATA")

TOPICS = {
    1: MANAGER_TOPIC,
    2: DATA_TOPIC,
}

# -------- PREDEFINED PAYLOADS --------
PAYLOADS = {
    1: {"service_id": "ALL", "msg": "START", "condition": None, "expected_reponse": False},
    2: {"service_id": "ALL", "msg": "IDLE", "condition": None, "expected_reponse": False},
    3: {"temp": 0}, # Placeholder for random number
    4: {"service_id": "ALL", "msg": "STATUS", "condition": None, "expected_reponse": False},
    5: {"temp":300}
}

async def publisher_loop(client):
    while True:
        try:
            print("\n====== MQTT PUBLISHER MENU ======")
            print("Select Topic:")
            for key, value in TOPICS.items():
                print(f"{key}. {value}")

            topic_choice = int(input("Choose topic number: "))
            topic = TOPICS.get(topic_choice)
            if not topic:
                print("❌ Invalid choice")
                continue

            print("\nSelect Payload:")
            for key, value in PAYLOADS.items():
                print(f"{key}. {value}")

            payload_choice = int(input("Choose payload number: "))
            if payload_choice not in PAYLOADS:
                print("❌ Invalid choice")
                continue

            rate = float(input("\nEnter publish rate (messages/sec): "))
            delay = 1.0 / rate

            print(f"\n🚀 Publishing to '{topic}' at {rate} msg/sec")
            print("Press Ctrl+C to stop this stream and return to menu...\n")

            # ---- Internal Publishing Loop ----
            while True:
                # Get a fresh copy of the payload template
                current_payload = PAYLOADS[payload_choice].copy()
                
                # If it's the temperature payload, update with random number
                if "temp" in current_payload and payload_choice==3:
                    print("Normal")
                    current_payload["temp"] = random.randint(1, 100)
                elif "temp" in current_payload and payload_choice==5:
                    print("abnormal")
                    current_payload["temp"] = random.randint(300,400)
                if client.is_connected:
                    payload_json = json.dumps(current_payload)
                    client.publish(topic, payload_json, qos=0)
                    print(f"Sent → {payload_json}")
                else:
                    print("⚠️ Client not connected. Attempting to skip...")

                await asyncio.sleep(delay)

        except KeyboardInterrupt:
            print("\n🛑 Stream stopped. Returning to menu...")
            await asyncio.sleep(0.5) # Small buffer to clear input
            continue
        except ValueError:
            print("❌ Please enter a valid number.")
            continue

async def main():
    ssl_ctx = ssl.create_default_context()
    client = mqtt_client(client_id=str(uuid.uuid4()))
    client.set_auth_credentials(USERNAME, PASSWORD)

    print(f"Connecting to {BROKER}...")
    try:
        await client.connect(BROKER, PORT, ssl=ssl_ctx, keepalive=30)
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return

    print("✅ Connected to broker.")
    await publisher_loop(client)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting program.")




        """
create table public.sensor_data (
  key text not null,
  value text null,
  constraint sensor_data_pkey primary key (key)
) TABLESPACE pg_default;
"""