import asyncio
import uuid
import ssl
import json
from gmqtt import Client as mqtt_client


BROKER = "31d09ce8b7fa4a92aafc62ae06187541.s1.eu.hivemq.cloud"
PORT = 8883
USERNAME = "Snappp"
PASSWORD = "Snap00989800"

# -------- PREDEFINED TOPICS --------
TOPICS = {
    1: "00989800/to_bridge_calls",   # manager
    2: "00989800/123",               # data
}

# -------- PREDEFINED PAYLOADS --------
PAYLOADS = {
    1: {
        "service_id": "46b01876-f798-46b9-904e-c33623f0b593",
        "msg": "START",
        "condition": None,
        "expected_reponse": False
    },
    2: {
        "service_id": "46b01876-f798-46b9-904e-c33623f0b593",
        "msg": "IDLE",
        "condition": None,
        "expected_reponse": False
    },
    3: {"temp": 20},
    4: {
        "service_id": "ALL",
        "msg": "STATUS",
        "condition": None,
        "expected_reponse": False
    },
}


async def publisher_loop(client):
    while True:
        try:
            print("\n====== MQTT PUBLISHER MENU ======")

            # ---- Topic Selection ----
            print("\nSelect Topic:")
            for key, value in TOPICS.items():
                print(f"{key}. {value}")

            topic_choice = int(input("Choose topic number: "))
            if topic_choice not in TOPICS:
                print("❌ Invalid choice")
                continue

            topic = TOPICS[topic_choice]

            # ---- Payload Selection ----
            print("\nSelect Payload:")
            for key, value in PAYLOADS.items():
                print(f"{key}. {value}")

            payload_choice = int(input("Choose payload number: "))
            if payload_choice not in PAYLOADS:
                print("❌ Invalid choice")
                continue

            payload_data = PAYLOADS[payload_choice]

            # ---- Rate Input ----
            rate = float(input("\nEnter publish rate (messages/sec): "))
            if rate <= 0:
                print("❌ Rate must be > 0")
                continue

            delay = 1.0 / rate

            print(f"\n🚀 Publishing to '{topic}' at {rate} msg/sec")
            print("Press Ctrl+C to stop...\n")

            # ---- Publishing Loop ----
            while True:
                if client.is_connected:
                    payload = json.dumps(payload_data)
                    client.publish(topic, payload, qos=0)
                    print(f"Sent → {payload}")
                else:
                    print("Client not connected.")

                await asyncio.sleep(delay)

        except KeyboardInterrupt:
            print("\n🛑 Publishing stopped.")
            print("Returning to menu...\n")
            continue


async def main():
    ssl_ctx = ssl.create_default_context()
    client = mqtt_client(client_id=str(uuid.uuid4()))

    print(f"Connecting to {BROKER}...")

    try:
        client.set_auth_credentials(USERNAME, PASSWORD)
        await client.connect(BROKER, PORT, ssl=ssl_ctx, keepalive=30)
        await asyncio.sleep(2)
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