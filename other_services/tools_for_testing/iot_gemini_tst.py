import asyncio
import uuid
import ssl
import json
from gmqtt import Client as mqtt_client

BROKER = "31d09ce8b7fa4a92aafc62ae06187541.s1.eu.hivemq.cloud"
PORT = 8883
USERNAME = "Snappp"
PASSWORD = "Snap00989800"
SUBSCRIBE_TOPIC = "$share/iot-data-pipeline-v2/00989800/#"

ssl_ctx = ssl.create_default_context()


def on_connect(client, flags, rc, properties):
    print("Connected to broker")
    client.subscribe(SUBSCRIBE_TOPIC, qos=1)
    print(f"Subscribed to {SUBSCRIBE_TOPIC}")


def on_message(client, topic, payload, qos, properties):
    try:
        data = payload.decode()
        data = json.loads(data)
        print(f"\nReceived message on {topic}")
        print("Payload:", data)
    except Exception as e:
        print("Error decoding message:", e)


def on_disconnect(client, packet, exc=None):
    print("Disconnected")


async def main():
    client = mqtt_client(client_id=str(uuid.uuid4()))

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    client.set_auth_credentials(USERNAME, PASSWORD)

    print(f"Connecting to {BROKER}...")
    await client.connect(BROKER, PORT, ssl=ssl_ctx, keepalive=30)

    # Keep program running
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())