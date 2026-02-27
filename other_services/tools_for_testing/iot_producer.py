import asyncio
import uuid
import random
from gmqtt import Client as mqtt_client

async def main():
    broker = "broker.hivemq.com"
    port = 1883
    publish_topic = "00989800/call-from-bridge-topic"
    tasks = ["Alert", "Task1", "Task2", "InvalidTask"]
    
    # Use a shorter, cleaner client ID
    client = mqtt_client(client_id=str(uuid.uuid4()))

    print(f"Connecting to {broker}...")
    
    # Set a keepalive to help detect drops faster
    try:
        await client.connect(broker, port, keepalive=30)
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    print("Starting publisher loop. Press Ctrl+C to stop.")
    
    try:
        while True:
            # 1. GUARD: Check if we are actually connected
            if client.is_connected:
                chosen_task = random.choice(tasks)
                print(f"Sending task: {chosen_task}")
                
                # Use qos=1 to ensure the broker actually gets it
                client.publish(publish_topic, chosen_task, qos=1)
            else:
                print(" Client disconnected! Waiting for auto-reconnect...")
            
            # 2. HEARTBEAT: This allows gmqtt to process pings/reconnects
            await asyncio.sleep(5) 
            
    except asyncio.CancelledError:
        print("Publisher stopped.")
    finally:
        if client.is_connected:
            await client.disconnect()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass