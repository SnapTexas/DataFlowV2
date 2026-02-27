import asyncio
import uuid
import random
from gmqtt import Client as mqtt_client

async def main():
    broker = "broker.hivemq.com"
    port = 1883
    #iot-data-pipeline-v2/
    publish_topic = "00989800/actions"
    
    # Task list to match your worker's allowed tasks
    tasks = ["Alert", "Task1", "Task2", "InvalidTask"]
    
    client = mqtt_client(client_id=str(uuid.uuid4()))

    print(f"Connecting to {broker}...")
    await client.connect(broker, port)

    print("Starting publisher loop. Press Ctrl+C to stop.")
    try:
        while True:
            # Pick a random task from the list
            chosen_task = random.choice(tasks)
            
            print(f"Sending task: {chosen_task}")
            client.publish(publish_topic, chosen_task)
            
            # Use await asyncio.sleep so the network loop can run in the background
            await asyncio.sleep(5) 
            
    except KeyboardInterrupt:
        print("Stopping publisher...")
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())