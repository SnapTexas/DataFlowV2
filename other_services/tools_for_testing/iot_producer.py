import asyncio
import uuid
import random
import ssl
import json
from gmqtt import Client as mqtt_client
async def main():
    broker = "31d09ce8b7fa4a92aafc62ae06187541.s1.eu.hivemq.cloud"
    port = 8883
    username = "Snappp"
    password = "Snap00989800"
    ssl_ctx = ssl.create_default_context()
    publish_topic = "00989800/to_bridge_calls"
    
    client = mqtt_client(client_id=str(uuid.uuid4()))

    print(f"Connecting to {broker}...")
    
    try:
        client.set_auth_credentials(username, password)
        # FIX 1: Pass the ssl context here!
        await client.connect(broker, port, ssl=ssl_ctx, keepalive=30)
        
        # FIX 2: You MUST await the sleep to give the handshake time to finish
        await asyncio.sleep(2) 
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    print("Starting publisher loop. Press Ctrl+C to stop.")
    
    try:
        while True:
            # client.is_connected only becomes True AFTER the handshake is 100% done
            if client.is_connected:
                chosen_id = "46b01876-f798-46b9-904e-c33623f0b593"
                data = {
                    "service_id": chosen_id,
                    "msg": "STATUS",
                    "condition": None,
                    "expected_reponse": False
                }
                payload = json.dumps(data)
                print(f"Sending task: {payload}")
                
                # QoS 1 is good here to ensure delivery
                client.publish(publish_topic, payload, qos=1)
            else:
                print("Client not ready yet... checking connection.")
            
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