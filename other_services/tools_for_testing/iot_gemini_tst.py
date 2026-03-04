import json, random, asyncio
from gmqtt import Client as MQTTClient

async def simulate_iot_device():
    client = MQTTClient("v2-sensor-01")
    await client.connect("broker.hivemq.com")
    
    temp, humid, count = 20.0, 50.0, 0
    while True:
        temp += random.uniform(-0.5, 0.5)
        humid += random.uniform(-1, 1)
        count += 1
        
        # v2 Payload with specific device ID and telemetry
        msg = {"id": f"v2_node_{count}", "temp": round(temp, 2), "humid": round(humid, 2)}
        client.publish("dataflow/v2/telemetry", json.dumps(msg))
        


        print(f"Terminal Output: Published to v2 Bus -> {msg}")
        await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(simulate_iot_device())
