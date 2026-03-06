import uuid
from gmqtt import Client as mqtt_client
from functools import partial
import asyncio
import ssl
import socket

# --- CONFIGURATION ---
list_of_allowed_tasks = ["Alert", "Task1", "Task2"]
tasks_completed = []
service_id = socket.gethostname()

def client_subscribe(actions_topic, client, flags, rc, properties):   
    client.subscribe(actions_topic)
    print(f"Actions Service {service_id} subscribed to: {actions_topic}")

async def do_tasks(ml_report_topic, client, topic, payload, qos, properties):
    if len(payload) > 0:
        task = payload.decode('utf-8')
        if task in list_of_allowed_tasks:
            print(f">>> Action Triggered: Doing {task}...")
            await asyncio.sleep(2)  # Simulate hardware/task delay
            
            tasks_completed.append(task)
            report = f"ID: {service_id} | Task {task} completed. History: {tasks_completed}"
            
            client.publish(ml_report_topic, report)
            print(f"Sent completion report to ML Service.")
        else:
            reason = f"Permission Denied for {task}"
            client.publish(ml_report_topic, f"Error: {reason}")

async def main():
    # HiveMQ Cloud Details
    host = "31d09ce8b7fa4a92aafc62ae06187541.s1.eu.hivemq.cloud"
    port = 8883
    username = "Snappp"
    password = "Snap00989800"
    
    actions_topic = "$share/iot-data-pipeline-v2/00989800/actions"
    ml_report_topic = "00989800/ml-report"
    
    ssl_ctx = ssl.create_default_context()

    client = mqtt_client(client_id=service_id)
    client.set_auth_credentials(username, password)
    
    client.on_connect = partial(client_subscribe, actions_topic)
    client.on_message = partial(do_tasks, ml_report_topic)
    
    print(f"Connecting Actions Service {service_id} to HiveMQ...")
    await client.connect(host, port, ssl=ssl_ctx)
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())