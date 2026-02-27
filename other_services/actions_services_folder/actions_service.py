
import  uuid
from gmqtt import Client as mqtt_client
from functools import partial
import asyncio

list_of_tasks=["Alert","Task1","Task2","Task3"]
list_of_allowed_tasks=["Alert","Task1","Task2"]
tasks_completed=[]

connection_mqtt=False


def client_subscribe(actions_topic,client, flags, rc, properties):   
    client.subscribe(actions_topic)
    print(f"Subscribed to topic : {actions_topic}")

def disconnected_from_mqtt(client, packet, exc=None):

    print("Disconnected from MQtt")

async def do_tasks(ml_report_topic,client, topic, payload, qos, properties):
    if len(payload)>0:
        task=payload.decode('utf-8')
        if task in list_of_allowed_tasks:
            print(f"Doing {task} ...")
            await asyncio.sleep(2)  # Simulate time taken to do the task
            await update_task_done(client=client,task=task,ml_report_topic=ml_report_topic)
        else:
            report=task_not_done("Invalid permission")
            send_task_report_to_ml_service(client,report,topic=ml_report_topic)

async def update_task_done(client,task,ml_report_topic):
    await asyncio.sleep(1)  # Simulate time taken to update the task status
    print(f"Task {task} is done!")
    tasks_completed.append(task)
    report = task_report(task)
    send_task_report_to_ml_service(client,report=report,topic=ml_report_topic)

def task_not_done(reason):
    global list_of_allowed_tasks
    return f"Task not done reason:{reason} , look at allowed tasks {list_of_allowed_tasks}"

def task_report(task):
    #send tasks completed to ml service 
    report = f"Report: {task} is completed, tasks completed till now :{tasks_completed}"
    return report

def send_task_report_to_ml_service(client,report,topic):
    client.publish(topic,report)
    print("Task Report Sent...")

async def main():
    print("Running...")
    broker="broker.hivemq.com"
    port=1883
    actions_topic="$share/iot-data-pipeline-v2/00989800/actions"
    ml_report_topic="iot-data-pipeline-v2/00989800/ml-report"
    client_id=str(uuid.uuid4())

    client=mqtt_client(client_id=client_id)
    client.on_disconnect=disconnected_from_mqtt
    client.on_message= partial(do_tasks,ml_report_topic)
    client.on_connect= partial(client_subscribe,actions_topic)
    await client.connect(host=broker,port=port)

    



    

    
    await asyncio.Event().wait()

if __name__=="__main__":
    asyncio.run(main())



