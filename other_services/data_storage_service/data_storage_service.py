import asyncio
import json
import os
import socket
from aiokafka import AIOKafkaConsumer
from supabase import create_client, Client
import colorlog
import logging
from dotenv import load_dotenv

load_dotenv()

# Load environment variables (API Keys)


# --- LOGGING ---
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s%(levelname)-8s%(reset)s %(blue)s%(message)s',
    log_colors={'INFO': 'cyan', 'ERROR': 'red', 'WARNING': 'yellow'}
))
logger = colorlog.getLogger('supabase_storage')
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# --- CONFIGURATION ---

BOOTSTRAP_SERVERS = os.getenv('KAFKA_BROKERS')
INPUT_TOPIC = os.getenv('TOPIC_VALIDATED')

# Update these with your Supabase credentials
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
# Initialize Supabase Client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

async def store_in_supabase(data):
    """
    Transforms the JSON Kafka payload into Key-Value rows for Supabase.
    """
    try:
        # Extract metadata for the composite key
        device = data.get('validated_by', socket.gethostname())
        timestamp = data.get('iso_timestamp', 'no-time')
        
        rows_to_insert = []

        # Iterate through the dictionary to create Key-Value pairs
        for key, value in data.items():
            # Skip metadata keys so you only store actual sensor data
            if key in ['iso_timestamp', 'unix_timestamp', 'validated_by']:
                continue
                
            # Create a unique key string (Example: sensor_1:temperature:2026-03-06T...)
            unique_key = f"{device}:{key}:{timestamp}"
            
            rows_to_insert.append({
                "key": unique_key,
                "value": str(value) # Ensuring value is a string as per your table
            })

        if rows_to_insert:
            # Using 'upsert' to prevent primary key conflicts on retries
            await asyncio.to_thread(
                supabase.table("sensor_data").upsert(rows_to_insert).execute
            )
            logger.info(f"Pushed {len(rows_to_insert)} Key-Value pairs to Supabase.")

    except Exception as e:
        logger.error(f"Supabase Insert Error: {e}")

async def storage_worker():
    # Setup Kafka Consumer
    consumer = AIOKafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="supabase-storage-group",
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    # Start the consumer
    await consumer.start()
    logger.info(f"Storage Service connected to Kafka. Listening on {INPUT_TOPIC}...")

    try:
        async for msg in consumer:
            # msg.value is already a dict thanks to the value_deserializer
            await store_in_supabase(msg.value)
    except Exception as e:
        logger.error(f"Fatal Consumer Error: {e}")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    try:
        asyncio.run(storage_worker())
    except KeyboardInterrupt:
        logger.warning("Storage service stopped manually.")