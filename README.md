

# DataFlow-V2: Scalable IoT & AI Pipeline

DataFlow-V2 is a high-performance, event-driven IoT pipeline. It handles high-velocity sensor data ingestion via **MQTT**, utilizes **Kafka** for resilient message queuing, processes data through **Google Gemini 2.0 AI**, and persists the validated results to **Supabase**.

## 🏗️ System Architecture

The project is structured as a suite of decoupled microservices, allowing each component to scale independently based on system load.

### 📁 Project Structure

* **`bridge_services/`**: Bridges MQTT messages to Kafka topics. Includes rate-limiting logic.
* **`data_validation_services/`**: Cleans and validates JSON payloads from the raw Kafka stream.
* **`kafka_cluster/`**: Contains the Docker configurations for Zookeeper and Kafka brokers.
* **`other_services/`**:
* **Manager**: The cluster orchestrator. Manages service heartbeats and scaling (`START`/`IDLE`).
* **ML Brain**: Batch processes sensor data through **Gemini AI** for anomaly detection.
* **Data Storage**: Final consumer that writes validated data to **Supabase**.



---

## ⚙️ Configuration & Secrets

This project uses environment variables to keep credentials secure. You **must** create `.env` files in your service directories.

### 1. Root `.env.example`

Create a `.env` file in the root folder (and service folders) with the following keys:

```text
# --- MQTT SETTINGS ---
MQTT_HOST=your_hivemq_endpoint.s1.eu.hivemq.cloud
MQTT_PORT=8883
MQTT_USER=Snappp
MQTT_PASS=your_mqtt_password

# --- KAFKA SETTINGS ---
KAFKA_BROKERS=kafka-1:9092,kafka-2:9092
TOPIC_RAW=raw-iot-data
TOPIC_VALIDATED=validated-data

# --- AI SETTINGS ---
GEMINI_API_KEY=your_google_gemini_key
MIN_BATCH_SIZE=100
GEMINI_COOLDOWN=90

# --- DATABASE SETTINGS ---
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your_supabase_anon_key

```

---

## 🚀 Deployment Guide

### 1. Installation

Clone the repository and ensure Docker is running:

```bash
git clone https://github.com/your-username/DataFlow-V2.git
cd DataFlow-V2

```

### 2. Building the Images

Since the services rely on specific C-extensions for Kafka and MQTT, build the containers without cache to ensure environment stability:

```bash
docker compose build --no-cache

```

### 3. Running the Cluster

Start the entire pipeline in detached mode:

```bash
docker compose up -d

```

---

## 📊 Data Flow Pipeline

1. **Ingest**: Sensors publish data to the MQTT Broker (HiveMQ).
2. **Bridge**: The `bridge_service` picks up MQTT data and pushes it to the `raw-iot-data` Kafka topic.
3. **Validate**: `validation_service` removes heartbeats/meta-data and pushes clean JSON to `validated-data`.
4. **Analyze**: `ml_brain` consumes batches and asks **Gemini 2.0** if the readings are "Normal" or require "Alerts".
5. **Act**: If Gemini returns an action (e.g., `Task1`), the brain publishes a command back to MQTT.
6. **Store**: `data_storage_service` saves every validated reading into **Supabase SQL**.

---

## 🛡️ Security & Reliability

* **Fault Tolerance**: If Kafka goes down, the Bridge will retry connection every 5s.
* **Reaper Logic**: The Manager service automatically evicts "stale" service IDs from the active set if heartbeats stop for >30s.
* **Rate Protection**: The Bridge monitors message frequency to prevent "Bad Data" flooding the Kafka cluster.

---

## 🛠️ Debugging

To view live logs from the orchestrator:

```bash
docker logs -f service_manager

```

To check for AI decision logs:

```bash
docker logs -f ml_brain_service

```
