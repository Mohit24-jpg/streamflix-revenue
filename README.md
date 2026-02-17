# 🎬 Streamflix: Real-Time Data Lakehouse

**Architecture:** `Kafka (Redpanda)` -> `Spark Structured Streaming` -> `Apache Iceberg` -> `MinIO (S3)`

## 🚀 Overview
Streamflix is a fault-tolerant streaming data pipeline designed to ingest, process, and analyze high-throughput user event logs (simulating a Netflix-scale architecture). It enforces **Exactly-Once** processing semantics and utilizes **Apache Iceberg** to bring ACID transactions to the data lake.

## 🛠️ Tech Stack
* **Ingestion:** Redpanda (Kafka API) for decoupling producers/consumers.
* **Processing:** Apache Spark Structured Streaming (Python) for ETL and stateful aggregation.
* **Storage:** Apache Iceberg on MinIO (S3-compatible object storage).
* **Infrastructure:** Docker & Docker Compose for reproducible deployment.

## 📊 Key Features
* **Schema Enforcement:** Validates incoming JSON payloads against a strict StructType schema.
* **ACID Compliance:** Uses Iceberg's `copy-on-write` to prevent dirty reads during streaming ingestion.
* **Hadoop Catalog Bypass:** Custom implementation to route around legacy Hive Metastore dependencies for lightweight deployment.

## ⚡ How to Run
1.  **Clone the repo:**
    ```bash
    git clone [https://github.com/MohitVaid/streamflix-revenue.git](https://github.com/Mohit24-jpg/streamflix-revenue.git)
    cd streamflix-revenue
    ```
2.  **Start Infrastructure:**
    ```bash
    docker-compose up -d
    ```
3.  **Start Pipeline:**
    ```bash
    # 1. Start Producer
    python producer.py
    
    # 2. Submit Spark Job
    docker exec -it spark spark-submit ... [Add your command here]
    ```
