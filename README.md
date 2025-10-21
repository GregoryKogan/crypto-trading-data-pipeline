# Real-Time Crypto Trading Analytics Pipeline

[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7.0-231F20?style=flat-square&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8+-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17-336791?style=flat-square&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=flat-square&logo=docker&logoColor=white)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](https://opensource.org/licenses/MIT)

A streaming data pipeline for real-time cryptocurrency trade data processing. This project demonstrates modern data engineering concepts using a containerized microservices architecture with Apache Spark, Kafka, and Airflow.

## Table of Contents

- [Core Technologies](#core-technologies)
- [Architecture](#architecture)
- [How to Run the Pipeline](#how-to-run-the-pipeline)
  - [Build and Start All Services](#1-build-and-start-all-services)
  - [Configure Airflow Connections](#2-configure-airflow-connections-one-time-setup)
  - [Start the Pipeline](#3-start-the-pipeline)
- [Monitoring & Verification](#monitoring--verification)
- [How to Stop the Pipeline](#how-to-stop-the-pipeline)
- [Project Structure](#project-structure)
- [License](#license)

## Core Technologies

- **Data Ingestion**: Python, Binance WebSocket API
- **Message Broker**: Apache Kafka
- **Stream Processing**: Apache Spark (Structured Streaming)
- **Data Storage**: PostgreSQL
- **Orchestration & Monitoring**: Apache Airflow
- **Containerization**: Docker & Docker Compose

## Architecture

The data flows from a live exchange feed through Kafka, is processed in real-time by Spark, and is stored in a PostgreSQL database. Airflow manages the submission and monitoring of the Spark job.

```mermaid
graph TD
    subgraph External Source
        API["Crypto Exchange API<br/>(WebSocket Endpoint)"]
    end

    subgraph Docker Compose
        Producer["Data Ingestion Producer<br/>(producer.py)"]
        Kafka["Apache Kafka<br/>(Topic: raw_trades)"]
        Airflow["Apache Airflow<br/>(Scheduling & Monitoring)"]
        Spark["Stream Processor<br/>(Spark Streaming)<br/>1-min Tumbling Windows"]
        Postgres["Data Mart (PostgreSQL)<br/>(Table: trades_1min_agg)"]
    end

    API -- "1. Live Trade Data (JSON)" --> Producer
    Producer -- "2. Raw Trade Events (JSON)" --> Kafka
    Kafka -- "3a. Consume Raw Events" --> Spark
    Airflow -. "3b. Submits & Monitors Job" .-> Spark
    Spark -- "4. Aggregated Data" --> Postgres
```

## How to Run the Pipeline

### 1. Build and Start All Services

From the project's root directory:

```bash
docker-compose up --build -d
```

This will start the following services:

- **Zookeeper**: Kafka coordination service
- **Kafka**: Message broker for streaming data
- **PostgreSQL**: Database for storing aggregated data
- **pgAdmin**: Web interface for database management
- **Producer**: Python service that ingests live crypto data
- **Spark Master**: Spark cluster master node
- **Spark Worker**: Spark cluster worker node
- **Airflow Init**: Initializes Airflow database and creates admin user
- **Airflow Webserver**: Web interface for pipeline monitoring
- **Airflow Scheduler**: Schedules and monitors DAGs

### 2. Configure Airflow Connections (One-Time Setup)

The Airflow DAGs need to know how to connect to PostgreSQL.

1. Navigate to the **Airflow UI**: [http://localhost:8081](http://localhost:8081)
2. Login with username `admin` and password `admin`.
3. Go to **Admin -> Connections**.

4. **Create the PostgreSQL Connection:**
    - Click the `+` button to add a new connection.
    - **Connection Id:** `crypto_pipeline_postgres`
    - **Connection Type:** `Postgres`
    - **Host:** `postgres`
    - **Database:** `crypto_data`
    - **Login:** `user`
    - **Password:** `password`
    - **Port:** `5432`
    - Click **Save**.

### 3. Start the Pipeline

1. In the Airflow UI, go to the **DAGs** view.
2. Find the `crypto_pipeline_submit_dag` and un-pause it using the toggle on the left.
3. Click on the DAG name, then click the "Play" button to trigger it manually. This will submit the Spark job.
4. Find the `crypto_pipeline_monitor_dag` and un-pause it. This DAG will now run automatically every 5 minutes to monitor the pipeline.

## Monitoring & Verification

- **Producer Logs**: Check that the producer is successfully publishing messages.

    ```bash
    docker logs -f producer
    ```

    You should see logs like `Published trade to Kafka: ...`

- **Spark Master UI**: [http://localhost:8080](http://localhost:8080)
  - You should see one "Running Application" corresponding to our `CryptoAnalytics` job.

- **Airflow UI**: [http://localhost:8081](http://localhost:8081)
  - The `crypto_pipeline_submit_dag` should have a successful run.
  - The `crypto_pipeline_monitor_dag` should have successful runs every 5 minutes.

- **pgAdmin (Database UI)**: [http://localhost:5050](http://localhost:5050)
  - Add a new server connection:
    - **Host:** `postgres`
    - **Port:** `5432`
    - **Username:** `user`
    - **Password:** `password`
  - Navigate to `crypto_data -> Schemas -> public -> Tables -> trades_1min_agg`.
  - Right-click the table and select "View/Edit Data" -> "All Rows". You should see aggregated data appearing and updating every minute.

## How to Stop the Pipeline

To stop all running containers and remove the network, run:

```bash
docker-compose down
```

To stop the containers AND remove all persisted data (PostgreSQL data, pgAdmin data, named volumes), use the `-v` flag:

```bash
docker-compose down -v
```

## Project Structure

```plaintext
/crypto-trading-data-pipeline/
|
├── .gitignore
├── docker-compose.yml
├── README.md
|
├── producer/
│   ├── Dockerfile
│   ├── producer.py
│   └── requirements.txt
|
├── spark_processor/
│   ├── Dockerfile
│   ├── processor.py
│   └── requirements.txt
|
├── airflow/
│   ├── dags/
│   │   ├── crypto_pipeline_monitor_dag.py
│   │   └── crypto_pipeline_submit_dag.py
│   ├── Dockerfile
│   └── requirements.txt
|
└── postgres/
    └── init/
        └── init.sql
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
