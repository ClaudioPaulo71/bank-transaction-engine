High-Performance Transaction Engine (Bank Simulation)

This project is a production-grade backend system designed to handle high-volume financial transactions using an asynchronous, event-driven architecture. It simulates a mission-critical banking environment, focusing on scalability, data integrity, and low-latency ingestion.

🏗️ Architecture Overview

The system follows a decoupled architecture to ensure that high traffic at the API level does not overwhelm the persistence layer:

FastAPI (Ingress Service): Receives incoming transaction requests, validates them using Pydantic schemas, and immediately dispatches them to a message broker.

Apache Kafka (Message Broker): Acts as a resilient buffer and distributed log, ensuring transaction durability and allowing for horizontal scaling of consumers.

Background Worker (Consumer): An asynchronous process that listens to Kafka topics and handles the heavy lifting of persisting data.

PostgreSQL (Persistence): Stores the final state of transactions with full ACID compliance, optimized for financial auditing.

🚀 Tech Stack

Language: Python 3.11+

Framework: FastAPI (Asynchronous ASGI)

Database: PostgreSQL with SQLAlchemy 2.0 (Async Engine)

Messaging: Apache Kafka (via aiokafka)

Containerization: Docker & Docker Compose

Validation: Pydantic v2

🛠️ Getting Started

Prerequisites

Docker and Docker Compose installed.

Python 3.11+ environment.

1. Infrastructure Setup

Launch the required services (PostgreSQL, Kafka, Zookeeper) using Docker:

docker-compose up -d


2. Python Environment Setup

Create a virtual environment and install the dependencies:

python -m venv venv
# Windows
.\venv\Scripts\activate
# Linux/macOS
source venv/bin/activate

pip install -r requirements.txt


3. Running the Application

Start the FastAPI server:

uvicorn app_bofa_standalone:app --reload


📊 API Usage

Once the server is running, access the interactive documentation at http://127.0.0.1:8000/docs.

Key Endpoints

POST /v1/transactions: Ingest a new transaction (Returns 202 Accepted).

GET /v1/transactions: Retrieve a list of all processed transactions.

GET /health: Check the status of the API, Kafka, and Database connections.

Sample Payload

{
  "account_id": "ACC-7890",
  "amount": 1250.75,
  "currency": "USD",
  "merchant_name": "Chicago Retail Hub"
}


🛡️ Engineering Best Practices

Asynchronous I/O: Every database and messaging operation is non-blocking, maximizing the throughput of the Event Loop.

ACID Compliance: Transactions are wrapped in atomic sessions to ensure data consistency.

Health Monitoring: Integrated health checks for proactive infrastructure monitoring.

Decoupled Logic: The producer and consumer are logically separated, allowing them to scale independently.

Developed for technical demonstration in the Financial Services domain.