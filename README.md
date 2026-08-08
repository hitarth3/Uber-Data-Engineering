# Uber Real-Time Data Engineering Project

An end-to-end real-time data engineering pipeline for Uber ride booking data using Azure Event Hubs, Azure Data Lake Storage (ADLS Gen2), Databricks / PySpark, and Delta Lake following the Medallion Lakehouse Architecture (Bronze, Silver, Gold).

---

## 📐 Architecture Overview

![Project Architecture](architecture.png)

### Pipeline Flow:
1. **Data Producer / Stream Generation**:
   - Python FastAPI application generating synthetic Uber ride events in real-time.
   - Streamed into **Azure Event Hubs**.
2. **Bronze Layer (Raw Ingestion)**:
   - Ingests raw streaming and batch event data directly into Azure Data Lake Storage Gen2 (ADLS) in raw format.
3. **Silver Layer (Cleaned & Conformed)**:
   - PySpark transformations, schema enforcement, deduplication, and dimension lookups (Cities, Vehicle Types, Payment Methods, Ride Statuses).
   - Star schema and One Big Table (OBT) modeling with Delta Lake.
4. **Gold / Analytics Layer**:
   - Aggregated analytical tables and business KPIs ready for BI reporting and analytics.

---

## 📁 Repository Structure

```
├── Code_Files/
│   ├── bronze_adls.ipynb      # ADLS Bronze ingestion notebook
│   ├── ingest.py              # Ingestion utilities
│   ├── model.py               # Data models & schemas
│   ├── silver.py              # Silver layer transformation script
│   ├── silver_obt.ipynb       # Silver layer OBT notebook
│   └── silver_obt.sql         # SQL queries for Silver/Gold modeling
├── Data/                      # Reference & mapping dimension datasets
│   ├── bulk_rides.json
│   ├── map_cancellation_reasons.json
│   ├── map_cities.json
│   ├── map_payment_methods.json
│   ├── map_ride_statuses.json
│   ├── map_vehicle_makes.json
│   └── map_vehicle_types.json
├── templates/                 # Web UI templates
│   ├── confirmation.html
│   └── home.html
├── api.py                     # FastAPI web server for ride simulation
├── connection.py              # Azure Event Hub producer integration
├── data.py                    # Synthetic ride data generator (Faker)
├── architecture.png           # Architecture diagram
├── requirements.txt           # Python dependencies
└── pyproject.toml             # Project configuration
```

---

## 🚀 Getting Started

### 1. Prerequisites
- Python 3.12+
- Azure Account (Azure Event Hubs & ADLS Gen2)
- Azure Databricks or local PySpark environment

### 2. Environment Configuration
Create a `.env` file in the root directory:
```env
CONNECTION_STRING="<your_azure_event_hub_connection_string>"
EVENT_HUBNAME="<your_event_hub_name>"
```

### 3. Installation
```bash
# Clone the repository
git clone https://github.com/hitarth3/Uber-Data-Engineering.git
cd Uber-Data-Engineering

# Install dependencies
pip install -r requirements.txt
```

### 4. Running the Ride Event Producer
```bash
# Start the FastAPI application
uvicorn api:app --reload --port 8000
```
Open your browser at `http://localhost:8000` to simulate and stream ride bookings into Azure Event Hub.

---

## 🛠️ Tech Stack
- **Languages**: Python, PySpark, SQL
- **Cloud Platform**: Microsoft Azure
- **Streaming Ingestion**: Azure Event Hubs
- **Storage**: Azure Data Lake Storage Gen2 (ADLS Gen2), Delta Lake
- **Processing / ETL**: Azure Databricks / Apache Spark
- **Web / API**: FastAPI, Uvicorn, Jinja2, Faker



