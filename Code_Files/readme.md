# Pipeline Processing & Modeling Code

This directory contains the core PySpark scripts, Databricks Delta Live Tables (DLT) definitions, and SQL scripts implementing the Medallion Architecture for the Uber real-time streaming pipeline.

---

## 📂 Files Overview

### 1. `ingest.py`
- **Purpose**: Defines the streaming ingestion flow from Azure Event Hubs (via Kafka protocol) into the raw bronze table (`rides_raw`).
- **Engine**: PySpark Structured Streaming / Delta Live Tables (`@dp.table`).

### 2. `silver.py`
- **Purpose**: Parses the raw JSON payload against a strict schema (`rides_schema`), handles both streaming events and batch backfill from `bulk_rides`, and appends clean records into `stg_rides`.

### 3. `silver_obt.sql`
- **Purpose**: Implements the One Big Table (OBT) transformation by watermarking the real-time stream (`WATERMARK booking_timestamp DELAY OF INTERVAL 3 MINUTES`) and enriching rides with dimension tables:
  - `map_vehicle_makes`
  - `map_vehicle_types`
  - `map_ride_statuses`
  - `map_payment_methods`
  - `map_cities`
  - `map_cancellation_reasons`

### 4. `model.py`
- **Purpose**: Implements Star Schema dimensional modeling using Delta Live Tables CDC (`dp.create_auto_cdc_flow`):
  - **`dim_passenger`** (SCD Type 1)
  - **`dim_driver`** (SCD Type 1)
  - **`dim_vehicle`** (SCD Type 1)
  - **`dim_payment`** (SCD Type 1)
  - **`dim_booking`** (SCD Type 1)
  - **`dim_location`** (SCD Type 2 with `city_updated_at` tracking)
  - **`fact`** (Ride fact table capturing measures like fare, distance, duration, tips, and ratings)

### 5. Notebooks
- **`bronze_adls.ipynb`**: Initial raw data ingestion from Azure Blob Storage / ADLS Gen2 into Delta Bronze tables.
- **`silver_obt.ipynb`**: Interactive notebook for testing schema definitions, Jinja dynamic query generation, and verifying Gold layer aggregations.
