# Olist E-Commerce Data Engineering Platform

An end-to-end data engineering project built using the Brazilian E-Commerce Public Dataset by Olist. The project demonstrates how raw e-commerce data can be ingested, processed, stored, transformed, and delivered as analytical datasets for business intelligence.

## Project Overview

This project implements a data engineering pipeline that transforms raw e-commerce data into structured analytical models and interactive business dashboards.

The platform covers:
- Data ingestion and raw data storage.
- Distributed data processing using PySpark.
- Workflow orchestration using Apache Airflow.
- Cloud data warehousing using Snowflake.
- Data transformation and testing using dbt.
- Business intelligence and visualization using Power BI.

## Architecture

**Data Flow:**

Olist Dataset → MinIO → PySpark → Airflow → Snowflake → dbt → Power BI

### Technologies Used

| Technology | Purpose |
|---|---|
| Python | Data processing and pipeline development |
| Docker | Containerized local environment |
| MinIO | Raw data object storage |
| Apache Spark / PySpark | Data processing |
| Apache Airflow | Pipeline orchestration |
| Snowflake | Cloud data warehouse |
| dbt | Data transformation and testing |
| Power BI | Business intelligence and visualization |
| SQL | Data querying and analytical modeling |

## Data Warehouse Design

The analytical warehouse follows a dimensional modeling approach.

### Fact Tables
- `FACT_ORDER`
- `FACT_SALES`
- `FACT_PAYMENT`

### Dimension Tables
- `DIM_CUSTOMER`
- `DIM_PRODUCT`
- `DIM_SELLER`
- `DIM_DATE`

The design separates different business processes to avoid double-counting when joining fact tables at different granularities.

## dbt Transformation

dbt is used to transform raw warehouse data into structured analytical models.

The project includes:
- Staging models.
- Fact and dimension models.
- Analytical marts.
- Reusable macros.
- Data quality tests.

### Validation Results

The recorded dbt execution completed successfully:

- 19 models executed.
- 8 tests passed.
- 0 warnings.
- 0 errors.
- 0 skipped tests.

### Key Data Validation Results

| Metric | Validated Result |
|---|---:|
| Fact Orders rows | 95,128 |
| Fact Sales rows | 108,640 |
| Unique customers | 92,081 |
| Total sales including freight | 15,217,260.20 |
| Product revenue | 13,046,300.07 |
| Freight value | 2,170,960.13 |

These figures represent the recorded validation results from the project execution.

## Power BI Dashboard

The Power BI report connects to Snowflake and contains five analytical pages:

1. **Executive Overview** – Overall business performance.
2. **Customer Analytics** – Customer behavior and analysis.
3. **Sales & Product Analytics** – Sales and product performance.
4. **Delivery & Operations** – Delivery and operational analysis.
5. **Sellers & Payments** – Seller performance and payment analysis.

The dashboard was refreshed successfully without reporting an error.

## Project Setup

### Prerequisites

- Python
- Docker Desktop
- Access to a Snowflake account
- Power BI Desktop
- dbt environment and project dependencies

### Configuration

Before running the project, configure the required environment variables and service connections.

Create your local `.env` file using `.env.example` as a reference.

Do not commit passwords, API tokens, Snowflake credentials, or other secrets to GitHub.

### Running the Pipeline

The intended workflow is:

1. Start the required Docker services.
2. Upload the raw Olist data to MinIO.
3. Execute the PySpark processing workflow.
4. Trigger the Airflow DAG.
5. Load the processed data into Snowflake.
6. Execute dbt transformations and tests.
7. Refresh the Power BI dashboard.

Refer to the project configuration and individual component directories for the exact execution commands.

## Repository Structure

The repository contains the dbt project, PySpark resources, and supporting project files. Additional deployment and orchestration configuration should be included where applicable.

## Business Value

This project demonstrates an end-to-end analytical data platform capable of transforming raw e-commerce data into trusted datasets for business analysis.

It supports analysis of:
- Sales and revenue.
- Customer behavior.
- Product performance.
- Seller performance.
- Payment methods.
- Delivery operations.

## Author

**Habiba Hamdy**

GitHub: [HabibaHamdy2004](https://github.com/HabibaHamdy2004)

---

*This project was developed for learning and portfolio purposes using the Brazilian E-Commerce Public Dataset by Olist.*
