# WMS Data Pipeline (Data Engineer Focus)

End-to-end ELT pipeline for a Warehouse Management System (WMS), built for local development with Docker Compose.

## 1. Project Goal

This project ingests WMS operational data from an API, persists raw changes to a landing zone, loads staging tables in PostgreSQL, and builds a mart layer for KPI dashboards.

Main business domains:

- Inbound receipts (`ib_receipts`)
- Outbound orders (`ob_orders`)

## 2. Key Features

- End-to-end ELT pipeline for Warehouse Management System (WMS) data
- Incremental data ingestion using watermark-based extraction
- Layered data architecture: Landing → Staging → Mart
- Apache Airflow orchestration with retries and monitoring
- Dimensional data modeling (fact and dimension tables)
- Observability with Prometheus, StatsD exporter, and Grafana dashboards
- KPI views for inbound and outbound warehouse operations

## 3. Architecture

`Mock WMS API -> Extractor -> Landing (Parquet) -> Staging (history + latest) -> Mart (dimensions + facts + KPI views) -> Grafana`

### Architecture Diagram

            +----------------------+
            |     Mock WMS API     |
            |  (Orders, Receipts)  |
            +----------+-----------+
                       |
                       v
                 Extractor Service
                       |
                       v
              Landing Zone (Parquet)
                       |
                       v
              Staging Layer (PostgreSQL)
                stg.<entity>_history
             stg.<entity> (latest state)
                       |
                       v
                 Mart Layer
             mart.dim_* (dimensions)
             mart.fact_* (facts)
             KPI analytical views
                       |
                       v
                   Grafana
                (BI Dashboards)

Core orchestration and platform components:

- Apache Airflow (DAG scheduling + retries + alerts)
- PostgreSQL (Airflow metadata DB + WMS DW DB)
- Prometheus + StatsD exporter + Postgres exporters
- Grafana dashboards (WMS KPIs + Airflow monitoring)

## 4. Repository Structure

- `dags/`: Airflow DAG definitions (`wms_pipeline`)
- `services/extractor/`: API extraction, normalization, landing writer
- `services/staging/`: load to `stg.*` (history and latest)
- `services/mart/`: load DW mart tables from staging
- `services/common/`: shared DB, watermark, run-log utilities
- `services/mock_wms_api/`: local mock API for source simulation
- `sql/`: DW schema, mart schema, KPI views, DB bootstrap scripts
- `observability/`: Grafana dashboards, datasource provisioning, Prometheus config
- `tests/`: unit and integration tests

## 5. Data Pipeline Design

### 5.1 Extract (Incremental)

Extractor job (`services/extractor/app/run.py`) reads watermark per entity from `public.etl_watermark`, applies lookback (`LOOKBACK_SECONDS`) to avoid missed updates, fetches paginated API data, normalizes records, and writes landing files.

Output location:

- `data/landing/<entity>/run_id=<run_id>/...`

Default output format:

- Parquet (`OUTPUT_FORMAT=parquet`)

### 5.2 Staging (Raw Vault-style pattern)

Staging job (`services/staging/app/run.py`) reads landing data and writes to:

- `stg.<entity>_history`: append-only change history
- `stg.<entity>`: latest state upsert

Each row carries metadata such as `_run_id`, `_extracted_at`, and `_watermark_effective` for traceability.

### 5.3 Mart (Analytics Layer)

Mart job (`services/mart/app/run.py`) executes SQL loaders in `sql/mart/` to populate:

- Dimensions: `mart.dim_*`
- Facts: `mart.fact_ib_receipt`, `mart.fact_ob_order`, and line-level fact tables

KPI views for BI:

- `mart.v_kpi_ib_daily`
- `mart.v_kpi_ob_daily`
- `mart.v_kpi_ib_wip_status`
- `mart.v_kpi_ob_wip_status`

### 5.4 Orchestration and Reliability

Airflow DAG: `dags/dag_wms_pipeline.py`

- Flow: `extractor -> [staging_ib, staging_ob] -> [mart_ib, mart_ob]`
- Retries: 2
- Retry delay: 5 minutes
- Daily schedule: `@daily`
- Notifications: retry/final-failure email callbacks + deadline alert callback

Operational tracking tables:

- `public.etl_watermark`
- `public.pipeline_run_log`

## 6. Data Model

The analytical data warehouse follows a dimensional modeling approach to support efficient analytical queries and reporting.

Core warehouse entities include:

Dimensions:

- `mart.dim_date`
- `mart.dim_product`
- `mart.dim_warehouse`

Fact tables:

- `mart.fact_ib_receipt`
- `mart.fact_ob_order`

## 7. Data Lineage

Data flows through multiple transformation stages in the pipeline:

1. Source API provides operational WMS data
2. Extractor service retrieves incremental updates using watermark strategy
3. Raw data is persisted to the landing zone in Parquet format
4. Staging jobs normalize data and maintain both history and latest-state tables
5. Mart jobs build analytical datasets using SQL transformations
6. KPI views are consumed by Grafana dashboards

## 8. Observability and Dashboards

### 8.1 Available Grafana Dashboards

WMS dashboards (`observability/grafana/dashboards/Postgres Dashboard/`):

- `wms-overview.json`
- `wms-inbound.json`
- `wms-outbound-bottleneck.json`

Airflow dashboards (`observability/grafana/dashboards/Airflow Dashboard/`):

- `airflow-dags-overview.json`
- `airflow-dag-dashboard.json`
- `airflow-cluster-dashboard.json`

### 8.2 Metrics Stack

- Airflow emits StatsD metrics -> `statsd-exporter`
- Prometheus scrapes:
  - `statsd-exporter:9102`
  - `postgres-exporter-airflow:9187`
  - `postgres-exporter-dw:9187`
- Grafana uses Prometheus datasource provisioned in:
  - `observability/grafana/provisioning/datasources/datasource.yml`

Note:

- Current provisioning includes Prometheus datasource by default.
- For WMS Postgres dashboards, create/import a PostgreSQL datasource in Grafana, then map it during dashboard import.

## 9. Local Setup

### 9.1 Prerequisites

- Docker + Docker Compose
- Make sure ports are available:
  - 3000 (Grafana)
  - 8080 (Airflow)
  - 9090 (Prometheus)
  - 5432 (DW Postgres)
  - 5433 (Airflow Postgres)
  - 8000 (Mock API)

### 9.2 Start the Full Stack

```bash
docker compose up -d --build
```

DW initialization runs automatically via `sql/run_all.sh` mounted to Postgres init.

### 9.3 Access Services

- Airflow: `http://localhost:8080` (`admin/admin`)
- Grafana: `http://localhost:3000` (`admin/admin`)
- Prometheus: `http://localhost:9090`
- MailHog: `http://localhost:8025`
- Mock API: `http://localhost:8000`

## 10. Running Pipeline Jobs

### 10.1 Trigger DAG in Airflow

DAG ID: `wms_pipeline`

You can trigger from UI or CLI:

```bash
docker compose exec airflow-webserver airflow dags trigger wms_pipeline
```

### 10.2 Run Service Modules Manually (inside Airflow container)

```bash
docker compose exec airflow-webserver bash
cd /opt/airflow/project
python -m services.extractor.app.run --run-id manual_001
python -m services.staging.app.run --entity ib_receipts --run-id manual_001
python -m services.staging.app.run --entity ob_orders --run-id manual_001
python -m services.mart.app.run --entity ib_receipts --run-id manual_001
python -m services.mart.app.run --entity ob_orders --run-id manual_001
```

## 11. Testing

Run tests from project root:

```bash
pytest
```

Pytest configuration is defined in `pytest.ini` (unit + integration tests).

## 12. Design Decisions

### 12.1 Layered Architecture

The pipeline follows a layered data architecture:

Landing → Staging → Mart

This separation allows raw ingested data to remain immutable while enabling transformations and analytical modeling in downstream layers.

### 12.2 Incremental Processing

Instead of full refresh loads, the pipeline uses a watermark-based incremental extraction strategy with a configurable lookback window to handle late updates.

### 12.3 SQL-first Mart Layer

Transformations in the mart layer are implemented using SQL scripts to keep business logic transparent and easily reviewable.

## 13. Data Engineer Notes

- Incremental extraction is watermark-driven with a configurable lookback window.
- Staging keeps both full history and latest-state tables for replay/debugging.
- Mart load is SQL-first, keeping transformation logic explicit and reviewable.
- `pipeline_run_log` enables per-run auditability (rows in, rows written, status, errors).
- Dashboards are separated into business KPIs (WMS) and platform reliability (Airflow).

## 14. Future Improvements

- Add automated data quality tests (freshness, null checks, referential integrity)
- Introduce CI pipeline for tests and SQL linting
- Implement partitioning strategy for large staging tables
- Add Spark-based transformation pipeline for high-volume processing
- Implement Slowly Changing Dimension (SCD) support for historical tracking

---
