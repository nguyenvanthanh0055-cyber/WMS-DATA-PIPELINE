# WMS Data Pipeline (Data Engineer Focus)

End-to-end ELT pipeline for a Warehouse Management System (WMS), built for local development with Docker Compose.

## 1. Project Goal

This project ingests WMS operational data from an API, persists raw changes to a landing zone, loads staging tables in PostgreSQL, and builds a mart layer for KPI dashboards.

Main business domains:

- Inbound receipts (`ib_receipts`)
- Outbound orders (`ob_orders`)

## 2. Architecture

`Mock WMS API -> Extractor -> Landing (Parquet) -> Staging (history + latest) -> Mart (dimensions + facts + KPI views) -> Grafana`

Core orchestration and platform components:

- Apache Airflow (DAG scheduling + retries + alerts)
- PostgreSQL (Airflow metadata DB + WMS DW DB)
- Prometheus + StatsD exporter + Postgres exporters
- Grafana dashboards (WMS KPIs + Airflow monitoring)

## 3. Repository Structure

- `dags/`: Airflow DAG definitions (`wms_pipeline`)
- `services/extractor/`: API extraction, normalization, landing writer
- `services/staging/`: load to `stg.*` (history and latest)
- `services/mart/`: load DW mart tables from staging
- `services/common/`: shared DB, watermark, run-log utilities
- `services/mock_wms_api/`: local mock API for source simulation
- `sql/`: DW schema, mart schema, KPI views, DB bootstrap scripts
- `observability/`: Grafana dashboards, datasource provisioning, Prometheus config
- `tests/`: unit and integration tests

## 4. Data Pipeline Design

### 4.1 Extract (Incremental)

Extractor job (`services/extractor/app/run.py`) reads watermark per entity from `public.etl_watermark`, applies lookback (`LOOKBACK_SECONDS`) to avoid missed updates, fetches paginated API data, normalizes records, and writes landing files.

Output location:

- `data/landing/<entity>/run_id=<run_id>/...`

Default output format:

- Parquet (`OUTPUT_FORMAT=parquet`)

### 4.2 Staging (Raw Vault-style pattern)

Staging job (`services/staging/app/run.py`) reads landing data and writes to:

- `stg.<entity>_history`: append-only change history
- `stg.<entity>`: latest state upsert

Each row carries metadata such as `_run_id`, `_extracted_at`, and `_watermark_effective` for traceability.

### 4.3 Mart (Analytics Layer)

Mart job (`services/mart/app/run.py`) executes SQL loaders in `sql/mart/` to populate:

- Dimensions: `mart.dim_*`
- Facts: `mart.fact_ib_receipt`, `mart.fact_ob_order`, and line-level fact tables

KPI views for BI:

- `mart.v_kpi_ib_daily`
- `mart.v_kpi_ob_daily`
- `mart.v_kpi_ib_wip_status`
- `mart.v_kpi_ob_wip_status`

### 4.4 Orchestration and Reliability

Airflow DAG: `dags/dag_wms_pipeline.py`

- Flow: `extractor -> [staging_ib, staging_ob] -> [mart_ib, mart_ob]`
- Retries: 2
- Retry delay: 5 minutes
- Daily schedule: `@daily`
- Notifications: retry/final-failure email callbacks + deadline alert callback

Operational tracking tables:

- `public.etl_watermark`
- `public.pipeline_run_log`

## 5. Observability and Dashboards

### 5.1 Available Grafana Dashboards

WMS dashboards (`observability/grafana/dashboards/Postgres Dashboard/`):

- `wms-overview.json`
- `wms-inbound.json`
- `wms-outbound-bottleneck.json`

Airflow dashboards (`observability/grafana/dashboards/Airflow Dashboard/`):

- `airflow-dags-overview.json`
- `airflow-dag-dashboard.json`
- `airflow-cluster-dashboard.json`

### 5.2 Metrics Stack

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

## 6. Local Setup

### 6.1 Prerequisites

- Docker + Docker Compose
- Make sure ports are available:
  - 3000 (Grafana)
  - 8080 (Airflow)
  - 9090 (Prometheus)
  - 5432 (DW Postgres)
  - 5433 (Airflow Postgres)
  - 8000 (Mock API)

### 6.2 Start the Full Stack

```bash
docker compose up -d --build
```

DW initialization runs automatically via `sql/run_all.sh` mounted to Postgres init.

### 6.3 Access Services

- Airflow: `http://localhost:8080` (`admin/admin`)
- Grafana: `http://localhost:3000` (`admin/admin`)
- Prometheus: `http://localhost:9090`
- MailHog: `http://localhost:8025`
- Mock API: `http://localhost:8000`

## 7. Running Pipeline Jobs

### 7.1 Trigger DAG in Airflow

DAG ID: `wms_pipeline`

You can trigger from UI or CLI:

```bash
docker compose exec airflow-webserver airflow dags trigger wms_pipeline
```

### 7.2 Run Service Modules Manually (inside Airflow container)

```bash
docker compose exec airflow-webserver bash
cd /opt/airflow/project
python -m services.extractor.app.run --run-id manual_001
python -m services.staging.app.run --entity ib_receipts --run-id manual_001
python -m services.staging.app.run --entity ob_orders --run-id manual_001
python -m services.mart.app.run --entity ib_receipts --run-id manual_001
python -m services.mart.app.run --entity ob_orders --run-id manual_001
```

## 8. Testing

Run tests from project root:

```bash
pytest
```

Pytest configuration is defined in `pytest.ini` (unit + integration tests).

## 9. Data Engineer Notes

- Incremental extraction is watermark-driven with a configurable lookback window.
- Staging keeps both full history and latest-state tables for replay/debugging.
- Mart load is SQL-first, keeping transformation logic explicit and reviewable.
- `pipeline_run_log` enables per-run auditability (rows in, rows written, status, errors).
- Dashboards are separated into business KPIs (WMS) and platform reliability (Airflow).

## 10. Future Improvements

- Add Grafana PostgreSQL datasource provisioning as code.
- Add data quality tests (freshness, null checks, referential checks) in pipeline.
- Add CI workflow for tests + SQL linting.
- Add partitioning/index strategy for high-volume staging history tables.
- Introduce a Spark-based transformation pipeline for high-volume data processing and backfills, with outputs published to mart_spark tables for dashboard consumption.

---
