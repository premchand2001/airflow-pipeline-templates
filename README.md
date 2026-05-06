# airflow-pipeline-templates

Production-grade Apache Airflow DAGs built for Amazon MWAA — based on real data engineering work in healthcare and enterprise environments using Databricks, AWS, and Delta Lake.

## Overview

This repository contains battle-tested Airflow DAG templates used to orchestrate complex multi-step data pipelines on AWS. Each DAG reflects real production patterns — not tutorials. The underlying data transformation layer is built on **Databricks with Delta Lake**, orchestrated end-to-end by Apache Airflow on Amazon MWAA.

---

## DAGs

### 1. `healthcare_etl_pipeline.py`
**End-to-end healthcare claims pipeline**

Orchestrates a full Medallion Architecture pipeline built on Databricks:
- Triggers AWS Glue ingestion job (Bronze layer)
- Validates S3 output with S3KeySensor
- Spins up EMR cluster and submits PySpark steps (Silver + Gold layers)
- Validates row counts between layers
- Terminates EMR cluster after processing
- Loads Gold layer into Snowflake via COPY INTO
- Sends success/failure alerts via Amazon SNS
check_source_data
├── no_data_skip (graceful exit if no data)
└── trigger_glue_ingestion
└── validate_bronze_s3
└── create_emr_cluster
└── submit_emr_steps
├── wait_bronze_to_silver
└── wait_silver_to_gold
└── validate_row_counts
└── terminate_emr_cluster
└── load_snowflake
└── validate_snowflake_load
└── notify_success

**Schedule:** Daily at 02:00 UTC
**SLA:** Must complete within 4 hours
**Retries:** 2 retries with 15-minute delay

---

### 2. `cdc_incremental_ingestion.py`
**CDC-based incremental ingestion pipeline**

Captures real-time changes from source databases via AWS DMS and loads into Amazon Redshift:
- Checks DMS replication task health before proceeding
- Detects schema drift and alerts via SNS
- Processes CDC files from S3 using AWS Glue
- Runs automated data quality checks:
  - Row count validation vs previous run
  - Null rate checks on critical columns
  - Duplicate detection on primary keys
- Merges staging data into production using UPSERT pattern
- Publishes data quality alerts if thresholds are breached

**Schedule:** Every 4 hours
**Retries:** 3 retries with 10-minute delay

---

## Tech Stack

| Tool | Purpose |
|---|---|
| **Databricks** | Data lake processing, Delta Lake management, PySpark transformations |
| **Delta Lake** | ACID-compliant storage — enables time-travel, schema evolution, MERGE operations |
| Apache Airflow 2.x | Pipeline orchestration |
| Amazon MWAA | Managed Airflow environment |
| AWS Glue | Serverless ETL — Bronze layer ingestion |
| AWS EMR | Distributed PySpark processing — Silver and Gold layers |
| Amazon S3 | Data lake storage |
| Amazon Redshift | Cloud data warehouse |
| Snowflake | Analytics warehouse — Gold layer consumption |
| AWS DMS | Change Data Capture |
| Amazon SNS | Alerting and notifications |

---

## Key Patterns Used

**Medallion Architecture (Bronze/Silver/Gold) on Databricks**
Built on Databricks using Delta Lake — each layer enforces progressively stricter data quality rules before data moves forward. Bronze lands raw data, Silver applies PySpark-based validation and deduplication, Gold produces star-schema aggregations for BI consumption.

**Delta Lake MERGE for late-arriving data**
Uses Delta MERGE operations to handle upserts cleanly — late-arriving records are handled without reprocessing entire partitions. Delta time-travel enables fast rollback during incidents without touching source systems.

**Branch Operator for Data Availability Checks**
Pipelines check for source data before starting — gracefully skipping runs with no new data rather than failing.

**EMR Sensor with reschedule mode**
Uses `mode="reschedule"` on EMR step sensors to avoid blocking worker slots during long-running Spark jobs.

**UPSERT pattern in Redshift**
CDC merges use a DELETE + INSERT pattern rather than UPDATE to work efficiently with Redshift's columnar storage.

**Schema Drift Detection**
Every run compares the current table schema against an expected schema definition and alerts immediately if columns change.

**SNS Alerting on both success and failure**
Uses `TriggerRule.ONE_FAILED` to ensure failure notifications fire even if only one task in the chain fails.

---

## Environment Variables (MWAA)

| Variable | Description |
|---|---|
| `environment` | prod / staging / dev |
| `s3_data_lake_bucket` | S3 bucket for data lake |
| `glue_iam_role` | IAM role for Glue jobs |
| `sns_alert_topic_arn` | SNS topic for pipeline alerts |
| `dms_replication_task_arn` | ARN of DMS CDC replication task |

---

## Connections Required

| Connection ID | Type |
|---|---|
| `aws_default` | Amazon Web Services |
| `snowflake_prod` | Snowflake |
| `redshift_prod` | Amazon Redshift |

---

## Author

**Premchand Kothapalli**
Data Engineer | AWS | Databricks | PySpark | Airflow | Snowflake
[LinkedIn](https://linkedin.com/in/pc-kothapalli) | premchandkdata@gmail.com
[GitHub](https://github.com/premchand2001)
