# airflow-pipeline-templates

Production-grade Apache Airflow DAGs for Amazon MWAA — orchestrating complex multi-step data pipelines across AWS Glue, EMR, Redshift, and Snowflake. Built from real healthcare and enterprise data engineering work at **Optum (UnitedHealth Group)** and **HGS**.

These aren't tutorial DAGs. They handle real production concerns: data availability checks before processing starts, DQ gate failures that halt pipelines before bad data reaches downstream consumers, EMR cost control via sensor reschedule mode, and schema drift alerts that fire before a silent corruption reaches anyone.

---

## DAGs

| File | Pipeline | Schedule |
|---|---|---|
| `healthcare_etl_pipeline.py` | Full Medallion Architecture — Glue → EMR → Snowflake | Daily 02:00 UTC |
| `cdc_incremental_ingestion.py` | CDC-based incremental ingestion — DMS → Glue → Redshift | Every 4 hours |

---

## DAG 1 — `healthcare_etl_pipeline.py`

End-to-end Medallion Architecture pipeline orchestrating Bronze ingestion through Silver transformation to Gold Snowflake load.

### Task Flow

```
check_source_data
├── no_data_skip          ← BranchPythonOperator: graceful exit if no new files
└── trigger_glue_ingestion
        └── validate_bronze_s3        ← S3KeySensor (mode=poke)
                └── create_emr_cluster
                        └── submit_emr_steps
                                ├── wait_bronze_to_silver   ← EmrStepSensor (mode=reschedule)
                                └── wait_silver_to_gold     ← EmrStepSensor (mode=reschedule)
                                        └── validate_row_counts
                                                └── terminate_emr_cluster
                                                        └── load_snowflake
                                                                └── validate_snowflake_load
                                                                        └── notify_success
```

**On any failure:** `TriggerRule.ONE_FAILED` fires `notify_failure` — alert fires even if only one upstream task fails.

### Key Design Decisions

**Branch on data availability**
Checks for new source files before spinning up EMR. No new data = graceful skip, not a pipeline failure. Prevents unnecessary EMR cluster costs and noisy failure alerts on quiet days.

**EMR sensors in `reschedule` mode**
`EmrStepSensor` uses `mode="reschedule"` — releases the Airflow worker slot while waiting for long-running Spark jobs. In MWAA with limited worker concurrency, blocking sensors starve other DAGs.

**Row count validation before Snowflake load**
Silver → Gold row counts are compared before the Snowflake load step proceeds. A row count drop of > 5% halts the pipeline and alerts — catches upstream truncation before it silently empties the warehouse.

**EMR cluster terminated on every run**
No persistent cluster. Cluster is created per run and terminated after Gold step completes — even on failure (cleanup task with `TriggerRule.ALL_DONE`). Eliminates idle EMR cost.

### DAG Config

| Parameter | Value |
|---|---|
| Schedule | `0 2 * * *` (Daily 02:00 UTC) |
| SLA | 4 hours |
| Retries | 2 retries, 15-minute delay |
| Catchup | False |
| Alerting | SNS on success and failure |

---

## DAG 2 — `cdc_incremental_ingestion.py`

CDC-based incremental ingestion pipeline. Captures row-level changes from source databases via AWS DMS and merges them into production Redshift tables using an UPSERT pattern.

### Task Flow

```
check_dms_task_health
        └── detect_schema_drift        ← alerts via SNS if columns changed
                └── trigger_glue_cdc_processing
                        └── run_dq_checks
                                ├── validate_row_count_delta
                                ├── check_null_rates
                                └── detect_duplicate_keys
                                        └── merge_staging_to_production   ← UPSERT
                                                └── publish_dq_summary
                                                        └── notify_success / notify_failure
```

### Key Design Decisions

**DMS health check before processing**
Verifies the DMS replication task is in `running` state before touching any CDC files. A stalled DMS task produces incomplete S3 output — processing it without checking produces silent partial loads.

**Schema drift detection before Glue runs**
Compares current source schema against the expected schema definition stored in config. Fires an SNS alert immediately if any column was added, dropped, or renamed. Glue processing is halted until the schema definition is reviewed and updated — prevents silent downstream corruption.

**UPSERT via DELETE + INSERT on Redshift**
Redshift doesn't support true `MERGE`. The CDC merge uses a staging table pattern:
1. Load CDC batch into a temp staging table
2. `DELETE` matching primary keys from production
3. `INSERT` all rows from staging

More efficient than `UPDATE` on Redshift's columnar storage — avoids row-level updates that fragment blocks.

**DQ gate before merge**
All three DQ checks (row count delta, null rates, duplicate keys) must pass before the merge step runs. A failure in any check halts the pipeline at the DQ stage — bad CDC batches never reach production tables.

### DAG Config

| Parameter | Value |
|---|---|
| Schedule | `0 */4 * * *` (Every 4 hours) |
| Retries | 3 retries, 10-minute delay |
| Catchup | False |
| Alerting | SNS on DQ failure and on successful merge |

---

## Environment Variables (MWAA)

| Variable | Description |
|---|---|
| `environment` | `prod` / `staging` / `dev` |
| `s3_data_lake_bucket` | S3 bucket for data lake |
| `glue_iam_role` | IAM role ARN for Glue jobs |
| `sns_alert_topic_arn` | SNS topic for pipeline alerts |
| `dms_replication_task_arn` | ARN of DMS CDC replication task |

---

## Connections Required

| Connection ID | Type |
|---|---|
| `aws_default` | Amazon Web Services |
| `snowflake_prod` | Snowflake |
| `redshift_prod` | Amazon Redshift |
