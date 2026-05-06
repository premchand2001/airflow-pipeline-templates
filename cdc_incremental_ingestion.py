"""
DAG: cdc_incremental_ingestion.py
Description: CDC-based incremental ingestion pipeline — captures real-time changes
             from source databases via AWS DMS and loads into Amazon Redshift.
             Includes automated data quality checks and schema drift alerts via SNS.
Author: Premchand Kothapalli
Environment: Amazon MWAA
Schedule: Every 4 hours
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
import boto3
import logging
import json

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Variables
# ─────────────────────────────────────────────
ENV = Variable.get("environment", default_var="prod")
S3_BUCKET = Variable.get("s3_bucket")
SNS_TOPIC_ARN = Variable.get("sns_alert_topic_arn")
DMS_REPLICATION_TASK_ARN = Variable.get("dms_replication_task_arn")
AWS_CONN_ID = "aws_default"
REDSHIFT_CONN_ID = "redshift_prod"

# ─────────────────────────────────────────────
# Data quality thresholds
# ─────────────────────────────────────────────
NULL_THRESHOLD_PCT = 5.0       # Max allowed null % per column
DUPLICATE_THRESHOLD_PCT = 1.0  # Max allowed duplicate % per table
ROW_COUNT_DROP_THRESHOLD = 20  # Alert if row count drops more than 20% vs previous run


def check_dms_task_status(**context):
    """
    Checks if the DMS replication task is running and healthy.
    Raises an exception if the task is in a failed or stopped state.
    """
    dms_client = boto3.client("dms")
    response = dms_client.describe_replication_tasks(
        Filters=[{"Name": "replication-task-arn", "Values": [DMS_REPLICATION_TASK_ARN]}]
    )

    tasks = response.get("ReplicationTasks", [])
    if not tasks:
        raise ValueError(f"DMS task not found: {DMS_REPLICATION_TASK_ARN}")

    task = tasks[0]
    status = task["Status"]
    logger.info(f"DMS task status: {status}")

    if status not in ["running", "starting"]:
        raise ValueError(
            f"DMS replication task is not running. Current status: {status}. "
            "Please investigate before proceeding."
        )

    context["task_instance"].xcom_push(
        key="dms_status",
        value={"status": status, "task_arn": DMS_REPLICATION_TASK_ARN}
    )
    logger.info("DMS task is healthy. Proceeding with ingestion.")


def run_data_quality_checks(**context):
    """
    Runs data quality checks on the newly ingested CDC data in Redshift:
    - Row count validation vs previous run
    - Null checks on critical columns
    - Duplicate detection on primary keys
    - Schema drift detection
    Publishes alerts to SNS if thresholds are breached.
    """
    hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
    run_date = context["ds"]
    alerts = []

    # ── 1. Row count check ───────────────────
    row_count_sql = f"""
        SELECT
            COUNT(*) AS current_count,
            (
                SELECT COUNT(*)
                FROM staging.cdc_claims_staging
                WHERE load_date = DATEADD(day, -1, '{run_date}')
            ) AS previous_count
        FROM staging.cdc_claims_staging
        WHERE load_date = '{run_date}';
    """
    result = hook.get_first(row_count_sql)
    current_count, previous_count = result[0], result[1]
    logger.info(f"Row counts — current: {current_count}, previous: {previous_count}")

    if previous_count and previous_count > 0:
        drop_pct = ((previous_count - current_count) / previous_count) * 100
        if drop_pct > ROW_COUNT_DROP_THRESHOLD:
            alerts.append(
                f"⚠️ Row count dropped by {drop_pct:.1f}% "
                f"({previous_count} → {current_count}) for {run_date}"
            )

    # ── 2. Null check on critical columns ────
    null_check_sql = f"""
        SELECT
            SUM(CASE WHEN claim_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS claim_id_null_pct,
            SUM(CASE WHEN member_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS member_id_null_pct,
            SUM(CASE WHEN service_date IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS service_date_null_pct
        FROM staging.cdc_claims_staging
        WHERE load_date = '{run_date}';
    """
    null_result = hook.get_first(null_check_sql)
    columns = ["claim_id", "member_id", "service_date"]
    for col, null_pct in zip(columns, null_result):
        if null_pct and null_pct > NULL_THRESHOLD_PCT:
            alerts.append(
                f"⚠️ High null rate on {col}: {null_pct:.2f}% (threshold: {NULL_THRESHOLD_PCT}%)"
            )

    # ── 3. Duplicate detection ───────────────
    duplicate_sql = f"""
        SELECT
            (COUNT(*) - COUNT(DISTINCT claim_id)) * 100.0 / NULLIF(COUNT(*), 0) AS duplicate_pct
        FROM staging.cdc_claims_staging
        WHERE load_date = '{run_date}';
    """
    dup_result = hook.get_first(duplicate_sql)
    duplicate_pct = dup_result[0] or 0
    logger.info(f"Duplicate percentage: {duplicate_pct:.2f}%")

    if duplicate_pct > DUPLICATE_THRESHOLD_PCT:
        alerts.append(
            f"⚠️ Duplicate records detected: {duplicate_pct:.2f}% "
            f"(threshold: {DUPLICATE_THRESHOLD_PCT}%)"
        )

    # ── 4. Push quality results to XCom ─────
    quality_report = {
        "run_date": run_date,
        "current_row_count": current_count,
        "duplicate_pct": float(duplicate_pct),
        "alerts": alerts,
        "passed": len(alerts) == 0,
    }
    context["task_instance"].xcom_push(key="quality_report", value=quality_report)

    if alerts:
        logger.warning(f"Data quality issues found: {alerts}")
    else:
        logger.info("All data quality checks passed.")


def publish_quality_alerts(**context):
    """
    Publishes data quality alerts to SNS if any checks failed.
    Continues pipeline even on quality warnings — does not block load.
    """
    quality_report = context["task_instance"].xcom_pull(
        task_ids="run_data_quality_checks",
        key="quality_report"
    )

    if not quality_report or quality_report.get("passed"):
        logger.info("No quality alerts to publish.")
        return

    alerts = quality_report.get("alerts", [])
    run_date = quality_report.get("run_date")

    sns_client = boto3.client("sns")
    message = (
        f"Data Quality Alerts — CDC Claims Pipeline ({run_date})\n\n"
        + "\n".join(alerts)
        + f"\n\nTotal rows loaded: {quality_report.get('current_row_count')}"
    )

    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"[DQ ALERT] CDC Claims Pipeline - {run_date}",
        Message=message,
    )
    logger.info(f"Published {len(alerts)} data quality alerts to SNS.")


def load_staging_to_production(**context):
    """
    Merges validated staging data into the production Redshift table
    using an UPSERT pattern — updates existing records and inserts new ones.
    """
    hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
    run_date = context["ds"]

    merge_sql = f"""
        BEGIN;

        -- Step 1: Delete existing records that will be updated
        DELETE FROM production.claims_fact
        WHERE claim_id IN (
            SELECT DISTINCT claim_id
            FROM staging.cdc_claims_staging
            WHERE load_date = '{run_date}'
        );

        -- Step 2: Insert all records from staging (new + updated)
        INSERT INTO production.claims_fact (
            claim_id,
            member_id,
            provider_id,
            service_date,
            claim_amount,
            diagnosis_code,
            procedure_code,
            claim_status,
            cdc_operation,
            loaded_at,
            partition_date
        )
        SELECT
            claim_id,
            member_id,
            provider_id,
            service_date,
            claim_amount,
            diagnosis_code,
            procedure_code,
            claim_status,
            cdc_operation,
            GETDATE() AS loaded_at,
            '{run_date}'::DATE AS partition_date
        FROM staging.cdc_claims_staging
        WHERE load_date = '{run_date}'
          AND cdc_operation != 'D';  -- exclude deleted records

        COMMIT;
    """

    hook.run(merge_sql)
    logger.info(f"Staging to production merge completed for {run_date}.")


def detect_schema_drift(**context):
    """
    Compares current staging table schema against expected schema definition.
    Raises an alert if new columns appear or existing columns change data types.
    """
    hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)

    schema_sql = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'staging'
          AND table_name = 'cdc_claims_staging'
        ORDER BY ordinal_position;
    """

    current_schema = {row[0]: row[1] for row in hook.get_records(schema_sql)}

    expected_schema = {
        "claim_id": "character varying",
        "member_id": "character varying",
        "provider_id": "character varying",
        "service_date": "date",
        "claim_amount": "double precision",
        "diagnosis_code": "character varying",
        "procedure_code": "character varying",
        "claim_status": "character varying",
        "cdc_operation": "character varying",
        "load_date": "date",
    }

    drift_detected = []
    for col, expected_type in expected_schema.items():
        if col not in current_schema:
            drift_detected.append(f"Missing column: {col}")
        elif current_schema[col] != expected_type:
            drift_detected.append(
                f"Type mismatch on {col}: expected {expected_type}, got {current_schema[col]}"
            )

    new_columns = set(current_schema.keys()) - set(expected_schema.keys())
    for col in new_columns:
        drift_detected.append(f"New column detected: {col} ({current_schema[col]})")

    if drift_detected:
        sns_client = boto3.client("sns")
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="[SCHEMA DRIFT] CDC Claims Pipeline",
            Message=f"Schema drift detected:\n\n" + "\n".join(drift_detected),
        )
        logger.warning(f"Schema drift detected: {drift_detected}")
    else:
        logger.info("No schema drift detected. Schema is consistent.")


# ─────────────────────────────────────────────
# Default args
# ─────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2020, 9, 1),
    "email": ["de-alerts@hgs.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}

# ─────────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────────
with DAG(
    dag_id="cdc_incremental_ingestion",
    default_args=default_args,
    description="CDC-based incremental ingestion — DMS → S3 → Redshift with data quality monitoring",
    schedule_interval="0 */4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["cdc", "redshift", "ingestion", "data-quality", "hgs"],
) as dag:

    # ── 1. Check DMS task health ──────────────────────────────
    check_dms = PythonOperator(
        task_id="check_dms_task_status",
        python_callable=check_dms_task_status,
        provide_context=True,
    )

    # ── 2. Detect schema drift before processing ──────────────
    schema_drift_check = PythonOperator(
        task_id="detect_schema_drift",
        python_callable=detect_schema_drift,
        provide_context=True,
    )

    # ── 3. Run Glue job — process CDC files from S3 ───────────
    process_cdc_files = GlueJobOperator(
        task_id="process_cdc_files",
        job_name="cdc-claims-s3-to-redshift-staging",
        script_args={
            "--run_date": "{{ ds }}",
            "--s3_input_path": f"s3://{S3_BUCKET}/dms-output/claims/",
            "--redshift_table": "staging.cdc_claims_staging",
            "--enable_dedup": "true",
        },
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
    )

    # ── 4. Run data quality checks ────────────────────────────
    dq_checks = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=run_data_quality_checks,
        provide_context=True,
    )

    # ── 5. Publish quality alerts if needed ───────────────────
    publish_alerts = PythonOperator(
        task_id="publish_quality_alerts",
        python_callable=publish_quality_alerts,
        provide_context=True,
    )

    # ── 6. Merge staging → production ────────────────────────
    merge_to_production = PythonOperator(
        task_id="load_staging_to_production",
        python_callable=load_staging_to_production,
        provide_context=True,
    )

    # ── 7. Success notification ───────────────────────────────
    notify_success = SnsPublishOperator(
        task_id="notify_success",
        target_arn=SNS_TOPIC_ARN,
        message="✅ CDC ingestion pipeline completed for {{ ds }} {{ execution_date.strftime('%H:%M') }} UTC.",
        subject="[SUCCESS] CDC Claims Pipeline - {{ ds }}",
        aws_conn_id=AWS_CONN_ID,
    )

    # ── 8. Failure notification ───────────────────────────────
    notify_failure = SnsPublishOperator(
        task_id="notify_failure",
        target_arn=SNS_TOPIC_ARN,
        message="❌ CDC ingestion pipeline FAILED for {{ ds }}. Check MWAA logs immediately.",
        subject="[FAILED] CDC Claims Pipeline - {{ ds }}",
        aws_conn_id=AWS_CONN_ID,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ─────────────────────────────────────────────
    # DAG dependency chain
    # ─────────────────────────────────────────────
    check_dms >> schema_drift_check >> process_cdc_files
    process_cdc_files >> dq_checks
    dq_checks >> publish_alerts
    publish_alerts >> merge_to_production
    merge_to_production >> notify_success

    [process_cdc_files, dq_checks, merge_to_production] >> notify_failure
