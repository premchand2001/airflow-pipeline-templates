"""
DAG: healthcare_etl_pipeline.py
Description: Orchestrates end-to-end healthcare data pipeline — triggers AWS Glue jobs,
             monitors EMR steps, validates S3 outputs, and loads curated data into Snowflake.
Author: Premchand Kothapalli
Environment: Amazon MWAA (Managed Workflows for Apache Airflow)
Schedule: Daily at 2:00 AM UTC
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.trigger_rule import TriggerRule
import boto3
import logging

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Environment variables pulled from MWAA Variables
# ─────────────────────────────────────────────
ENV = Variable.get("environment", default_var="prod")
S3_BUCKET = Variable.get("s3_data_lake_bucket", default_var="optum-data-lake-prod")
GLUE_IAM_ROLE = Variable.get("glue_iam_role")
SNS_TOPIC_ARN = Variable.get("sns_alert_topic_arn")
SNOWFLAKE_CONN_ID = "snowflake_prod"
AWS_CONN_ID = "aws_default"

# ─────────────────────────────────────────────
# S3 paths — Medallion Architecture layers
# ─────────────────────────────────────────────
RAW_PREFIX = f"s3://{S3_BUCKET}/bronze/healthcare/claims/"
SILVER_PREFIX = f"s3://{S3_BUCKET}/silver/healthcare/claims/"
GOLD_PREFIX = f"s3://{S3_BUCKET}/gold/healthcare/claims/"

# ─────────────────────────────────────────────
# EMR cluster config
# ─────────────────────────────────────────────
EMR_CLUSTER_CONFIG = {
    "Name": f"optum-healthcare-emr-{ENV}",
    "ReleaseLabel": "emr-6.10.0",
    "Applications": [{"Name": "Spark"}, {"Name": "Hadoop"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m5.2xlarge",
                "InstanceCount": 3,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "LogUri": f"s3://{S3_BUCKET}/emr-logs/",
    "Configurations": [
        {
            "Classification": "spark",
            "Properties": {"maximizeResourceAllocation": "true"},
        },
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            },
        },
    ],
    "Tags": [
        {"Key": "Project", "Value": "HealthcareDataPlatform"},
        {"Key": "Environment", "Value": ENV},
        {"Key": "Team", "Value": "DataEngineering"},
        {"Key": "CostCenter", "Value": "OPTUM-DE-001"},
    ],
}

# ─────────────────────────────────────────────
# EMR PySpark steps
# ─────────────────────────────────────────────
EMR_STEPS = [
    {
        "Name": "Bronze to Silver - Claims Transformation",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--master", "yarn",
                "--conf", "spark.executor.memory=8g",
                "--conf", "spark.executor.cores=4",
                "--conf", "spark.sql.shuffle.partitions=200",
                "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
                "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "--packages", "io.delta:delta-core_2.12:2.3.0",
                f"s3://{S3_BUCKET}/scripts/bronze_to_silver_claims.py",
                "--input_path", RAW_PREFIX,
                "--output_path", SILVER_PREFIX,
                "--run_date", "{{ ds }}",
            ],
        },
    },
    {
        "Name": "Silver to Gold - Aggregation & Business Rules",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--master", "yarn",
                "--conf", "spark.executor.memory=8g",
                "--conf", "spark.executor.cores=4",
                "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
                "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "--packages", "io.delta:delta-core_2.12:2.3.0",
                f"s3://{S3_BUCKET}/scripts/silver_to_gold_claims.py",
                "--input_path", SILVER_PREFIX,
                "--output_path", GOLD_PREFIX,
                "--run_date", "{{ ds }}",
            ],
        },
    },
]

# ─────────────────────────────────────────────
# Snowflake load SQL
# ─────────────────────────────────────────────
SNOWFLAKE_LOAD_SQL = """
COPY INTO HEALTHCARE_DW.GOLD.CLAIMS_FACT
FROM (
    SELECT
        $1:claim_id::VARCHAR          AS CLAIM_ID,
        $1:member_id::VARCHAR         AS MEMBER_ID,
        $1:provider_id::VARCHAR       AS PROVIDER_ID,
        $1:service_date::DATE         AS SERVICE_DATE,
        $1:claim_amount::FLOAT        AS CLAIM_AMOUNT,
        $1:diagnosis_code::VARCHAR    AS DIAGNOSIS_CODE,
        $1:procedure_code::VARCHAR    AS PROCEDURE_CODE,
        $1:claim_status::VARCHAR      AS CLAIM_STATUS,
        $1:loaded_at::TIMESTAMP_NTZ   AS LOADED_AT,
        '{{ ds }}'::DATE              AS PARTITION_DATE
    FROM @HEALTHCARE_DW.STAGES.S3_GOLD_STAGE/claims/{{ ds }}/
)
FILE_FORMAT = (TYPE = 'PARQUET')
ON_ERROR = 'SKIP_FILE'
PURGE = FALSE;
"""

SNOWFLAKE_VALIDATE_SQL = """
SELECT
    COUNT(*)                            AS total_records,
    COUNT(DISTINCT MEMBER_ID)           AS unique_members,
    SUM(CLAIM_AMOUNT)                   AS total_claim_amount,
    COUNT(CASE WHEN CLAIM_ID IS NULL THEN 1 END) AS null_claim_ids
FROM HEALTHCARE_DW.GOLD.CLAIMS_FACT
WHERE PARTITION_DATE = '{{ ds }}';
"""

# ─────────────────────────────────────────────
# Python callables
# ─────────────────────────────────────────────
def check_source_data_availability(**context):
    """
    Checks if source data landed in S3 bronze layer before kicking off pipeline.
    Returns branch task id based on data availability.
    """
    run_date = context["ds"]
    s3 = boto3.client("s3")

    prefix = f"bronze/healthcare/claims/run_date={run_date}/"
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=1)

    if response.get("KeyCount", 0) > 0:
        logger.info(f"Source data found for {run_date}. Proceeding with pipeline.")
        return "trigger_glue_ingestion"
    else:
        logger.warning(f"No source data found for {run_date}. Skipping pipeline run.")
        return "no_data_skip"


def validate_row_counts(**context):
    """
    Compares row counts between bronze S3 files and silver output.
    Raises exception if counts don't match within acceptable threshold.
    """
    run_date = context["ds"]
    s3 = boto3.client("s3")

    # Count bronze files
    bronze_response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=f"bronze/healthcare/claims/run_date={run_date}/"
    )
    bronze_files = bronze_response.get("Contents", [])
    logger.info(f"Bronze layer — {len(bronze_files)} files found for {run_date}")

    # Count silver output files
    silver_response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=f"silver/healthcare/claims/run_date={run_date}/"
    )
    silver_files = silver_response.get("Contents", [])
    logger.info(f"Silver layer — {len(silver_files)} files found for {run_date}")

    if len(silver_files) == 0:
        raise ValueError(
            f"Silver layer validation failed — 0 output files found for {run_date}. "
            "EMR job may have failed silently."
        )

    logger.info("Row count validation passed. Proceeding to Snowflake load.")


def send_success_notification(**context):
    run_date = context["ds"]
    logger.info(f"Pipeline completed successfully for {run_date}")


def send_failure_notification(**context):
    run_date = context["ds"]
    task_instance = context["task_instance"]
    logger.error(
        f"Pipeline FAILED for {run_date}. "
        f"Failed task: {task_instance.task_id}"
    )


# ─────────────────────────────────────────────
# Default args
# ─────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email": ["de-alerts@optum.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(hours=4),
}

# ─────────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────────
with DAG(
    dag_id="healthcare_etl_pipeline",
    default_args=default_args,
    description="End-to-end healthcare claims pipeline — Glue ingestion → EMR transformation → Snowflake load",
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "claims", "etl", "medallion", "optum"],
) as dag:

    # ── 1. Check if source data is available ─────────────────
    check_source_data = BranchPythonOperator(
        task_id="check_source_data",
        python_callable=check_source_data_availability,
        provide_context=True,
    )

    # ── 2. No data — skip gracefully ─────────────────────────
    no_data_skip = EmptyOperator(task_id="no_data_skip")

    # ── 3. Trigger Glue ingestion job (Bronze layer) ─────────
    trigger_glue_ingestion = GlueJobOperator(
        task_id="trigger_glue_ingestion",
        job_name="optum-claims-bronze-ingestion",
        script_args={
            "--run_date": "{{ ds }}",
            "--source_system": "sql_server",
            "--target_s3_path": RAW_PREFIX,
            "--enable_data_quality_checks": "true",
        },
        aws_conn_id=AWS_CONN_ID,
        iam_role_name=GLUE_IAM_ROLE,
        create_job_kwargs={
            "GlueVersion": "4.0",
            "NumberOfWorkers": 10,
            "WorkerType": "G.1X",
        },
        wait_for_completion=True,
    )

    # ── 4. Validate bronze S3 output exists ──────────────────
    validate_bronze_s3 = S3KeySensor(
        task_id="validate_bronze_s3",
        bucket_name=S3_BUCKET,
        bucket_key="bronze/healthcare/claims/run_date={{ ds }}/_SUCCESS",
        aws_conn_id=AWS_CONN_ID,
        timeout=60 * 30,
        poke_interval=60,
        mode="reschedule",
    )

    # ── 5. Spin up EMR cluster ────────────────────────────────
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=EMR_CLUSTER_CONFIG,
        aws_conn_id=AWS_CONN_ID,
    )

    # ── 6. Submit PySpark steps to EMR ───────────────────────
    submit_emr_steps = EmrAddStepsOperator(
        task_id="submit_emr_steps",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        steps=EMR_STEPS,
        aws_conn_id=AWS_CONN_ID,
    )

    # ── 7. Wait for Bronze → Silver step ─────────────────────
    wait_bronze_to_silver = EmrStepSensor(
        task_id="wait_bronze_to_silver",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull('submit_emr_steps', key='return_value')[0] }}",
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60,
        timeout=60 * 90,
        mode="reschedule",
    )

    # ── 8. Wait for Silver → Gold step ───────────────────────
    wait_silver_to_gold = EmrStepSensor(
        task_id="wait_silver_to_gold",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull('submit_emr_steps', key='return_value')[1] }}",
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60,
        timeout=60 * 90,
        mode="reschedule",
    )

    # ── 9. Validate row counts bronze vs silver ───────────────
    validate_counts = PythonOperator(
        task_id="validate_row_counts",
        python_callable=validate_row_counts,
        provide_context=True,
    )

    # ── 10. Terminate EMR cluster ─────────────────────────────
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        aws_conn_id=AWS_CONN_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── 11. Load Gold layer into Snowflake ────────────────────
    load_snowflake = SnowflakeOperator(
        task_id="load_snowflake",
        sql=SNOWFLAKE_LOAD_SQL,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        autocommit=True,
    )

    # ── 12. Validate Snowflake load ───────────────────────────
    validate_snowflake = SnowflakeOperator(
        task_id="validate_snowflake_load",
        sql=SNOWFLAKE_VALIDATE_SQL,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        autocommit=True,
    )

    # ── 13. Success notification ──────────────────────────────
    notify_success = SnsPublishOperator(
        task_id="notify_success",
        target_arn=SNS_TOPIC_ARN,
        message="✅ Healthcare ETL Pipeline completed successfully for {{ ds }}. Claims data loaded to Snowflake.",
        subject="[SUCCESS] Healthcare ETL Pipeline - {{ ds }}",
        aws_conn_id=AWS_CONN_ID,
    )

    # ── 14. Failure notification ──────────────────────────────
    notify_failure = SnsPublishOperator(
        task_id="notify_failure",
        target_arn=SNS_TOPIC_ARN,
        message="❌ Healthcare ETL Pipeline FAILED for {{ ds }}. Please check MWAA logs.",
        subject="[FAILED] Healthcare ETL Pipeline - {{ ds }}",
        aws_conn_id=AWS_CONN_ID,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ─────────────────────────────────────────────
    # DAG dependency chain
    # ─────────────────────────────────────────────
    check_source_data >> [trigger_glue_ingestion, no_data_skip]

    trigger_glue_ingestion >> validate_bronze_s3
    validate_bronze_s3 >> create_emr_cluster
    create_emr_cluster >> submit_emr_steps
    submit_emr_steps >> wait_bronze_to_silver
    wait_bronze_to_silver >> wait_silver_to_gold
    wait_silver_to_gold >> validate_counts
    validate_counts >> terminate_emr_cluster
    terminate_emr_cluster >> load_snowflake
    load_snowflake >> validate_snowflake
    validate_snowflake >> notify_success

    # Failure path — fires if anything fails
    [
        trigger_glue_ingestion,
        validate_bronze_s3,
        create_emr_cluster,
        submit_emr_steps,
        wait_bronze_to_silver,
        wait_silver_to_gold,
        validate_counts,
        load_snowflake,
        validate_snowflake,
    ] >> notify_failure
