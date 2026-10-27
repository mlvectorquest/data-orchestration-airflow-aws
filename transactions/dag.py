import json
from pathlib import Path
from airflow.sdk import DAG, Asset, TaskGroup, task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

CWD                    = Path(__file__).parent
S3_BUCKET              = "{{ var.value.s3_bucket }}"
S3_SQL                 = 'artifacts/transactions/sql/{{ dag_run.run_id }}'
RAW_DB                 = "raw"
TRANSACTIONS_DB        = "transactions"
WAREHOUSE_PATH         = f"s3://{S3_BUCKET}/iceberg-warehouse/transactions/"
PROMOTE_SCRIPT         = CWD / "glue_script.py"
GLUE_ROLE_NAME         = "dev-lakehouse-glue-role"
AWS_CONN_ID            = "aws_default"
ATHENA_CONN_ID         = "athena_default"
REGION                 = 'us-east-1'
SQL_DIR                = Path(__file__).parent / 'sql'

RAW_INGESTION_COMPLETE = Asset("raw_ingestion_complete")
TRANSACTIONS_COMPLETE = Asset("transactions_complete")


# Each table declares its upstream transactions tables in `depends_on`.
# An empty list means the table can be promoted as soon as the branch fires.
TABLES = [
    {
        "table": "users",
        "partition_keys": [],
        "upsert_keys": ["user_id"],
        "depends_on": [],
    },
    {
        "table": "artists",
        "partition_keys": [],
        "upsert_keys": ["artist_id"],
        "depends_on": [],
    },
    {
        "table": "songs",
        "partition_keys": [],
        "upsert_keys": ["song_id"],
        "depends_on": ["artists"],
    },
    {
        "table": "song_versions",
        "partition_keys": [],
        "upsert_keys": ["version_id"],
        "depends_on": ["songs"],
    },
    {
        "table": "user_levels",
        "partition_keys": [],
        "upsert_keys": ["user_id", "changed_at"],
        "depends_on": ["users"],
    },
    {
        "table": "events",
        "partition_keys": ["event_date"],
        "upsert_keys": ["event_id"],
        "depends_on": ["users", "songs", "artists", "song_versions"],
    },
]

with DAG(
    dag_id="transactions",
    schedule=[RAW_INGESTION_COMPLETE],
    max_active_tasks=2,
    max_active_runs=1,
) as dag:


    # Pull metadata from triggering asset event
    @task(inlets=[RAW_INGESTION_COMPLETE])
    def metadata(*, inlet_events, **context) -> dict:

        events = inlet_events[RAW_INGESTION_COMPLETE]

        if not events:
            raise ValueError("No asset events found.")
        
        latest = events[-1]

        return {
            "data_interval" : latest.extra["data_interval"],
        }

    metadata_out = metadata()

    # Build promotion tasks first — store handles in a dict so we can wire
    # cross-table dependencies after every task exists.
    upload_tasks  = {}
    promote_tasks = {}

    for table in TABLES:

        table_name = table["table"]
        SQL        = SQL_DIR / f"{table_name}.sql"
        SQL_KEY    = S3_SQL  + f"/{table_name}.sql"

        # Render the table's SQL and push the rendered query to S3.
        @task(task_id=f"{table_name}_upload_sql")
        def upload(file, s3_bucket, s3_key, **context):

            ti = context["ti"]
            sql_template = file.read_text()
            sql_rendered = ti.task.render_template(sql_template, context)

            hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            hook.load_string(
                string_data=sql_rendered,
                key=s3_key,
                bucket_name=s3_bucket,
                replace=True,
            )

        # Set up table-specific configurations
        payload = {"sql": f"s3://{S3_BUCKET}/{SQL_KEY}"}
        payload.update({k: v for k, v in table.items() if k != "depends_on"})

        # Execute glue script
        promote = GlueJobOperator(
            task_id             = f"promote_{table_name}",
            job_name            = f"transactions_promote_{table_name}",
            script_location     = PROMOTE_SCRIPT.as_posix(),
            s3_bucket           = S3_BUCKET,
            iam_role_name       = GLUE_ROLE_NAME,
            replace_script_file = True,
            verbose             = True,
            region_name         = "us-east-1",
            script_args         = {
                "--config"                  : json.dumps(payload),
                "--datalake-formats"        : "iceberg",
                "--enable-glue-datacatalog" : "",
                "--conf": (
                    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
                    "--conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog "
                    "--conf spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
                    "--conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO "
                    f"--conf spark.sql.catalog.iceberg.warehouse=s3://{S3_BUCKET}/iceberg-warehouse/"
                ),
                "--warehouse_path": f"s3://{S3_BUCKET}/iceberg-warehouse/",
            },
            create_job_kwargs   = {"GlueVersion": "5.0"},
            wait_for_completion = True,
            aws_conn_id         = AWS_CONN_ID,
            trigger_rule        = "none_failed_min_one_success",
        )

        upload_task = upload(SQL, S3_BUCKET, SQL_KEY)
        metadata_out >> upload_task
        upload_tasks[table_name] = upload_task

        # Store the `promote` task in the `promote_tasks` dictionary
        # - Use the table_name variable as the dictionary key
        promote_tasks[table_name] = promote

    # Create a task group called `validate_transactions`
    with TaskGroup("validate_transactions") as validate_transactions:

        for table in TABLES:
            table_name = table["table"]
            pk_expr    = ", ".join(table["upsert_keys"])

            SQLCheckOperator(
                task_id=f"check_{table_name}_unique",
                conn_id=ATHENA_CONN_ID,
                trigger_rule="none_failed_min_one_success",
                sql=f"""
                    SELECT CASE
                        WHEN COUNT(*) = COUNT(DISTINCT ({pk_expr}))
                        THEN 1 ELSE 0
                    END
                    FROM transactions.{table_name}
                """,
            )
            


        SQLCheckOperator(
            task_id="check_events_version_id_nulls",
            conn_id=ATHENA_CONN_ID,
            trigger_rule="none_failed_min_one_success",
            sql="""
                SELECT CASE
                    WHEN COUNT(*) = 0 THEN 1
                    WHEN CAST(SUM(CASE WHEN version_id IS NULL THEN 1 ELSE 0 END) AS DOUBLE)
                         / COUNT(*) < 0.30
                    THEN 1 ELSE 0
                END
                FROM transactions.events
            """,
        )


    # Alter this task so it emits an event indicating transactions
    # have been updated. 
    @task(
        outlets=[TRANSACTIONS_COMPLETE],
        trigger_rule="none_failed"
        )
    def notify_complete() -> None:
        pass
        
    # Wire dependencies
    for table in TABLES:
        table_name = table["table"]

        upload_tasks[table_name] >> promote_tasks[table_name]

        # Set inter table dependencies
        for upstream in table["depends_on"]:
            promote_tasks[upstream] >> promote_tasks[table_name]

        promote_tasks[table_name] >> validate_transactions

    validate_transactions >> notify_complete()
