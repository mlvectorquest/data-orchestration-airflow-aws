from pathlib import Path
from airflow.sdk import DAG, Asset, task, Param, TaskGroup

# Import the GlueJob Operator
#### YOUR CODE HERE

# Import the SQLCheckOperator
#### YOUR CODE HERE

# Import the S3Hook
#### YOUR CODE HERE

# ── Config ─────────────────────────────────────────────────────────────────────
S3_BUCKET = "{{ var.value.s3_bucket }}"

# Create a variable named LANDING_PREFIX
# Assign the variable to a string representing 
# the top level folder where data intervals are stored
#### YOUR CODE HERE

# Create a variable named WAREHOUSE_LOCATION 
# Assign the variable to an S3 uri pointing to 
# the location where the iceberg warehouse should be stored
#  (format s3://bucket/location)
#### YOUR CODE HERE

# Create a variable named ICEBERG_SCRIPT that points to the absolute path
# for the glue_script within the raw directory
#### YOUR CODE HERE

# Create variables for the connection ids here
#### YOUR CODE HERE

# Keep this line unchanged
REGION = "us-east-1"

# Initialize an Asset that is emitted by the `run_pipeline` dag.
#### YOUR CODE HERE

# Initialize an Asset to represent completion of raw ingestion
#### YOUR CODE HERE

# Initialize a DAG 
# - Set the id to "raw"
# - Schedule the dag to trigger when the run_pipeline dag completes
# - Set maximum active runs to 1
# - Set maximum active tasks to 2
#### YOUR CODE HERE

    # Define an airflow python task named `capture_landing_keys`
    # - Set the task decorator argument `inlets` to the Asset that triggers
    #   the raw dag
    # - Include the following parameters in the function signature:
    #   - `s3_bucket` a templated string argument. Renders the s3_bucket name at runtime
    #   - `inlet_events` (context variable used for accessing that tasks's inlet events)
    #### YOUR CODE HERE
        
        # Pull the data interval value from the 
        # latest inlet Asset events metadata
        #### YOUR CODE HERE

        # Initialize an S3Hook
        #### YOUR CODE HERE

        # Using the `LANDING_PREFIX` variable and the data_interval
        # pulled from the Asset, define the S3 prefix
        # for the data_interval folder (format: <landing>/<data_interval>/)
        #### YOUR CODE HERE

        # Use the S3Hook's `list_prefix` method
        # to list the table prefixes stored in the data_interval
        # Push the output to the xcom
        #### YOUR CODE HERE

    # Keep this line unchanged
    table_keys = capture_landing_keys(S3_BUCKET)

    # Fill in the partial configurations for the GlueJobOperator
    submit_glue_jobs = GlueJobOperator.partial(
        # Set the task_id to `ingest`
        #### YOUR CODE HERE

        # Set the aws_conn_id
        #### YOUR CODE HERE

        # Set the script_location to the absolute path
        # for the raw glue script
        #### YOUR CODE HERE

        # Set the s3_bucket to your templated S3_BUCKET variable
        #### YOUR CODE HERE

        # Set the iam_role_name to the role defined in this file
        #### YOUR CODE HERE

        # Set the reion_name to the region defined in this file
        #### YOUR CODE HERE

        # Set the attribute that ensures downstream tasks
        # wait for this task to complete
        #### YOUR CODE HERE

        # Set the attribute that ensures the glue script is replaced
        # each time this task is executed
        #### YOUR CODE HERE

        # Keep this line unchanged
        map_index_template="{{ task.script_args['--table'] }}",

        # Set the attribute that ensures glue logs flow into the airflow UI
        #### YOUR CODE HERE
        create_job_kwargs={
            "GlueVersion": "5.0",
            "NumberOfWorkers": 2,
            "WorkerType": "G.1X",
            "DefaultArguments": {
                # Set the datalake format to "iceberg"
                "--datalake-formats": "iceberg",
                "--conf": (
                    # Register Iceberg's SQL extensions with Spark — adds support for
                    # MERGE INTO, CALL procedures, and other Iceberg-specific syntax.
                    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
                    # Define a Spark catalog named 'iceberg' backed by Iceberg's SparkCatalog
                    # implementation. Tables addressed as iceberg.<db>.<table> route through this.
                    "--conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog "
                    # Use AWS Glue Data Catalog as the metadata store for this catalog —
                    # databases and tables are persisted to Glue, visible to Athena.
                    "--conf spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
                    # Use Iceberg's native S3 file IO for reading and writing table data —
                    # avoids HDFS-style overhead and integrates with Iceberg's S3 optimizations.
                    "--conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO "
                    # Root S3 location where this catalog stores Iceberg metadata and data files.
                    # New tables created via this catalog land under <warehouse>/<db>/<table>/.
                    f"--conf spark.sql.catalog.iceberg.warehouse={WAREHOUSE_LOCATION} "
                    # Only overwrite partitions that the incoming dataframe touches; leave
                    # other partitions intact. Required for partition-level upserts/replaces.
                    "--conf spark.sql.sources.partitionOverwriteMode=dynamic"
                ),
            },
        }
    ).expand_kwargs(
        table_keys.map(lambda key: {
            "job_name": f"ingest_raw_{key.strip('/').split('/')[-1]}",
            "script_args": {
                "--table": key.strip("/").split("/")[-1],
                "--landing_path": f"s3://{S3_BUCKET}/{key}",
                "--data_interval": "{{ triggering_asset_events.for_asset(name='raw_ingestion_pending')[-1].extra['data_interval'] }}"
            }
        })
    )

    with TaskGroup("validate_raw") as validate_raw:

        # Pass the following SQL query to an SQLCheckOperator
        # Use an athena connection id
        #### YOUR CODE HERE
        """
            SELECT COUNT(*) FROM raw.logs
            WHERE data_interval = '{{ triggering_asset_events.for_asset(name='raw_ingestion_pending')[-1].extra['data_interval'] }}'
        """
            

        # Pass the following SQL query to an SQLCheckOperator
        # Use an athena connection id
        #### YOUR CODE HERE
        """
            SELECT COUNT(*) FROM raw.songs
            WHERE data_interval = '{{ triggering_asset_events.for_asset(name='raw_ingestion_pending')[-1].extra['data_interval'] }}'
        """
            

    # Define a task that sets `outlets` to the 
    # Asset used to trigger the transactions dag
    #### YOUR CODE HERE

        # Add data_interval to the outlet's metadata
        #### YOUR CODE HERE

    # Set task dependencies
    # The glue job, validations, and the notification task
    # should run in sequential order
    #### YOUR CODE HERE