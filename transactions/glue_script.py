import sys
import json
import boto3
import uuid
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# Use getResolvedOptions to collect arguments passed to the script
# Isolate the "config" argument
args = getResolvedOptions(sys.argv, ["config"])

config = json.loads(args["config"])

table_name = config['table']
sql_s3_path = config['sql']
upsert_keys = config['upsert_keys']
partition_keys = config.get('partition_keys', [])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Unique view for parallel safety
unique_stg = f"stg_{table_name}_{str(uuid.uuid4())[:8]}"

bucket, key = sql_s3_path.replace("s3://", "").split("/", 1)
s3 = boto3.client("s3")
sql_query = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")

stg_df = spark.sql(sql_query)

# Drop duplicates on upsert keys
stg_df = stg_df.dropDuplicates(upsert_keys)

if partition_keys:
    stg_df = stg_df.sortWithinPartitions(*partition_keys)

stg_df.createOrReplaceTempView(unique_stg)

target_table = f"iceberg.transactions.{table_name}"

if not spark.catalog.tableExists(target_table):
    writer = stg_df.writeTo(target_table).using("iceberg")
    if partition_keys:
        writer = writer.partitionedBy(*partition_keys)
    writer.create()
else:

    target_df = spark.table(target_table)
    new_cols = set(stg_df.columns) - set(target_df.columns)
    
    if new_cols:

        for col in new_cols:
            # Iceberg-compatible SQL type (e.g., DECIMAL(10,2))
            col_type = stg_df.schema[col].dataType.simpleString()
            spark.sql(f"ALTER TABLE {target_table} ADD COLUMN {col} {col_type}")


    # Match on upsert keys only. Adding partition keys to the ON clause (for partition
    # pruning) made the events merge fail with: "Couldn't find event_date#9 in [...]".
    # Suspected Spark/Iceberg planner issue with partition columns in a merge condition.
    on_clause = " AND ".join([f"t.{k} = s.{k}" for k in upsert_keys])
    
    spark.sql(f"""
    MERGE INTO {target_table} t
    USING {unique_stg} s
    ON {on_clause}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")


print(f"Completed Iceberg merge for {table_name}")
