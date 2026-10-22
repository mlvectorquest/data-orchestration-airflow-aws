import sys
import json
import boto3
import uuid
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# Use getResolvedOptions to collect arguments passed to the script
# Isolate the "config" argument
#### YOUR CODE HERE

# Use json.loads to convert the config argument 
# to a python dictionary
#### YOUR CODE HERE

table_name = config['table']
sql_s3_path = config['sql']
upsert_keys = config['upsert_keys']
partition_keys = config.get('partition_keys', [])

# Initialize a spark context
#### YOUR CODE HERE

# Wrap the spark context in a GlueContext
#### YOUR CODE HERE

# Store the glue context's spark_session to the variable `spark`
#### YOUR CODE HERE


# Unique view for parallel safety
unique_stg = f"stg_{table_name}_{str(uuid.uuid4())[:8]}"

# Using boto3, read the file stored at the bucket an key defined above
# Store the file's string content as the variable `sql_query`
#### YOUR CODE HERE

# Use spark to execute the query.
# Store the output to the variable `stg_df`
#### YOUR CODE HERE

# Drop duplicates on upsert keys
#### YOUR CODE HERE

if partition_keys:
    # Sort the dataframe within partitions. Apply this change inplace
    #### YOUR CODE HERE

# Create staging table Temporary View using 
# the `unique_stg` variable defined above
#### YOUR CODE HERE

target_table = f"iceberg.transactions.{table_name}"

# Check if the production table exists
#### YOUR CODE HERE

    # If it doesn't exist, write the staging table
    # to the production table using iceberg
    #### YOUR CODE HERE
    
    # If the table has partition keys (defined in the dag.py)
    # configure the table to partitioned by those columns
    #### YOUR CODE HERE
    
    # Create the table
    #### YOUR CODE HERE
else:

    target_df = spark.table(target_table)
    new_cols = set(stg_df.columns) - set(target_df.columns)
    
    if new_cols:

        for col in new_cols:
            # Iceberg-compatible SQL type (e.g., DECIMAL(10,2))
            col_type = stg_df.schema[col].dataType.simpleString()
            spark.sql(f"ALTER TABLE {target_table} ADD COLUMN {col} {col_type}")


    # Inclusion of partition keys in 'on_clause' enables Partition Pruning
    on_clause = " AND ".join([f"t.{k} = s.{k}" for k in upsert_keys])
    
    # Using spark, merge the staging table's data
    # into the production table.
    # Update records if the record exists in the production table
    # Insert if the record doesn't exist in the production table
    #### YOUR CODE HERE

print(f"Completed Iceberg merge for {table_name}")
