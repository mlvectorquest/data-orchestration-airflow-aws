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

# Initialize a spark context
#### YOUR CODE HERE

# Wrap the spark context in a GlueContext
#### YOUR CODE HERE

# Store the glue context's spark_session to the variable `spark`
#### YOUR CODE HERE

# Using boto3, read the file stored at the bucket an key defined above
# Store the file's string content as the variable `sql_query`
#### YOUR CODE HERE

# Using spark, execute the SQL query
# Store the output to a variable
#### YOUR CODE HERE

# Create a string that points to the production table
# Use the catalog configured by the GlueJobOperator
#### YOUR CODE HERE

# Write the SQL query's result to the production table
# Replace the table in full
#### YOUR CODE HERE

print(f"Completed Iceberg overwrite for {table_name}")