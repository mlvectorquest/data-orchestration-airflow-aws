import sys
import json
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

args = getResolvedOptions(sys.argv, ["config"])

config = json.loads(args["config"])

table_name = config['table']
sql_s3_path = config['sql']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

bucket, key = sql_s3_path.replace("s3://", "").split("/", 1)
s3 = boto3.client("s3")
sql_query = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")

result_df = spark.sql(sql_query)

target_table = f"iceberg.analytics.{table_name}"

result_df.writeTo(target_table).using("iceberg").createOrReplace()

print(f"Completed Iceberg overwrite for {table_name}")