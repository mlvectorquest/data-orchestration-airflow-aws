import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ── Read job arguments ────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", 
    "table", 
    "landing_path",
    "data_interval",
    "primary_keys",
])

# Key columns to deduplicate on, passed by the DAG as a comma-separated string
primary_keys = args["primary_keys"].split(",")

# ── Configure spark ───────────────────────────────────────────────────────────
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args) 

# ── Set up locations ────────────────────────────────────────────────────────────

table = args["table"]
landing_path = args["landing_path"]

ICEBERG_TABLE = f"iceberg.raw.{table}"

# ── Read landing files ──────────────────────────────────────────────────────────

data_interval = args["data_interval"]

# Spark infers schema from JSON natively — no header/inferSchema options needed.
# Default reader expects newline-delimited JSON (one object per line); for files
# containing a single object or pretty-printed JSON, add .option("multiLine", "true").
df = spark.read.json(landing_path)

if df.rdd.isEmpty():
    print(f"No files at {landing_path} — skipping.")
    job.commit()
    sys.exit(0)

# Add data_interval column to the dataframe
df = df.withColumn("data_interval", F.lit(data_interval))

# Deduplicate on the table's primary key
df = df.dropDuplicates(primary_keys)

# ── Write to Iceberg ────────────────────────────────────────────────────────────

# Check if the table exists
if not spark.catalog.tableExists(ICEBERG_TABLE):
    (
        df
        .writeTo(ICEBERG_TABLE)
        .partitionedBy("data_interval")
        .using("iceberg")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .create()
    )
    print(f"[{table}] Created Iceberg table: {ICEBERG_TABLE}")
else:
    df.writeTo(ICEBERG_TABLE).overwritePartitions()
    print(f"[{table}] Overwrote partition data_interval={data_interval}")

count = spark.sql(f"""
    SELECT COUNT(*) FROM {ICEBERG_TABLE}
    WHERE data_interval = '{data_interval}'
""").collect()[0][0]
print(f"[{table}] {count:,} rows in partition data_interval={data_interval}")
