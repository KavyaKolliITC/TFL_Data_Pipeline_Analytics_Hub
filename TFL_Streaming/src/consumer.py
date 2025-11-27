from pyspark.sql import SparkSession 
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite

# hdfs file system for archiving
from py4j.java_gateway import java_import

# create spark session
spark = (
    SparkSession.builder
        .appName("UK_TFL_STREAMING_CONSUMER_DEDUPED")
        .config("spark.jars.packages", "com.amazon.deequ:deequ:1.2.2-spark-3.3")
        .getOrCreate()
)

# kafka config
kafka_servers = "ip-172-31-3-80.eu-west-2.compute.internal:9092"
topic = "ukde011025tfldata"

# hdfs paths
incoming_path = "hdfs:////tmp/DE011025/uk/streaming/incoming/"
archive_path = "/tmp/DE011025/uk/streaming/archive"
checkpoint_path = "hdfs:////tmp/DE011025/uk/streaming/incoming_checkpoints"

# tfl arrivals api schema
tfl_schema = ArrayType(StructType([
    StructField("id", StringType()),
    StructField("operationType", IntegerType()),
    StructField("vehicleId", StringType()),
    StructField("naptanId", StringType()),
    StructField("stationName", StringType()),
    StructField("lineId", StringType()),
    StructField("lineName", StringType()),
    StructField("platformName", StringType()),
    StructField("direction", StringType()),
    StructField("bearing", StringType()),
    StructField("destinationNaptanId", StringType()),
    StructField("destinationName", StringType()),
    StructField("timestamp", StringType()),
    StructField("timeToStation", IntegerType()),
    StructField("currentLocation", StringType()),
    StructField("towards", StringType()),
    StructField("expectedArrival", StringType()),
    StructField("timeToLive", StringType()),
    StructField("modeName", StringType())
]))

# move old parquet files from incoming to archive
hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
hadoop_path_incoming = spark._jvm.org.apache.hadoop.fs.Path(incoming_path)
hadoop_path_archive = spark._jvm.org.apache.hadoop.fs.Path(archive_path)

if hadoop_fs.exists(hadoop_path_incoming):
    files = hadoop_fs.listStatus(hadoop_path_incoming)
    for f in files:
        src = f.getPath()
        dst = spark._jvm.org.apache.hadoop.fs.Path(archive_path + "/" + src.getName())
        hadoop_fs.rename(src, dst)
    print("archived old incoming parquet files")

# read stream from kafka
kafka_df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
)

raw_json_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_value")

# parse json using from_json
parsed = raw_json_df.withColumn(
    "data", from_json(col("json_value"), tfl_schema)
)

# validate schema using pydeequ
def run_schema_validation(batch_df, batch_id):

    print(f"running deequ validation on batch {batch_id}")
    
    valid_rows_df = batch_df.filter(col("data").isNotNull())

    if valid_rows_df.count() == 0:
        raise Exception("json parsing failed. incoming data does not match schema.")

    exploded = valid_rows_df.select(explode(col("data")).alias("event"))
    df = exploded.select("event.*")

    check = (Check(spark, CheckLevel.Error, "schemacheck")
        .isComplete("id")
        .isComplete("naptanId")
        .isComplete("stationName")
        .isComplete("lineId")
        .isComplete("expectedArrival")
        .isInt("operationType")
        .isInt("timeToStation")
        .isContainedIn("modeName", ["tube"])
    )

    results = VerificationSuite(spark).onData(df).addCheck(check).run()
    status = results.checkResults["schemacheck"].status

    print("deequ results:", status)

    if status != "Success":
        raise Exception(f"schema validation failed in batch {batch_id} — data does not match schema.")

    print("batch schema validated successfully")

# write validated json as parquet to incoming
def write_to_incoming(batch_df, batch_id):

    exploded = batch_df.select(explode(col("data")).alias("event"))
    df = exploded.select("event.*")

    df.write.mode("overwrite").parquet(incoming_path)
    print(f"wrote validated batch {batch_id} to incoming parquet")

# apply validation and write for each micro-batch
query = (
    parsed.writeStream
        .foreachBatch(lambda batch_df, batch_id: (
            run_schema_validation(batch_df, batch_id),
            write_to_incoming(batch_df, batch_id)
        ))
        .option("checkpointLocation", checkpoint_path)
        .start()
)

query.awaitTermination()
