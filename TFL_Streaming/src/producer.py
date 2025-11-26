from pyspark.sql import SparkSession
import requests
from kafka import KafkaProducer
import json
import os

# ------------------------------------
# ENV Setup
# ------------------------------------
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-21-openjdk-21.0.7.0.6-2.el8.x86_64/bin/java"
os.environ["PATH"] = os.path.join(os.environ["JAVA_HOME"], "bin") + os.pathsep + os.environ["PATH"]

# ------------------------------------
# Spark Init
# ------------------------------------
spark = SparkSession.builder.appName("UK_TFL_DE011025_PRODUCER").getOrCreate()

# ------------------------------------
# TFL API ENDPOINTS
# ------------------------------------
api_list = {
    "piccadilly":   "https://api.tfl.gov.uk/Line/Piccadilly/Arrivals",
    "northern":     "https://api.tfl.gov.uk/Line/Northern/Arrivals",
    "central":      "https://api.tfl.gov.uk/Line/Central/Arrivals",
    "bakerloo":     "https://api.tfl.gov.uk/Line/Bakerloo/Arrivals",
    "metropolitan": "https://api.tfl.gov.uk/Line/Metropolitan/Arrivals",
    "victoria":     "https://api.tfl.gov.uk/Line/Victoria/Arrivals"
}

# Add your app_id & key
app_id  = "92293faa428041caad3dd647d39753a0"
app_key = "ba72936a3db54b4ba5792dc8f7acc043"

# ------------------------------------
# Kafka Producer Init
# ------------------------------------
producer = KafkaProducer(
    bootstrap_servers=['ip-172-31-3-80.eu-west-2.compute.internal:9092'],
    key_serializer=str.encode,
    value_serializer=str.encode
)

topic = "ukde011025tfldata"

# ------------------------------------
# PROCESS ALL APIS
# ------------------------------------
for line_name, api in api_list.items():
    url = f"{api}?app_id={app_id}&app_key={app_key}"

    # --- Fetch ---
    response = requests.get(url)
    response.raise_for_status()
    body = response.text

    # --- Convert to DF ---
    df = spark.read.json(spark.sparkContext.parallelize([body]))

    # --- Convert DF rows → JSON strings ---
    json_rows = df.toJSON().collect()
    json_payload = "\n".join(json_rows)

    # --- Send to Kafka using LINE NAME as KEY ---
    producer.send(
        topic,
        key=line_name,  # <---- Victoria, Central, Northern, etc
        value=json_payload
    )

    print(f"Sent {line_name} dataset → Kafka")

# Finish
producer.flush()
producer.close()
print("All datasets sent to Kafka successfully.")
