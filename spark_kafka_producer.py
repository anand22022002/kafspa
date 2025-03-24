from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaProducer") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.local.ip", "127.0.0.1") \
    .config("spark.driver.port", "4042") \
    .config("spark.executor.heartbeatInterval", "20s") \
    .config("spark.network.timeout", "60s") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
    .getOrCreate()

# Read the CSV file (replace 'wind_power_data.csv' with your actual file path)
df = spark.read.option("header", "true").csv("dataset.csv")

# Convert data to JSON format
json_df = df.select(to_json(struct([df[col] for col in df.columns])).alias("value"))

# Write data to the Kafka topic
json_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "wind-data") \
    .save()

print("Data has been successfully sent to the Kafka topic!")
