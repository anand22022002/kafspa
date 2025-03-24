
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date,col, from_json, current_date, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, MapType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.local.ip", "127.0.0.1") \
    .config("spark.driver.port", "4043") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,io.delta:delta-core_2.12:2.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Define the schema for Kafka messages
schema = StructType([
    StructField("Date/Time", StringType()),
    StructField("LV ActivePower (kW)", StringType()),
    StructField("Wind Speed (m/s)", StringType()),
    StructField("Theoretical_Power_Curve (KWh)", StringType()),
    StructField("Wind Direction (°)", StringType())
])

# Read data from Kafka in streaming fashion
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "wind-data") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize Kafka data and apply schema
parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data"))

# query = parsed_df.select("data.`Date/Time`").writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start()

# query.awaitTermination()



transformed_df = parsed_df.select(
    to_date(col("data.`Date/Time`"), "dd MM yyyy").alias("signal_date"),  # Extract date
    to_timestamp(col("data.`Date/Time`"), "dd MM yyyy HH:mm").alias("signal_ts"),  # Extract timestamp
    current_date().alias("create_date"),
    current_timestamp().alias("create_ts"),
    col("data.`LV ActivePower (kW)`").alias("ActivePower_kW"),
    col("data.`Wind Speed (m/s)`").alias("WindSpeed_m_s"),
    col("data.`Theoretical_Power_Curve (KWh)`").alias("PowerCurve_kWh"),
    col("data.`Wind Direction (°)`").alias("WindDirection_deg")
)
# query = transformed_df.select("signal_date", "signal_ts", "create_date", "create_ts").writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start()

# query.awaitTermination()


# Write the transformed data to Delta table in streaming fashion
delta_stream = transformed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/home/xs-514ansana/checkpoints/delta_kafka_consumer") \
    .start("/home/xs-514ansana/delta/tables/wind_data")

delta_stream.awaitTermination()
