# # Description: This script demonstrates how to perform basic data analysis on a Delta Table using PySpark.
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import countDistinct, hour, avg, col, when, broadcast
# from pyspark.sql.types import StructType, StructField, StringType

# # Initialize Spark Session
# spark = SparkSession.builder \
#     .appName("WindPowerAnalysis") \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#     .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
#     .getOrCreate()

# # Load the Delta Table
# delta_table_path = "/home/xs-514ansana/delta/tables/wind_data"
# delta_df = spark.read.format("delta").load(delta_table_path)

# # === Query 1: Distinct Count of signal_ts Per Day ===
# distinct_count_df = delta_df.groupBy("signal_date").agg(countDistinct("signal_ts").alias("distinct_signal_ts_count"))
# print("\n=== Query 1: Distinct Count of signal_ts Per Day ===")
# distinct_count_df.show(truncate=False)

# # === Query 2: Average Values for All Signals Per Hour ===
# averages_per_hour = delta_df.groupBy("signal_date", hour("signal_ts").alias("hour")).agg(
#     avg("ActivePower_kW"),
#     avg("WindSpeed_m_s"),
#     avg("PowerCurve_kWh"),
#     avg("WindDirection_deg")
# )
# print("\n=== Query 2: Average Values for All Signals Per Hour ===")
# averages_per_hour.show(truncate=False)

# # === Query 3: Adding Generation Indicator Based on Average ActivePower ===
# averaged_with_indicator = averages_per_hour.withColumn(
#     "generation_indicator",
#     when(col("avg(ActivePower_kW)") < 200, "Low")
#     .when((col("avg(ActivePower_kW)") >= 200) & (col("avg(ActivePower_kW)") < 600), "Medium")
#     .when((col("avg(ActivePower_kW)") >= 600) & (col("avg(ActivePower_kW)") < 1000), "High")
#     .otherwise("Exceptional")
# )
# print("\n=== Query 3: Adding Generation Indicator to Averages ===")
# averaged_with_indicator.show(truncate=False)

# # === Step 5: JSON Mapping for Signal Names ===
# # Define schema for mapping DataFrame
# mapping_schema = StructType([
#     StructField("sig_name", StringType(), True),
#     StructField("sig_mapping_name", StringType(), True)
# ])

# # Provide updated mapping data directly
# mapping_data = [
#     ("avg(ActivePower_kW)", "active_power_average"),
#     ("avg(WindSpeed_m_s)", "wind_speed_average"),
#     ("avg(PowerCurve_kWh)", "theo_power_curve_average"),
#     ("avg(WindDirection_deg)", "wind_direction_average")
# ]

# # Create the mapping DataFrame using the provided schema
# mapping_df = spark.createDataFrame(mapping_data, schema=mapping_schema)

# # Display the mapping DataFrame
# print("\n=== Mapping DataFrame Created ===")
# mapping_df.show(truncate=False)

# # === Step 6: Rename Signal Columns Based on Mapping ===
# renamed_df = averaged_with_indicator
# for row in mapping_df.collect():
#     sig_name = row["sig_name"]
#     sig_mapping_name = row["sig_mapping_name"]
    
#     # Rename the column directly if it exists in the DataFrame
#     if sig_name in renamed_df.columns:
#         renamed_df = renamed_df.withColumnRenamed(sig_name, sig_mapping_name)

# # Show the resulting DataFrame with renamed columns
# print("\n=== Final DataFrame with Renamed Signal Names ===")
# renamed_df.show(truncate=False)

# print("\nAll operations completed successfully!")


from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, hour, avg, col, when, broadcast
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("WindPowerAnalysis") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
    .getOrCreate()

# Load the Delta Table
delta_table_path = "/home/xs-514ansana/delta/tables/wind_data"
delta_df = spark.read.format("delta").load(delta_table_path)

# === Query 1: Distinct Count of signal_ts Per Day ===
distinct_count_df = delta_df.groupBy("signal_date").agg(countDistinct("signal_ts"))
print("\n=== Query 1: Distinct Count of signal_ts Per Day ===")
distinct_count_df.show(truncate=False)

# === Query 2: Average Values for All Signals Per Hour ===
# No aliases used here; original column names remain "avg(ActivePower_kW)", etc.
averages_per_hour = delta_df.groupBy("signal_date", hour("signal_ts").alias("hour")).agg(
    avg("ActivePower_kW"),
    avg("WindSpeed_m_s"),
    avg("PowerCurve_kWh"),
    avg("WindDirection_deg")
)
print("\n=== Query 2: Average Values for All Signals Per Hour ===")
averages_per_hour.show(truncate=False)

# === Query 3: Adding Generation Indicator Based on Average ActivePower ===
averaged_with_indicator = averages_per_hour.withColumn(
    "generation_indicator",
    when(col("avg(ActivePower_kW)") < 200, "Low")
    .when((col("avg(ActivePower_kW)") >= 200) & (col("avg(ActivePower_kW)") < 600), "Medium")
    .when((col("avg(ActivePower_kW)") >= 600) & (col("avg(ActivePower_kW)") < 1000), "High")
    .otherwise("Exceptional")
)
print("\n=== Query 3: Adding Generation Indicator to Averages ===")
averaged_with_indicator.show(truncate=False)

# === Step 5: JSON Mapping for Signal Names ===
mapping_schema = StructType([
    StructField("sig_name", StringType(), True),
    StructField("sig_mapping_name", StringType(), True)
])

mapping_data = [
    {"sig_name": "avg(ActivePower_kW)", "sig_mapping_name": "active_power_average"},
    {"sig_name": "avg(WindSpeed_m_s)", "sig_mapping_name": "wind_speed_average"},
    {"sig_name": "avg(PowerCurve_kWh)", "sig_mapping_name": "theo_power_curve_average"},
    {"sig_name": "avg(WindDirection_deg)", "sig_mapping_name": "wind_direction_average"},
    {"sig_name": "generation_indicator", "sig_mapping_name": "generation_status"}
]

mapping_df = spark.createDataFrame(mapping_data, schema=mapping_schema)
print("\n=== Mapping DataFrame Created ===")
mapping_df.show(truncate=False)

# === Step 6: Perform Broadcast Join for Renaming Columns ===
# Perform broadcast join and rename columns
renamed_df = averaged_with_indicator
for row in mapping_df.collect():
    sig_name = row["sig_name"]
    sig_mapping_name = row["sig_mapping_name"]
    if sig_name in renamed_df.columns:
        renamed_df = renamed_df.withColumnRenamed(sig_name, sig_mapping_name)

print("\n=== Final DataFrame with Renamed Signal Names ===")
renamed_df.show(truncate=False)

# Save the processed DataFrame for the ML task
processed_data_path = "/home/xs-514ansana/processed/renamed_data"
renamed_df.write.mode("overwrite").format("delta").save(processed_data_path)
print("\nProcessed data saved to:", processed_data_path)
