from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, hour, avg, when, col

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DeltaAnalysis") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


df = spark.read.format("delta").load("/home/xs-514ansana/delta/tables/wind_data")

from pyspark.sql.functions import countDistinct

# Count distinct timestamps per day
distinct_count = df.groupBy("signal_date").agg(countDistinct("signal_ts").alias("distinct_signal_ts_count"))
distinct_count.show(truncate=False)


averages_per_hour = df.groupBy("signal_date", hour("signal_ts").alias("hour")) \
    .agg(
        avg("ActivePower_kW").alias("avg_active_power"),
        avg("WindSpeed_m_s").alias("avg_wind_speed"),
        avg("PowerCurve_kWh").alias("avg_power_curve"),
        avg("WindDirection_deg").alias("avg_wind_direction")
    )
averages_per_hour.show(truncate=False)


data_with_indicator = df.withColumn("generation_indicator", when(col("ActivePower_kW") < 200, "Low")
    .when((col("ActivePower_kW") >= 200) & (col("ActivePower_kW") < 600), "Medium")
    .when((col("ActivePower_kW") >= 600) & (col("ActivePower_kW") < 1000), "High")
    .otherwise("Exceptional"))
data_with_indicator.show(truncate=False)

from pyspark.sql.functions import broadcast

mapping = spark.createDataFrame([
    {"sig_name": "LV ActivePower (kW)", "sig_mapping_name": "active_power_average"},
    {"sig_name": "Wind Speed (m/s)", "sig_mapping_name": "wind_speed_average"},
    {"sig_name": "Theoretical_Power_Curve (KWh)", "sig_mapping_name": "theo_power_curve_average"},
    {"sig_name": "Wind Direction (°)", "sig_mapping_name": "wind_direction_average"}
])
final_df = data_with_indicator.join(broadcast(mapping), data_with_indicator["sig_name"] == mapping["sig_name"], "left") \
    .drop("sig_name") \
    .withColumnRenamed("sig_mapping_name", "signal_name")
final_df.show(truncate=False)

final_df.write.format("delta").save("/home/xs-514ansana/delta/tables/analysis_results")
