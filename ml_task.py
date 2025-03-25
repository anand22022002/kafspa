from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("WindPowerMLTask") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
    .getOrCreate()

# Load the processed data
processed_data_path = "/home/xs-514ansana/processed/renamed_data"
renamed_df = spark.read.format("delta").load(processed_data_path)
print("\n=== Processed Data Loaded ===")
renamed_df.show(truncate=False)

# Encode `generation_status` column as numeric using StringIndexer
indexer = StringIndexer(inputCol="generation_status", outputCol="generation_status_index")
indexed_df = indexer.fit(renamed_df).transform(renamed_df)

# Combine features into a single vector column using VectorAssembler
feature_columns = ["wind_speed_average", "theo_power_curve_average", "wind_direction_average", "generation_status_index"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
final_df = assembler.transform(indexed_df)

# Split data into training and testing sets
train_df, test_df = final_df.randomSplit([0.8, 0.2], seed=42)

# Train a Linear Regression model to predict active power
lr = LinearRegression(featuresCol="features", labelCol="active_power_average",regParam=0.1)
lr_model = lr.fit(train_df)

# Evaluate the model
predictions = lr_model.transform(test_df)
evaluator = RegressionEvaluator(labelCol="active_power_average", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"\nRoot Mean Square Error (RMSE): {rmse}")

evaluator = RegressionEvaluator(labelCol="active_power_average", predictionCol="prediction", metricName="mae")
mae = evaluator.evaluate(predictions)
print(f"\nMean Absolute Error (MAE): {mae}")

# Predict for a single day's data
single_day_df = final_df.filter(col("signal_date") == "2018-01-01")
single_day_predictions = lr_model.transform(single_day_df)
print("\n=== Predictions for Single Day ===")
single_day_predictions.select("signal_date", "hour", "active_power_average", "prediction").show(truncate=False)


# pandas_df = predictions.select("active_power_average", "prediction").toPandas()

# import matplotlib.pyplot as plt
# plt.figure(figsize=(10, 6))
# plt.scatter(pandas_df["active_power_average"], pandas_df["prediction"], alpha=0.5)
# plt.xlabel("Actual Active Power")
# plt.ylabel("Predicted Active Power")
# plt.title("Actual vs Predicted Active Power")
# plt.plot([0, max(pandas_df["active_power_average"])], [0, max(pandas_df["active_power_average"])], color='red', linestyle='--')
# plt.show()

