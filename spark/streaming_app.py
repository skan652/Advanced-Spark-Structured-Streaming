from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, count, window, to_timestamp, when, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
import sys

# Define schema matching your actual data
device_schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("temperature", StringType(), True),  # Can be string like "hot" or number
    StructField("humidity", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("event_time", StringType(), True)  # Your timestamp field
])

# Create Spark session with Kafka packages
spark = SparkSession.builder \
    .appName("AdvancedSparkStreaming") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("Starting Spark Structured Streaming application...")
print("Using Apache Spark 3.5.0")
print("=" * 60)

# Read from Kafka
try:
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "weather") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()  # Use 'weather' topic from your producer
    
    print("✅ Connected to Kafka at kafka:9092")
    print("✅ Subscribed to topic: weather")
except Exception as e:
    print(f"❌ Failed to connect: {e}")
    sys.exit(1)

# Parse JSON with error handling
json_df = df.select(
    from_json(col("value").cast("string"), device_schema).alias("data"),
    col("value").cast("string").alias("raw_json")
)

# Split into valid and invalid events
valid_events = json_df.filter(col("data").isNotNull())
invalid_events = json_df.filter(col("data").isNull()).select("raw_json")

# Extract valid data and clean it
parsed_events = valid_events.select("data.*")

# Convert event_time to timestamp, handling invalid values
parsed_events = parsed_events \
    .withColumn("event_timestamp", to_timestamp("event_time")) \
    .drop("event_time")

# Clean temperature data
parsed_events = parsed_events \
    .withColumn("temperature_clean", 
        when(col("temperature").cast("double").isNotNull(), col("temperature").cast("double"))
        .when(col("temperature") == "hot", lit(40.0))  # Treat "hot" as 40 degrees
        .otherwise(None)) \
    .drop("temperature")

# Filter out events with null timestamp (can't do windowing without timestamp)
valid_timestamp_events = parsed_events.filter(col("event_timestamp").isNotNull())

print("\n✅ Processed stream schema:")
valid_timestamp_events.printSchema()

# Apply watermark and window aggregations for temperature
windowed_temp_agg = valid_timestamp_events \
    .withWatermark("event_timestamp", "10 minutes") \
    .groupBy(
        window("event_timestamp", "5 minutes"),
        "device_id"
    ) \
    .agg(
        avg("temperature_clean").alias("avg_temperature"),
        count("*").alias("event_count")
    )

# Country-based aggregation
country_agg = valid_timestamp_events \
    .withWatermark("event_timestamp", "10 minutes") \
    .groupBy(
        window("event_timestamp", "10 minutes"),
        "country"
    ) \
    .agg(
        count("*").alias("events_per_country"),
        avg("temperature_clean").alias("avg_country_temp")
    )

# Humidity aggregation (if humidity data exists)
humidity_agg = valid_timestamp_events \
    .withWatermark("event_timestamp", "10 minutes") \
    .groupBy(
        window("event_timestamp", "10 minutes"),
        "device_id"
    ) \
    .agg(avg("humidity").alias("avg_humidity"))

# Write results to console (update mode)
query1 = windowed_temp_agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .queryName("temperature_by_device") \
    .start()

# Write to Parquet file (append mode - only new records)
query2 = windowed_temp_agg.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/opt/spark/data/parquet/temperature_by_device") \
    .option("checkpointLocation", "/tmp/checkpoints/temp_device_parquet") \
    .trigger(processingTime="10 seconds") \
    .queryName("temperature_by_device_parquet") \
    .start()

query3 = country_agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .queryName("country_aggregations") \
    .start()

# Write country aggregations to Parquet (append mode)
query4 = country_agg.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/opt/spark/data/parquet/country_aggregations") \
    .option("checkpointLocation", "/tmp/checkpoints/country_agg_parquet") \
    .trigger(processingTime="10 seconds") \
    .queryName("country_aggregations_parquet") \
    .start()

query5 = humidity_agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .queryName("humidity_by_device") \
    .start()

# Write invalid events to Kafka topic
query6 = invalid_events.select(col("raw_json").alias("value")).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "weather-errors") \
    .option("checkpointLocation", "/tmp/checkpoints/invalid_events_kafka") \
    .trigger(processingTime="10 seconds") \
    .queryName("invalid_events_kafka") \
    .start()

# Also write invalid events to console for debugging
query7 = invalid_events.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .queryName("invalid_events_console") \
    .start()

print("\n" + "=" * 60)
print("✅ Streaming queries started. Waiting for data...")
print("📊 Spark UI available at: http://localhost:4040")
print("📝 Press Ctrl+C to stop")
print("=" * 60 + "\n")

# Wait for termination
spark.streams.awaitAnyTermination()
