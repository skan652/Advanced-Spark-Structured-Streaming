from pyspark.sql.types import StructType, StructField, StringType, DoubleType

event_schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("event_time", StringType(), True)
])