from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, IntegerType, DoubleType, StringType

# إعدادات
KAFKA_SERVER = "kafka:9092"

TOPIC_NAME = 'iot-sensors'
OUTPUT_CSV = 'sensor_stream_from_kafka.csv'
CHECKPOINT_DIR = './spark_checkpoint'

# إنشاء Spark Session
spark = SparkSession.builder \
    .appName("Kafka_to_CSV") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# تعريف السكيما
schema = StructType() \
    .add("sensor_id", IntegerType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("timestamp", StringType())

print(f"📥 Reading from Kafka topic: {TOPIC_NAME}")

# قراءة من Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .load()

# تحويل البيانات
parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# كتابة في CSV
query = parsed.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("header", "true") \
    .option("path", OUTPUT_CSV) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .start()

print(f"💾 Writing to CSV: {OUTPUT_CSV}")
print("⏳ Streaming... Press Ctrl+C to stop")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n⛔ Stopping stream...")
    query.stop()
    spark.stop()
    print("✅ Stream stopped")
