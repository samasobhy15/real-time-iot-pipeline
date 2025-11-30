# ========================================
# Stream Alerts Final - With Columns
# ========================================
import os
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, current_timestamp, to_timestamp, concat_ws
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# ========== الإعدادات ==========
KAFKA_SERVERS = "localhost:29092"
TOPIC = "sensor_stream"
OUTPUT_CSV = "alerts_output"
CHECKPOINT = "checkpoint_alerts"

print("="*70)
print(" 🚨 STREAMING ALERTS PIPELINE WITH COLUMNS ")
print("="*70 + "\n")

# ========== إنشاء Spark Session ==========
spark = SparkSession.builder \
    .appName("StreamingAlertsWithColumns") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Session Created\n")

# ========== Schema التعريف ==========
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("sensor_id", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True)
])

# ========== قراءة من Kafka ==========
print(f"📥 Reading from Kafka: {KAFKA_SERVERS} | Topic: {TOPIC}")
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# ========== Parse JSON ==========
parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

print("✅ Data Stream Parsed\n")

# ========== إنشاء Columns للـ Alerts ==========
alerts_df = parsed_stream \
    .withColumn("temp_high", when(col("temperature") > 40, 1).otherwise(0)) \
    .withColumn("temp_low", when(col("temperature") < 0, 1).otherwise(0)) \
    .withColumn("hum_high", when(col("humidity") > 90, 1).otherwise(0)) \
    .withColumn("hum_low", when(col("humidity") < 10, 1).otherwise(0))

# ========== تحديد نوع التنبيه (Alert Type Column) ==========
alerts_df = alerts_df.withColumn(
    "alert_type",
    when(col("temp_high") == 1, "HIGH_TEMP")
    .when(col("temp_low") == 1, "LOW_TEMP")
    .when(col("hum_high") == 1, "HIGH_HUMIDITY")
    .when(col("hum_low") == 1, "LOW_HUMIDITY")
    .otherwise("NORMAL")
)

# ========== Anomaly Flag Column ==========
alerts_df = alerts_df.withColumn(
    "anomaly_flag",
    when(
        (col("temp_high") + col("temp_low") + col("hum_high") + col("hum_low")) > 0,
        "ALERT"
    ).otherwise("OK")
)

# ========== رسالة التنبيه (Alert Message Column) ==========
alerts_df = alerts_df.withColumn(
    "alert_message",
    when(col("anomaly_flag") == "ALERT",
         concat_ws(" | ",
                   when(col("temp_high") == 1, "⚠️ Temperature > 40°C").otherwise(""),
                   when(col("temp_low") == 1, "⚠️ Temperature < 0°C").otherwise(""),
                   when(col("hum_high") == 1, "⚠️ Humidity > 90%").otherwise(""),
                   when(col("hum_low") == 1, "⚠️ Humidity < 10%").otherwise("")
         )
    ).otherwise("✅ Normal")
)

# ========== إضافة Timestamp ==========
alerts_df = alerts_df.withColumn(
    "event_time", 
    to_timestamp(col("timestamp"))
).withColumn(
    "ingestion_time", 
    current_timestamp()
)

# ========== اختيار الأعمدة النهائية ==========
final_alerts = alerts_df.select(
    "event_time",
    "sensor_id",
    "temperature",
    "humidity",
    "temp_high",
    "temp_low",
    "hum_high",
    "hum_low",
    "anomaly_flag",
    "alert_type",
    "alert_message",
    "ingestion_time"
)

print("✅ Alert Columns Created:")
print("   - temp_high, temp_low, hum_high, hum_low (Binary flags)")
print("   - anomaly_flag (ALERT/OK)")
print("   - alert_type (HIGH_TEMP/LOW_TEMP/HIGH_HUMIDITY/LOW_HUMIDITY/NORMAL)")
print("   - alert_message (Descriptive message)")
print("\n" + "="*70)

# ========== كتابة النتائج: Console ==========
console_query = final_alerts.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

print("🖥️  Console Output: Started")

# ========== كتابة النتائج: CSV ==========
csv_query = final_alerts.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", OUTPUT_CSV) \
    .option("checkpointLocation", CHECKPOINT) \
    .option("header", "true") \
    .start()

print(f"💾 CSV Output: {OUTPUT_CSV}")
print("\n⏳ Streaming Active... Press Ctrl+C to stop\n")
print("="*70)

# ========== انتظار التشغيل ==========
try:
    console_query.awaitTermination()
except KeyboardInterrupt:
    print("\n⛔ Stopping streams...")
    console_query.stop()
    csv_query.stop()
    spark.stop()
    print("✅ Streams stopped successfully")