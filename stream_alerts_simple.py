"""
Stream Alerts - Simple Version
بدون تحميل packages - يستخدم Kafka-Python مباشرة
"""

from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import time

# الإعدادات
KAFKA_SERVERS = "localhost:29092"
TOPIC = "sensor_stream"
OUTPUT_CSV = "alerts_output_simple.csv"

print("="*70)
print(" 🚨 STREAMING ALERTS - SIMPLE VERSION ")
print("="*70)
print(f"\n📥 Connecting to Kafka: {KAFKA_SERVERS}")
print(f"📡 Subscribing to topic: {TOPIC}\n")

# إنشاء Consumer
try:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',  # قراءة من البداية
        enable_auto_commit=True,
        group_id='alerts-consumer-group',  # مجموعة جديدة
        consumer_timeout_ms=60000  # انتظار دقيقة
    )
    print("✅ Connected to Kafka successfully!\n")
except Exception as e:
    print(f"❌ Failed to connect to Kafka: {e}")
    exit(1)

# قائمة لحفظ النتائج
alerts_data = []

print("="*70)
print("⏳ Listening for messages... Press Ctrl+C to stop")
print("="*70)
print()

message_count = 0

try:
    for message in consumer:
        data = message.value
        message_count += 1
        
        # استخراج البيانات
        timestamp = data.get('timestamp', '')
        sensor_id = data.get('sensor_id', 0)
        temperature = data.get('temperature', 0.0)
        humidity = data.get('humidity', 0.0)
        
        # تحليل الـ Alerts (Columns)
        temp_high = 1 if temperature > 40 else 0
        temp_low = 1 if temperature < 0 else 0
        hum_high = 1 if humidity > 90 else 0
        hum_low = 1 if humidity < 10 else 0
        
        # تحديد نوع التنبيه
        if temp_high:
            alert_type = "HIGH_TEMP"
            alert_message = "⚠️ Temperature > 40°C"
        elif temp_low:
            alert_type = "LOW_TEMP"
            alert_message = "⚠️ Temperature < 0°C"
        elif hum_high:
            alert_type = "HIGH_HUMIDITY"
            alert_message = "⚠️ Humidity > 90%"
        elif hum_low:
            alert_type = "LOW_HUMIDITY"
            alert_message = "⚠️ Humidity < 10%"
        else:
            alert_type = "NORMAL"
            alert_message = "✅ Normal"
        
        # Anomaly Flag
        anomaly_flag = "ALERT" if (temp_high + temp_low + hum_high + hum_low) > 0 else "OK"
        
        # وقت المعالجة
        ingestion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # حفظ النتيجة
        alert_record = {
            'event_time': timestamp,
            'sensor_id': sensor_id,
            'temperature': temperature,
            'humidity': humidity,
            'temp_high': temp_high,
            'temp_low': temp_low,
            'hum_high': hum_high,
            'hum_low': hum_low,
            'anomaly_flag': anomaly_flag,
            'alert_type': alert_type,
            'alert_message': alert_message,
            'ingestion_time': ingestion_time
        }
        
        alerts_data.append(alert_record)
        
        # طباعة في Console
        status_icon = "🔴" if anomaly_flag == "ALERT" else "🟢"
        print(f"{status_icon} [{message_count}] {timestamp} | Sensor {sensor_id} | "
              f"Temp: {temperature}°C | Humidity: {humidity}% | "
              f"Status: {anomaly_flag} | {alert_message}")
        
        # حفظ في CSV كل 10 رسائل
        if message_count % 10 == 0:
            df = pd.DataFrame(alerts_data)
            df.to_csv(OUTPUT_CSV, index=False)
            print(f"\n💾 Saved {message_count} records to {OUTPUT_CSV}\n")

except KeyboardInterrupt:
    print("\n\n⛔ Stopping stream...")
    
finally:
    # حفظ البيانات النهائية
    if alerts_data:
        df = pd.DataFrame(alerts_data)
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"\n✅ Final save: {len(alerts_data)} records saved to {OUTPUT_CSV}")
        
        # إحصائيات
        alerts_count = df[df['anomaly_flag'] == 'ALERT'].shape[0]
        normal_count = df[df['anomaly_flag'] == 'OK'].shape[0]
        
        print("\n" + "="*70)
        print(" 📊 STATISTICS ")
        print("="*70)
        print(f"Total messages processed: {message_count}")
        print(f"🔴 Alerts: {alerts_count}")
        print(f"🟢 Normal: {normal_count}")
        print("="*70)
    
    consumer.close()
    print("\n✅ Consumer closed successfully")