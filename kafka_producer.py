from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# إعدادات
KAFKA_SERVER = "localhost:29092" 
TOPIC_NAME   = "sensor_stream"
NUM_MESSAGES = 0  # 0 = infinite
DELAY_SECONDS = 5

# إنشاء Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    client_id="producer-win",
    api_version_auto_timeout_ms=30000,
    linger_ms=10,
    retries=3
)

print(f"🚀 Producing messages to topic '{TOPIC_NAME}' every {DELAY_SECONDS}s via {KAFKA_SERVER}")
print("🎲 VARIED MODE: Mix of Normal and different Alert types")
print("-" * 60)

count = 0

def generate_varied_data():
    """
    توليد بيانات متنوعة:
    - 60% Normal
    - 20% HIGH_TEMP
    - 10% HIGH_HUMIDITY
    - 5% LOW_TEMP
    - 5% LOW_HUMIDITY
    """
    sensor_id = random.randint(1, 5)
    
    # اختيار نوع البيانات بناءً على احتمالات
    rand = random.random()
    
    if rand < 0.60:  # 60% - Normal
        temperature = round(random.uniform(20.0, 39.0), 2)
        humidity = round(random.uniform(30.0, 80.0), 2)
        status = "🟢 NORMAL"
    
    elif rand < 0.80:  # 20% - HIGH TEMP
        temperature = round(random.uniform(41.0, 50.0), 2)
        humidity = round(random.uniform(30.0, 80.0), 2)
        status = "🔥 HIGH_TEMP"
    
    elif rand < 0.90:  # 10% - HIGH HUMIDITY
        temperature = round(random.uniform(20.0, 35.0), 2)
        humidity = round(random.uniform(91.0, 98.0), 2)
        status = "💧 HIGH_HUMIDITY"
    
    elif rand < 0.95:  # 5% - LOW TEMP
        temperature = round(random.uniform(-5.0, -0.5), 2)
        humidity = round(random.uniform(30.0, 80.0), 2)
        status = "❄️ LOW_TEMP"
    
    else:  # 5% - LOW HUMIDITY
        temperature = round(random.uniform(20.0, 35.0), 2)
        humidity = round(random.uniform(5.0, 9.5), 2)
        status = "🏜️ LOW_HUMIDITY"
    
    return {
        "sensor_id": sensor_id,
        "temperature": temperature,
        "humidity": humidity,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }, status

try:
    while True:
        # توليد بيانات متنوعة
        message, status = generate_varied_data()
        
        # إرسال الرسالة
        producer.send(TOPIC_NAME, value=message)
        producer.flush()
        
        count += 1
        print(f"{status} [{count}] Sent: Sensor {message['sensor_id']} | "
              f"Temp: {message['temperature']}°C | Humidity: {message['humidity']}%")
        
        if NUM_MESSAGES and count >= NUM_MESSAGES:
            break
        
        time.sleep(DELAY_SECONDS)

except KeyboardInterrupt:
    print("\n⛔ Stopped by user")
finally:
    producer.close()
    print(f"\n🎯 Producer finished: {count} messages sent")