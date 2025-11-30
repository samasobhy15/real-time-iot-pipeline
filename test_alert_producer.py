from kafka import KafkaProducer
import json
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# بعث رسالة فيها مشكلة (HIGH TEMP)
alert_message = {
    "sensor_id": 999,
    "temperature": 45.5,  # أعلى من 40!
    "humidity": 55.0,
    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
}

producer.send("sensor_stream", value=alert_message)
producer.flush()
print(f"🔥 Sent ALERT test message: {alert_message}")

# بعث رسالة عادية
normal_message = {
    "sensor_id": 888,
    "temperature": 25.0,
    "humidity": 50.0,
    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
}

producer.send("sensor_stream", value=normal_message)
producer.flush()
print(f"✅ Sent NORMAL test message: {normal_message}")

producer.close()