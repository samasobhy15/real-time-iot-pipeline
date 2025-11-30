from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

KAFKA_SERVER = "localhost:29092"
TOPIC_NAME = "sensor_stream"
NUM_MESSAGES = 0
DELAY_SECONDS = 5

producer = KafkaProducer(
bootstrap_servers=KAFKA_SERVER,
value_serializer=lambda x: json.dumps(x).encode("utf-8"),
client_id="producer-win",
api_version_auto_timeout_ms=30000,
linger_ms=10,
retries=3
)

print(f"Producing to '{TOPIC_NAME}' every {DELAY_SECONDS}s via {KAFKA_SERVER}")
print("-" * 60)
count = 0

try:
while True:
message = {
"sensor_id": random.randint(1, 5),
"temperature": round(random.uniform(20.0, 35.0), 2),
"humidity": round(random.uniform(30.0, 60.0), 2),
"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
}
producer.send(TOPIC_NAME, value=message)
producer.flush()
count += 1
print(f"[{count}] Sent: {message}")
if NUM_MESSAGES and count >= NUM_MESSAGES:
break
time.sleep(DELAY_SECONDS)
except KeyboardInterrupt:
print("\nStopped by user")
finally:
producer.close()
print(f"\nProducer finished: {count} messages sent")