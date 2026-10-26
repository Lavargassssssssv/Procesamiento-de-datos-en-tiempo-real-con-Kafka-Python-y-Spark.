import json
import random
import time
from kafka import KafkaProducer


def generate_sensor_data():
    return {
        "sensor_id": random.randint(1, 5),
        "temperature": round(random.uniform(18, 35), 2),
        "humidity": round(random.uniform(30, 70), 2),
        "timestamp": int(time.time())
    }


producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

print("Productor iniciado. Enviando datos a Kafka...")

try:
    while True:
        sensor_data = generate_sensor_data()
        producer.send("datos_sensores", value=sensor_data)
        producer.flush()
        print(f"Enviado: {sensor_data}")
        time.sleep(1)

except KeyboardInterrupt:
    print("\nProductor detenido.")

finally:
    producer.close()
