from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "vehicle_data",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

for message in consumer:
    data = message.value
    print(f"Received Data: Vehicle {data['vehicle_id']} ({data['vehicle_type']}) | Speed: {data['speed']} km/h | Fuel: {data['fuel_level']}% | Status: {data['status']}")
