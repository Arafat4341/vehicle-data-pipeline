# from kafka import KafkaConsumer
# import json

# consumer = KafkaConsumer(
#     "vehicle_data",
#     bootstrap_servers="localhost:9092",
#     auto_offset_reset="earliest",
#     value_deserializer=lambda v: json.loads(v.decode("utf-8"))
# )

# for message in consumer:
#     data = message.value
#     print(f"Received Data: Vehicle {data['vehicle_id']} ({data['vehicle_type']}) | Speed: {data['speed']} km/h | Fuel: {data['fuel_level']}% | Status: {data['status']}")

from kafka import KafkaConsumer
import json

# Kafka Consumer setup
consumer = KafkaConsumer(
    "vehicle_data",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("Consumer is listening for messages...")

for message in consumer:
    data = message.value
    print("\n🚗 Received Vehicle Data 🚗")
    print(f"Vehicle ID     : {data.get('vehicle_id', 'N/A')}")
    print(f"Type           : {data.get('vehicle_type', 'N/A')}")
    print(f"Speed          : {data.get('speed', 'N/A')} km/h")
    print(f"Fuel Level     : {data.get('fuel_level', 'N/A')}%")
    print(f"Status         : {data.get('status', 'N/A')}")
    print(f"Latitude       : {data.get('latitude', 'N/A')}")
    print(f"Longitude      : {data.get('longitude', 'N/A')}")
    print(f"Timestamp      : {data.get('timestamp', 'N/A')}")
    print("-" * 50)
