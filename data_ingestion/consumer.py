from kafka import KafkaConsumer
import json

# Kafka Config
PROCESSED_TOPIC = "processed_vehicle_data"

# Kafka Consumer setup
consumer = KafkaConsumer(
    PROCESSED_TOPIC,
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("Consumer is listening for messages...")

for message in consumer:
    data = message.value
    print("\n Received Vehicle Data ")
    print(f"Vehicle ID     : {data.get('vehicle_id', 'N/A')}")
    print(f"Type           : {data.get('vehicle_type', 'N/A')}")
    print(f"Speed          : {data.get('speed', 'N/A')} km/h")
    print(f"Fuel Level     : {data.get('fuel_level', 'N/A')}%")
    print(f"Status         : {data.get('status', 'N/A')}")
    print(f"Latitude       : {data.get('latitude', 'N/A')}")
    print(f"Longitude      : {data.get('longitude', 'N/A')}")
    print(f"Timestamp      : {data.get('timestamp', 'N/A')}")
    
    # New Processed Fields
    print(f"Fuel Efficiency: {data.get('fuel_efficiency', 'N/A')} km/L")
    print(f"Trip Distance  : {data.get('trip_distance', 'N/A')} km")
    print(f"Engine Load    : {data.get('engine_load', 'N/A')}%")
    print(f"Battery Level  : {data.get('battery_level', 'N/A')}%")  # If added in transformation

    print("-" * 50)
