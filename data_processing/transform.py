from kafka import KafkaConsumer, KafkaProducer
import json

# Kafka Config
KAFKA_BROKER = "localhost:9092"
RAW_TOPIC = "raw_vehicle_data"
PROCESSED_TOPIC = "processed_vehicle_data"

# Initialize Consumer
consumer = KafkaConsumer(
    RAW_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest"
)

# Initialize Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

def transform_data(data):
    """
    Apply necessary transformations:
    - Remove invalid data
    - Convert values if needed
    - Add new calculated fields
    """
    if "speed" not in data or "fuel_level" not in data or "status" not in data:
        return None  # Skip invalid records

    # Ensure speed is in km/h (assume incoming speed is sometimes in mph)
    if data.get("unit") == "mph":
        data["speed"] = round(data["speed"] * 1.60934, 2)  # Convert mph → km/h

    # Add new calculated field: Fuel Efficiency (simplified)
    if data["speed"] > 0:
        data["fuel_efficiency"] = round(data["fuel_level"] / data["speed"], 2)
    else:
        data["fuel_efficiency"] = None

    return data

# Consume, transform, and produce new data
for message in consumer:
    raw_data = message.value
    transformed_data = transform_data(raw_data)

    if transformed_data:
        producer.send(PROCESSED_TOPIC, value=transformed_data)
        print(f"Processed & Sent: {transformed_data}")