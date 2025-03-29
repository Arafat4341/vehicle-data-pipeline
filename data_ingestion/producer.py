from kafka import KafkaProducer
import json
import time
import random

# kafka config
RAW_TOPIC = "raw_vehicle_data"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Vehicle details
vehicle_types = ["sedan", "truck", "motorcycle", "SUV"]
vehicle_ids = {vt: [f"{vt.upper()}_{i}" for i in range(1, 6)] for vt in vehicle_types}
vehicle_positions = {v: [random.uniform(-90, 90), random.uniform(-180, 180)] for v in sum(vehicle_ids.values(), [])}

# Function to simulate vehicle data
def generate_vehicle_data():
    vehicle_type = random.choice(vehicle_types)
    vehicle_id = random.choice(vehicle_ids[vehicle_type])
    
    # Simulate GPS movement
    lat, lon = vehicle_positions[vehicle_id]
    lat += round(random.uniform(-0.01, 0.01), 6)  # Small movement
    lon += round(random.uniform(-0.01, 0.01), 6)
    vehicle_positions[vehicle_id] = [lat, lon]
    
    return {
        "vehicle_id": vehicle_id,
        "vehicle_type": vehicle_type,
        "speed": round(random.uniform(0, 180), 2),
        "fuel_level": round(random.uniform(5, 100), 2),
        "latitude": lat,
        "longitude": lon,
        "engine_temperature": round(random.uniform(70, 120), 2),
        "tire_pressure": round(random.uniform(30, 40), 2),
        "acceleration": round(random.uniform(-3, 5), 2),
        "status": random.choice(["moving", "idle", "parked"]),
        "timestamp": time.time()
    }

# Continuous data streaming
while True:
    data = generate_vehicle_data()
    producer.send(RAW_TOPIC, value=data)
    print(f"Sent: {data}")
    time.sleep(2)
