from kafka import KafkaProducer
import json
import time
import random

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Vehicle details
vehicle_types = ["sedan", "truck", "motorcycle", "SUV"]
vehicle_ids = {vt: [f"{vt.upper()}_{i}" for i in range(1, 6)] for vt in vehicle_types}

# Function to simulate vehicle data
def generate_vehicle_data():
    vehicle_type = random.choice(vehicle_types)
    vehicle_id = random.choice(vehicle_ids[vehicle_type])
    
    # Simulated values
    return {
        "vehicle_id": vehicle_id,
        "vehicle_type": vehicle_type,
        "speed": round(random.uniform(0, 180), 2),  # Speed in km/h
        "fuel_level": round(random.uniform(5, 100), 2),  # Fuel %
        "latitude": round(random.uniform(-90, 90), 6),
        "longitude": round(random.uniform(-180, 180), 6),
        "engine_temperature": round(random.uniform(70, 120), 2),  # °C
        "tire_pressure": round(random.uniform(30, 40), 2),  # PSI
        "acceleration": round(random.uniform(-3, 5), 2),  # m/s²
        "status": random.choice(["moving", "idle", "parked"]),
        "timestamp": time.time()
    }

# Continuous data streaming
while True:
    data = generate_vehicle_data()
    producer.send("vehicle_data", value=data)
    print(f"Sent: {data}")
    time.sleep(2)
