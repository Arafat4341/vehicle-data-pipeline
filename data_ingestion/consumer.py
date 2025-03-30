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
    alerts = []  # Store alerts

    # Alert conditions
    if data.get("high_speed_alert"):
        alerts.append("⚠️ High Speed Alert: Vehicle exceeding safe limits!")

    if data.get("fuel_level", 100) < 10:
        alerts.append(f"⛽ Low Fuel Alert: {data['fuel_level']}% remaining")

    if data.get("engine_health", 100) < 50:
        alerts.append(f"⚙️ Engine Health Warning: {data['engine_health']}/100")
    
    if data.get("engine_temperature", 90) > 100:
        alerts.append(f"🔥 Engine Overheat Alert: {data['engine_temperature']}°C")

    # Print received data
    print("\nReceived Vehicle Data ")
    print(f"Vehicle ID     : {data.get('vehicle_id', 'N/A')}")
    print(f"Type           : {data.get('vehicle_type', 'N/A')}")
    print(f"Speed          : {data.get('speed', 'N/A')} km/h")
    print(f"Fuel Level     : {data.get('fuel_level', 'N/A')}%")
    print(f"Engine temp.   : {data.get('engine_temperature', 'N/A')}°C")
    print(f"Status         : {data.get('status', 'N/A')}")
    print(f"Latitude       : {data.get('latitude', 'N/A')}")
    print(f"Longitude      : {data.get('longitude', 'N/A')}")
    print(f"Timestamp      : {data.get('timestamp', 'N/A')}")

    # Processed Fields
    print(f"Fuel Efficiency: {data.get('fuel_efficiency', 'N/A')} km/L")
    print(f"High-Speed Alert: {'Yes' if data.get('high_speed_alert') else 'No'}")
    print(f"Engine Health  : {data.get('engine_health', 'N/A')}/100")
    print(f"Idle           : {'Yes' if data.get('idle') else 'No'}")

    # Print alerts if any
    if alerts:
        print("\n ALERTS ")
        for alert in alerts:
            print(alert)

    print("-" * 50)
