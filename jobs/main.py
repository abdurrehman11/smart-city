import os
from confluent_kafka import SerializingProducer
import simplejson as js
from datetime import datetime, timedelta
import random
import uuid
import json
import time


LONDON_COORDINATES = { "latitude": 51.5074, "longitude": -0.1278 }
BIRMINGHAM_COORDINATES = { "latitude": 52.4862, "longitude": -1.8904 }

# Calculate movement increments
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude']) / 100

# Environment variables for configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()

random.seed(42)

def simulate_vehicle_movement():
    global start_location

    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    start_location['latitude'] += random.uniform(-0.005, 0.005)
    start_location['longitude'] += random.uniform(-0.005, 0.005)

    return start_location

def get_next_timestamp():
    global start_time

    start_time += timedelta(seconds=random.randint(30 , 60))
    return start_time

def generate_vehicle_data(vehicle_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'vehicleId': vehicle_id,
        'timestamp': get_next_timestamp().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'make': 'BMW',
        'model': 'C500',
        'year': 2024,
        'fuelType': 'Hybrid'
    }

def generate_gps_data(vehicle_id, timestamp, vehicle_type='private'):
    return {
        'id': uuid.uuid4(),
        'vehicleId': vehicle_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),
        'direction': 'North-East',
        'vehicleType': vehicle_type
    }

def generate_traffic_camera_data(vehicle_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'vehicleId': vehicle_id,
        'cameraId': camera_id,
        'timestamp': timestamp,
        'location': location,
        'snapshot': 'Base64EncodedString'
    }

def generate_weather_data(vehicle_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'vehicleId': vehicle_id,
        'timestamp': timestamp,
        'location': location,
        'temperature': random.uniform(-5, 26),
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rain', 'Snow']),
        'precipitation': random.uniform(0, 25),
        'windSpeed': random.uniform(0, 100),
        'humidity': random.randint(0, 100),
        'airQualityIndex': random.uniform(0, 500)
    }

def generate_emergency_incident_data(vehicle_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'vehicleId': vehicle_id,
        'timestamp': timestamp,
        'location': location,
        'incidentId': uuid.uuid4(),
        'incidentType': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Description of the incident'
    }

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to Topic: {msg.topic()},  Partition: [{msg.partition()}]")

def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )

    producer.flush()

def simulate_journey(producer, vehicle_id):
    while True:
        vehicle_data = generate_vehicle_data(vehicle_id)
        gps_data = generate_gps_data(vehicle_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(
            vehicle_id, 
            timestamp=vehicle_data['timestamp'],
            location=vehicle_data['location'],
            camera_id='Nike-Cam123'
        )
        weather_data = generate_weather_data(
            vehicle_id, 
            timestamp=vehicle_data['timestamp'], 
            location=vehicle_data['location']
        )
        emergency_incident_data = generate_emergency_incident_data(
            vehicle_id, 
            timestamp=vehicle_data['timestamp'], 
            location=vehicle_data['location']
        )

        if (vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['latitude']
                and vehicle_data['location'][1] <= BIRMINGHAM_COORDINATES['longitude']):
            print("Vehicle has reached Birmingham. Simulation ending...")
            break

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        # print(vehicle_data)
        # print(gps_data)
        # print(traffic_camera_data)
        # print(weather_data)
        # print(emergency_incident_data)

        time.sleep(5)

if __name__ == "__main__":
    VEHICLE_ID = "Vehicle-FDX-9667"

    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f"Kafka error: {err}")
    }

    producer = SerializingProducer(producer_config)
    print()

    try:
        simulate_journey(producer, VEHICLE_ID)
    except KeyboardInterrupt:
        print("Simulation ended by user")
    except Exception as e:
        print(f"Unexpected error occured: {e}")
