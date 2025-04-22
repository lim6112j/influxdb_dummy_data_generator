import random
import time
import argparse

def generate_car_data(duration):
    """Generates dummy car movement data in InfluxDB line protocol format every 1 second."""

    start_time = time.time()
    end_time = start_time + (duration * 3600)  # Convert hours to seconds
    current_time = start_time
    while current_time <= end_time:
        # Simulate car data
        latitude = round(random.uniform(34.0, 34.1), 6)  # Example range
        longitude = round(random.uniform(-118.2, -118.1), 6)  # Example range
        speed = round(random.uniform(0, 60), 2)  # Speed in mph
        heading = round(random.uniform(0, 359), 2)  # Heading in degrees
        timestamp = int(current_time * 1e9)  # Nanosecond timestamp

        # Construct the line protocol string
        line = f"car_data,car_id=1 latitude={latitude},longitude={longitude},speed={speed},heading={heading} {timestamp}"
        print(line)

        current_time += 1
        time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate car data for a given time duration.")
    parser.add_argument("--duration", type=int, help="Duration in hours", required=True)

    args = parser.parse_args()

    generate_car_data(args.duration)
