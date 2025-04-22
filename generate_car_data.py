import random
import time

def generate_car_data(num_points=10):
    """Generates dummy car movement data in InfluxDB line protocol format."""

    for i in range(num_points):
        # Simulate car data
        latitude = round(random.uniform(34.0, 34.1), 6)  # Example range
        longitude = round(random.uniform(-118.2, -118.1), 6)  # Example range
        speed = round(random.uniform(0, 60), 2)  # Speed in mph
        heading = round(random.uniform(0, 359), 2)  # Heading in degrees
        timestamp = int(time.time_ns())  # Nanosecond timestamp

        # Construct the line protocol string
        line = f"car_data,car_id=1 latitude={latitude},longitude={longitude},speed={speed},heading={heading} {timestamp}"
        print(line)

if __name__ == "__main__":
    generate_car_data()
