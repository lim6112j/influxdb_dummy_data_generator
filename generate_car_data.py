import random
import time
import argparse
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

def generate_car_data(duration, influxdb_url, influxdb_token, influxdb_org, influxdb_bucket):
    """Generates dummy car movement data and writes it to InfluxDB every 1 second."""

    start_time = time.time()
    end_time = start_time + (duration * 3600)  # Convert hours to seconds

    # Initialize InfluxDB client
    client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    current_time = start_time
    while current_time <= end_time:
        # Simulate car data
        latitude = round(random.uniform(34.0, 34.1), 6)  # Example range
        longitude = round(random.uniform(-118.2, -118.1), 6)  # Example range
        speed = round(random.uniform(0, 60), 2)  # Speed in mph
        heading = round(random.uniform(0, 359), 2)  # Heading in degrees

        # Create a Point object
        point = Point("car_data") \
            .tag("car_id", "1") \
            .field("latitude", latitude) \
            .field("longitude", longitude) \
            .field("speed", speed) \
            .field("heading", heading) \
            .time(int(current_time * 1e9), "ns")

        # Write the data to InfluxDB
        write_api.write(bucket=influxdb_bucket, org=influxdb_org, record=point)

        current_time += 1
        time.sleep(1)

    # Close the client
    client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate car data and inject it into InfluxDB for a given duration.")
    parser.add_argument("--duration", type=int, help="Duration in hours", required=True)
    parser.add_argument("--influxdb_url", type=str, help="InfluxDB URL", required=True)
    parser.add_argument("--influxdb_token", type=str, help="InfluxDB token", required=True)
    parser.add_argument("--influxdb_org", type=str, help="InfluxDB organization", required=True)
    parser.add_argument("--influxdb_bucket", type=str, help="InfluxDB bucket", required=True)

    args = parser.parse_args()

    generate_car_data(args.duration, args.influxdb_url, args.influxdb_token, args.influxdb_org, args.influxdb_bucket)
