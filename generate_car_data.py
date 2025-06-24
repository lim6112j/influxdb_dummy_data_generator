import random
import time
import argparse
import os
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

def generate_car_data(duration):
    """Generates dummy car movement data and writes it to InfluxDB every 1 second."""

    start_time = time.time()
    end_time = start_time + (duration * 3600)  # Convert hours to seconds

    # Load environment variables
    load_dotenv()
    influxdb_url = os.getenv('INFLUXDB_URL')
    influxdb_token = os.getenv('INFLUXDB_TOKEN')
    influxdb_org = os.getenv('INFLUXDB_ORG')
    influxdb_bucket = os.getenv('INFLUXDB_BUCKET')
    
    # Use defaults if not found in environment
    if not influxdb_url:
        influxdb_url = 'http://localhost:8086'
    if not influxdb_org:
        influxdb_org = 'ciel'
    if not influxdb_bucket:
        influxdb_bucket = 'location_202506'

    # Check if all required environment variables are set
    if not all([influxdb_url, influxdb_token, influxdb_org, influxdb_bucket]):
        print("Error: Missing required environment variables.")
        print(f"INFLUXDB_URL: {'✓' if influxdb_url else '✗'}")
        print(f"INFLUXDB_TOKEN: {'✓' if influxdb_token else '✗'}")
        print(f"INFLUXDB_ORG: {'✓' if influxdb_org else '✗'}")
        print(f"INFLUXDB_BUCKET: {'✓' if influxdb_bucket else '✗'}")
        print("\nPlease check your .env file and ensure all variables are set correctly.")
        return

    print(f"Connecting to InfluxDB at: {influxdb_url}")
    print(f"Organization: {influxdb_org}")
    print(f"Bucket: {influxdb_bucket}")
    print(f"Token: {influxdb_token[:20]}..." if influxdb_token else "Token: None")

    # Initialize InfluxDB client
    try:
        client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)
        
        # Test the connection by checking if we can access the health endpoint
        health = client.health()
        print(f"✓ InfluxDB health status: {health.status}")
        
        # Test bucket access
        buckets_api = client.buckets_api()
        try:
            bucket = buckets_api.find_bucket_by_name(influxdb_bucket)
            if not bucket:
                print(f"Error: Bucket '{influxdb_bucket}' not found.")
                print("Available buckets:")
                buckets = buckets_api.find_buckets()
                for b in buckets.buckets:
                    print(f"  - {b.name}")
                client.close()
                return
            print(f"✓ Found bucket: {bucket.name}")
        except Exception as e:
            print(f"Error accessing buckets: {e}")
            print("This might be a permissions issue with your token.")
            client.close()
            return
        
        write_api = client.write_api(write_options=SYNCHRONOUS)
        
    except Exception as e:
        print(f"Error connecting to InfluxDB: {e}")
        return

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
        try:
            write_api.write(bucket=influxdb_bucket, org=influxdb_org, record=point)
            if int(current_time) % 10 == 0:  # Print every 10 seconds
                print(f"✓ Written data point at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time))}")
        except Exception as e:
            print(f"Error writing data: {e}")
            client.close()
            return

        current_time += 1
        time.sleep(1)

    # Close the client
    client.close()
    print(f"✓ Successfully generated {int(duration * 3600)} data points")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate car data and inject it into InfluxDB for a given duration.")
    parser.add_argument("--duration", type=int, help="Duration in hours", required=True)

    args = parser.parse_args()

    generate_car_data(args.duration)
