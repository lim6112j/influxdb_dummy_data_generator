import random
import time
import argparse
import os
import math
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


def get_route_from_osrm(origin, destination, osrm_url):
    """Fetch route coordinates from OSRM server."""
    import requests
    
    # Format coordinates for OSRM (longitude,latitude)
    origin_str = f"{origin[1]},{origin[0]}"
    destination_str = f"{destination[1]},{destination[0]}"
    
    # OSRM route API endpoint
    url = f"{osrm_url}/route/v1/driving/{origin_str};{destination_str}"
    params = {
        'overview': 'full',
        'geometries': 'geojson',
        'steps': 'true'
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data['code'] != 'Ok':
            raise Exception(f"OSRM error: {data.get('message', 'Unknown error')}")
        
        # Extract coordinates from the route geometry
        coordinates = data['routes'][0]['geometry']['coordinates']
        # Convert from [lon, lat] to [lat, lon] format
        route_points = [(coord[1], coord[0]) for coord in coordinates]
        
        # Get total duration and distance
        duration = data['routes'][0]['duration']  # in seconds
        distance = data['routes'][0]['distance']  # in meters
        
        print(f"✓ Route fetched: {len(route_points)} points, {distance/1000:.2f}km, {duration/60:.1f}min")
        
        return route_points, duration, distance
        
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to OSRM server: {e}")
        return None, None, None
    except Exception as e:
        print(f"Error processing OSRM response: {e}")
        return None, None, None


def generate_car_data(duration, origin, destination, osrm_url):
    """Generates dummy car movement data and writes it to InfluxDB every 1 second."""

    start_time = time.time()
    end_time = start_time + (duration * 3600)  # Convert hours to seconds

    # Load environment variables
    env_file = '.env'
    if os.path.exists(env_file):
        print(f"Loading environment from: {os.path.abspath(env_file)}")
        load_dotenv(env_file, override=True)
    else:
        print(f"Warning: .env file not found at {os.path.abspath(env_file)}")
        load_dotenv(override=True)

    # Load environment variables
    influxdb_url = os.getenv('INFLUXDB_URL')
    influxdb_token = os.getenv('INFLUXDB_TOKEN')
    influxdb_org = os.getenv('INFLUXDB_ORG')
    influxdb_bucket = os.getenv('INFLUXDB_BUCKET')

    print(f"Loaded environment variables:")
    print(f"  INFLUXDB_URL: {influxdb_url}")
    print(f"  INFLUXDB_ORG: {influxdb_org}")
    print(f"  INFLUXDB_BUCKET: {influxdb_bucket}")
    print(
        f"  INFLUXDB_TOKEN: {influxdb_token[:20]}..." if influxdb_token else "Token: None")

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
    print(
        f"Token: {influxdb_token[:20]}..." if influxdb_token else "Token: None")

    # Initialize InfluxDB client
    try:
        client = InfluxDBClient(
            url=influxdb_url, token=influxdb_token, org=influxdb_org)

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

    # Get route from OSRM
    print(f"Fetching route from OSRM server at {osrm_url}")
    print(f"Origin: {origin[0]}, {origin[1]}")
    print(f"Destination: {destination[0]}, {destination[1]}")
    
    route_points, route_duration, route_distance = get_route_from_osrm(origin, destination, osrm_url)
    
    if not route_points:
        print("Failed to get route from OSRM. Exiting.")
        client.close()
        return
    
    # Calculate how many times to repeat the route to fill the duration
    total_seconds = duration * 3600
    route_duration_seconds = route_duration
    
    if total_seconds < route_duration_seconds:
        print(f"Warning: Requested duration ({duration}h) is shorter than route duration ({route_duration_seconds/3600:.2f}h)")
        print("Will only generate data for partial route.")
    
    # Calculate speed for each segment to match real-world timing
    segment_speeds = []
    for i in range(len(route_points) - 1):
        # Calculate distance between consecutive points
        lat1, lon1 = route_points[i]
        lat2, lon2 = route_points[i + 1]
        
        # Haversine formula for distance
        R = 6371000  # Earth's radius in meters
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        segment_distance = R * c
        
        # Calculate time for this segment (assuming equal time distribution)
        segment_time = route_duration / (len(route_points) - 1)
        
        # Calculate speed in km/h
        speed_ms = segment_distance / segment_time if segment_time > 0 else 0
        speed_kmh = speed_ms * 3.6  # Convert m/s to km/h
        segment_speeds.append(round(speed_kmh, 2))
    
    current_time = start_time
    route_index = 0
    cycle_count = 0
    
    while current_time <= end_time:
        # Check if we've started a new cycle
        elapsed_time = current_time - start_time
        current_cycle = int(elapsed_time // route_duration)
        
        if current_cycle > cycle_count:
            cycle_count = current_cycle
            print(f"Starting route cycle #{cycle_count + 1}")
        
        # Move to next route point based on timing
        elapsed_time = current_time - start_time
        
        # Calculate which cycle we're in
        current_cycle = int(elapsed_time // route_duration)
        time_in_current_cycle = elapsed_time % route_duration
        
        # Calculate progress through current route (0.0 to 1.0)
        route_progress = time_in_current_cycle / route_duration
        
        # Calculate route index based on progress
        route_index = int(route_progress * (len(route_points) - 1))
        route_index = min(route_index, len(route_points) - 1)
        
        # Debug output every 30 seconds
        if int(current_time) % 30 == 0:
            print(f"Debug: Cycle {current_cycle + 1}, Progress: {route_progress:.2%}, Point: {route_index + 1}/{len(route_points)}")
        
        latitude, longitude = route_points[route_index]
        
        # Get speed for current segment
        speed = segment_speeds[min(route_index, len(segment_speeds) - 1)] if segment_speeds else 0
        
        # Calculate heading to next point
        if route_index < len(route_points) - 1:
            lat1, lon1 = route_points[route_index]
            lat2, lon2 = route_points[route_index + 1]
            
            # Calculate bearing
            dlon = math.radians(lon2 - lon1)
            lat1_rad = math.radians(lat1)
            lat2_rad = math.radians(lat2)
            
            y = math.sin(dlon) * math.cos(lat2_rad)
            x = math.cos(lat1_rad) * math.sin(lat2_rad) - math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(dlon)
            
            heading = math.degrees(math.atan2(y, x))
            heading = (heading + 360) % 360  # Normalize to 0-360
        else:
            heading = 0  # At destination
        
        heading = round(heading, 2)

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
            write_api.write(bucket=influxdb_bucket,
                            org=influxdb_org, record=point)
            if int(current_time) % 10 == 0:  # Print every 10 seconds
                print(
                    f"✓ Written data point at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time))} - Point {route_index + 1}/{len(route_points)}")
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
    parser = argparse.ArgumentParser(
        description="Generate car data and inject it into InfluxDB for a given duration.")
    parser.add_argument("--duration", type=int,
                        help="Duration in hours", required=True)
    parser.add_argument("--origin", type=str, nargs=2, metavar=('LAT', 'LON'),
                        help="Origin coordinates (latitude longitude)", required=True)
    parser.add_argument("--destination", type=str, nargs=2, metavar=('LAT', 'LON'),
                        help="Destination coordinates (latitude longitude)", required=True)
    parser.add_argument("--osrm-url", type=str, default="http://localhost:5001",
                        help="OSRM server URL (default: http://localhost:5001)")

    args = parser.parse_args()

    origin = (float(args.origin[0]), float(args.origin[1]))
    destination = (float(args.destination[0]), float(args.destination[1]))
    
    generate_car_data(args.duration, origin, destination, args.osrm_url)
