import random
import time
import argparse
import os
import math
import requests
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


def get_route_from_osrm(origin, destination, osrm_url):
    """Fetch route coordinates from OSRM server with step data."""

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
            raise Exception(
                f"OSRM error: {data.get('message', 'Unknown error')}")

        route = data['routes'][0]
        
        # Extract step-by-step locations
        step_locations = []
        total_step_duration = 0
        total_step_distance = 0
        
        for leg in route['legs']:
            for step in leg['steps']:
                # Get maneuver location (intersection/turn point)
                maneuver_location = step['maneuver']['location']
                step_info = {
                    'location': [maneuver_location[1], maneuver_location[0]],  # [lat, lon]
                    'duration': step['duration'],  # seconds
                    'distance': step['distance'],  # meters
                    'instruction': step['maneuver']['type'],
                    'name': step.get('name', ''),
                    'speed_kmh': 0  # Will calculate below
                }
                
                # Calculate speed for this step
                if step['duration'] > 0:
                    speed_ms = step['distance'] / step['duration']
                    step_info['speed_kmh'] = round(speed_ms * 3.6, 2)  # Convert m/s to km/h
                
                step_locations.append(step_info)
                total_step_duration += step['duration']
                total_step_distance += step['distance']

        # Also get the full route geometry for backup
        coordinates = route['geometry']['coordinates']
        route_points = [(coord[1], coord[0]) for coord in coordinates]

        # Get total duration and distance
        duration = route['duration']  # in seconds
        distance = route['distance']  # in meters

        print(f"✓ Route fetched: {len(step_locations)} steps, {distance/1000:.2f}km, {duration/60:.1f}min")
        print(f"✓ Step locations: {len(step_locations)} maneuver points")

        return route_points, duration, distance, step_locations

    except requests.exceptions.RequestException as e:
        print(f"Error connecting to OSRM server: {e}")
        return None, None, None, None
    except Exception as e:
        print(f"Error processing OSRM response: {e}")
        return None, None, None, None


def generate_intermediate_points(start_location, end_location, duration, speed_kmh):
    """Generate intermediate points between two locations based on duration and speed."""
    lat1, lon1 = start_location
    lat2, lon2 = end_location
    
    # Calculate number of points based on duration (1 point per second)
    num_points = max(int(duration), 1)
    
    points = []
    for i in range(num_points):
        # Linear interpolation between start and end points
        progress = i / max(num_points - 1, 1)
        
        # Interpolate latitude and longitude
        lat = lat1 + (lat2 - lat1) * progress
        lon = lon1 + (lon2 - lon1) * progress
        
        points.append({
            'latitude': lat,
            'longitude': lon,
            'timestamp': f'2023-01-01T{i//3600:02d}:{(i%3600)//60:02d}:{i%60:02d}Z',
            'speed_kmh': speed_kmh,
            'progress': progress
        })
    
    return points


def clear_existing_car_data(client, influxdb_bucket, influxdb_org):
    """Clear existing car data from InfluxDB"""
    try:
        delete_api = client.delete_api()
        
        # Delete all car_data measurements for car_id=1
        start_time = "1970-01-01T00:00:00Z"  # Delete all historical data
        stop_time = "2030-01-01T00:00:00Z"   # Far future to ensure we get everything
        
        delete_api.delete(
            start=start_time,
            stop=stop_time,
            predicate='_measurement="car_data" AND car_id="1"',
            bucket=influxdb_bucket,
            org=influxdb_org
        )
        
        print("✓ Cleared existing car data from InfluxDB")
        
    except Exception as e:
        print(f"Warning: Error clearing existing data: {e}")


def generate_car_data(duration, origin, destination, osrm_url, movement_mode='one-way', clear_existing=True):
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
        
        # Clear existing data if requested
        if clear_existing:
            print("Clearing existing car data from InfluxDB...")
            clear_existing_car_data(client, influxdb_bucket, influxdb_org)

    except Exception as e:
        print(f"Error connecting to InfluxDB: {e}")
        return

    # Get route from OSRM using the provided coordinates
    print(f"Fetching route from OSRM server at {osrm_url}")
    print(f"Origin: {origin[0]}, {origin[1]}")
    print(f"Destination: {destination[0]}, {destination[1]}")

    route_points, route_duration, route_distance, step_locations = get_route_from_osrm(
        origin, destination, osrm_url)

    if not route_points or not step_locations:
        print("Failed to get route from OSRM. Exiting.")
        client.close()
        return

    # Calculate how many times to repeat the route to fill the duration
    total_seconds = duration * 3600
    route_duration_seconds = route_duration

    if total_seconds < route_duration_seconds:
        print(
            f"Warning: Requested duration ({duration}h) is shorter than route duration ({route_duration_seconds/3600:.2f}h)")
        print("Will only generate data for partial route.")

    print(f"Using {len(step_locations)} step locations for car movement")
    for i, step in enumerate(step_locations):
        print(f"Step {i+1}: {step['instruction']} at {step['location'][0]:.6f}, {step['location'][1]:.6f} - {step['speed_kmh']} km/h")

    # Generate route points based on movement mode
    print(f"Generating route points from OSRM geometry (mode: {movement_mode})...")
    all_route_points = []
    
    if movement_mode == 'round-trip':
        # Get return route from destination back to origin
        print("Fetching return route from OSRM...")
        return_route_points, return_duration, return_distance, return_step_locations = get_route_from_osrm(
            destination, origin, osrm_url)
        
        if not return_route_points or not return_step_locations:
            print("Failed to get return route from OSRM. Using reverse of forward route.")
            # Use reverse of forward route as fallback
            return_route_points = list(reversed(route_points))
            return_duration = route_duration
            return_distance = route_distance
            return_step_locations = list(reversed(step_locations))
        
        # Generate forward route points (start to destination)
        forward_route_points = []
        if route_points and len(route_points) > 1:
            for i, point in enumerate(route_points):
                speed_kmh = (route_distance / 1000) / (route_duration / 3600) if route_duration > 0 else 30
                step_index = min(i // max(1, len(route_points) // len(step_locations)), len(step_locations) - 1)
                current_step = step_locations[step_index] if step_index < len(step_locations) else step_locations[-1]
                
                forward_route_points.append({
                    'location': point,
                    'speed_kmh': current_step.get('speed_kmh', speed_kmh),
                    'instruction': current_step.get('instruction', 'continue'),
                    'step_index': step_index,
                    'intermediate_index': i % max(1, len(route_points) // len(step_locations)),
                    'step_duration': current_step.get('duration', 0),
                    'step_distance': current_step.get('distance', 0),
                    'step_name': current_step.get('name', ''),
                    'direction': 'forward'
                })
        
        # Generate backward route points (destination to start)
        backward_route_points = []
        if return_route_points and len(return_route_points) > 1:
            for i, point in enumerate(return_route_points):
                speed_kmh = (return_distance / 1000) / (return_duration / 3600) if return_duration > 0 else 30
                step_index = min(i // max(1, len(return_route_points) // len(return_step_locations)), len(return_step_locations) - 1)
                current_step = return_step_locations[step_index] if step_index < len(return_step_locations) else return_step_locations[-1]
                
                backward_route_points.append({
                    'location': point,
                    'speed_kmh': current_step.get('speed_kmh', speed_kmh),
                    'instruction': current_step.get('instruction', 'continue'),
                    'step_index': step_index,
                    'intermediate_index': i % max(1, len(return_route_points) // len(return_step_locations)),
                    'step_duration': current_step.get('duration', 0),
                    'step_distance': current_step.get('distance', 0),
                    'step_name': current_step.get('name', ''),
                    'direction': 'backward'
                })
        
        # Combine forward and backward routes into a complete cycle
        all_route_points = forward_route_points + backward_route_points
        print(f"Forward route: {len(forward_route_points)} points")
        print(f"Backward route: {len(backward_route_points)} points")
        
    else:
        # One-way mode: just use the forward route
        if route_points and len(route_points) > 1:
            for i, point in enumerate(route_points):
                speed_kmh = (route_distance / 1000) / (route_duration / 3600) if route_duration > 0 else 30
                step_index = min(i // max(1, len(route_points) // len(step_locations)), len(step_locations) - 1)
                current_step = step_locations[step_index] if step_index < len(step_locations) else step_locations[-1]
                
                all_route_points.append({
                    'location': point,
                    'speed_kmh': current_step.get('speed_kmh', speed_kmh),
                    'instruction': current_step.get('instruction', 'continue'),
                    'step_index': step_index,
                    'intermediate_index': i % max(1, len(route_points) // len(step_locations)),
                    'step_duration': current_step.get('duration', 0),
                    'step_distance': current_step.get('distance', 0),
                    'step_name': current_step.get('name', ''),
                    'direction': 'forward'
                })
        else:
            # Fallback to step-based generation if route geometry is not available
            for i, step in enumerate(step_locations):
                all_route_points.append({
                    'location': step['location'],
                    'speed_kmh': step['speed_kmh'],
                    'instruction': step['instruction'],
                    'step_index': i,
                    'intermediate_index': 0,
                    'step_duration': step['duration'],
                    'step_distance': step['distance'],
                    'step_name': step.get('name', ''),
                    'direction': 'forward'
                })
    
    print(f"Generated {len(all_route_points)} total points for {movement_mode} movement")
    
    # Calculate total route time based on generated points
    total_route_time = len(all_route_points)  # 1 second per point
    
    current_time = start_time
    point_index = 0
    cycle_count = 0

    while current_time <= end_time:
        # Calculate elapsed time and determine current point
        elapsed_time = current_time - start_time
        
        # For one-way mode, stop when we reach the destination
        if movement_mode == 'one-way':
            point_index = int(elapsed_time) % len(all_route_points)
            
            # If we've reached the end of the route in one-way mode, stop generating data
            if point_index >= len(all_route_points) - 1:
                print(f"✓ Reached destination in one-way mode. Stopping data generation.")
                break
        else:
            # For round-trip mode, continue cycling through the route
            # Calculate which cycle we're in
            current_cycle = int(elapsed_time // total_route_time)
            time_in_current_cycle = elapsed_time % total_route_time
            
            # Check if we've started a new cycle
            if current_cycle > cycle_count:
                cycle_count = current_cycle
                print(f"Starting route cycle #{cycle_count + 1}")
            
            # Calculate point index based on elapsed time
            point_index = int(time_in_current_cycle) % len(all_route_points)
        
        # Get current point data
        current_point = all_route_points[point_index]
        latitude, longitude = current_point['location']
        speed = float(current_point['speed_kmh'])
        
        # Calculate heading to next point
        if point_index < len(all_route_points) - 1:
            lat1, lon1 = current_point['location']
            lat2, lon2 = all_route_points[point_index + 1]['location']
            
            # Calculate bearing
            dlon = math.radians(lon2 - lon1)
            lat1_rad = math.radians(lat1)
            lat2_rad = math.radians(lat2)
            
            y = math.sin(dlon) * math.cos(lat2_rad)
            x = math.cos(lat1_rad) * math.sin(lat2_rad) - \
                math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(dlon)
            
            heading = math.degrees(math.atan2(y, x))
            heading = (heading + 360) % 360  # Normalize to 0-360
        else:
            heading = 0  # At destination
        
        heading = float(round(heading, 2))

        # Debug output every 30 seconds
        if int(current_time) % 30 == 0:
            step_info = current_point.get('instruction', 'moving')
            direction = current_point.get('direction', 'unknown')
            progress_info = f"Point {point_index + 1}/{len(all_route_points)}"
            if movement_mode == 'round-trip':
                progress_info += f" (Cycle {cycle_count + 1})"
            print(f"Debug: {progress_info} ({direction}): {step_info} - {latitude:.6f}, {longitude:.6f} - {speed} km/h")

        # Create a Point object with step information
        point = Point("car_data") \
            .tag("car_id", "1") \
            .field("latitude", latitude) \
            .field("longitude", longitude) \
            .field("speed", speed) \
            .field("heading", heading) \
            .field("step_index", int(current_point.get('step_index', 0))) \
            .field("instruction", current_point.get('instruction', 'moving')) \
            .field("intermediate_index", int(current_point.get('intermediate_index', 0))) \
            .field("cycle_count", int(cycle_count)) \
            .field("step_duration", float(current_point.get('step_duration', 0))) \
            .field("step_distance", float(current_point.get('step_distance', 0))) \
            .field("step_name", current_point.get('step_name', '')) \
            .time(int(current_time * 1e9), "ns")

        # Write the data to InfluxDB
        try:
            write_api.write(bucket=influxdb_bucket,
                            org=influxdb_org, record=point)
            if int(current_time) % 30 == 0:  # Print every 30 seconds
                progress_info = f"Point {point_index + 1}/{len(all_route_points)}"
                if movement_mode == 'round-trip':
                    progress_info += f" (Cycle {cycle_count + 1})"
                print(
                    f"✓ Written data point at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time))} - {progress_info}")
        except Exception as e:
            print(f"Error writing data: {e}")
            client.close()
            return

        current_time += 1
        time.sleep(1)

    # Close the client
    client.close()
    
    # Calculate actual points generated
    actual_points = int(current_time - start_time)
    if movement_mode == 'one-way':
        print(f"✓ Successfully generated {actual_points} data points (one-way to destination)")
    else:
        print(f"✓ Successfully generated {actual_points} data points (round-trip mode)")


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
    parser.add_argument("--movement-mode", type=str, choices=['one-way', 'round-trip'], 
                        default='one-way', help="Movement mode: one-way or round-trip (default: one-way)")
    parser.add_argument("--no-clear", action='store_true',
                        help="Don't clear existing car data before generating new data")

    args = parser.parse_args()

    origin = (float(args.origin[0]), float(args.origin[1]))
    destination = (float(args.destination[0]), float(args.destination[1]))

    generate_car_data(args.duration, origin, destination, args.osrm_url, args.movement_mode, clear_existing=not args.no_clear)
