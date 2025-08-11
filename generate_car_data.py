import random
import time
import argparse
import os
import math
import requests
import json
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dynamic_route_manager import route_manager


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


def create_kafka_producer(bootstrap_servers, sasl_username, sasl_password):
    """Create and return a Kafka producer"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='PLAIN',
            sasl_plain_username=sasl_username,
            sasl_plain_password=sasl_password,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        print(f"✓ Connected to Kafka at {bootstrap_servers}")
        return producer
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        return None


def create_vehicle_message(vehicle_id, probe_name, longitude, latitude, speed_kmh, heading, 
                          elevation=5000, pos_accuracy=500):
    """Create a vehicle message in the kuk11-2.3-dv.json format"""
    current_time = datetime.now()
    message_time = int(time.time() * 1000)  # Current timestamp in milliseconds
    
    # Convert speed from km/h to the units used in the JSON (appears to be in units where 5000 = ~50 km/h)
    speed_units = int(speed_kmh * 100) if speed_kmh > 0 else 0
    
    # Convert heading from degrees to decidegrees (multiply by 100)
    heading_decidegrees = int(heading * 100)
    
    message = {
        "messageTime": message_time,
        "messageType": 0,
        "dataTxt": {
            "authenticationInfo": "F2",
            "dataPacketNbr": 1,
            "dataPacketPriorityCd": 5,
            "pdu": {
                "userNameTxt": "VEHICLE001",
                "frED": 0,
                "publication": {
                    "publishGuaranteedBool": True,
                    "format": {
                        "data": {
                            "publishSerialNbr": 1,
                            "publishSerialCnt": 1,
                            "publishLatePublicationFlagBool": False,
                            "publicationType": {
                                "publishManagementCd": 1,
                                "publicationData": {
                                    "endApplicationMessageId": "MSG001",
                                    "endApplicationMessageMsg": {
                                        "probeID": {
                                            "name": probe_name,
                                            "id": "RANDOM123"
                                        },
                                        "vehicleID": vehicle_id,
                                        "vehicleType": "BUS",
                                        "timeInfo": {
                                            "year": current_time.year,
                                            "month": current_time.month,
                                            "day": current_time.day,
                                            "hour": current_time.hour,
                                            "minute": current_time.minute,
                                            "second": current_time.second,
                                            "millisecond": current_time.microsecond // 1000,
                                            "alivecount": 1
                                        },
                                        "thePosition": {
                                            "longitude": longitude,
                                            "latitude": latitude,
                                            "elevation": elevation,
                                            "heading": heading_decidegrees,
                                            "speed": speed_units,
                                            "posAccuracy": pos_accuracy
                                        },
                                        "vehicleEvents": {
                                            "hazardLights": False,
                                            "absActivated": False,
                                            "hardBraking": False,
                                            "lightsChanged": 0,
                                            "flatTire": 0,
                                            "disabledVehicle": False,
                                            "getOnDown": False,
                                            "trouble": False,
                                            "hardDeceleration": False,
                                            "hardStop": False,
                                            "hardTurn": False,
                                            "uTurn": False
                                        },
                                        "serviceEvents": {
                                            "roadWork": False,
                                            "waypointArrived": False,
                                            "waypoinDeparture": False,
                                            "eta": 300,
                                            "serviceDoorOpen": False,
                                            "LiftOn": False
                                        },
                                        "vehicleStatus": {
                                            "lights": False,
                                            "lightBar": False,
                                            "brakeStatus": 0,
                                            "throttlePos": 0,
                                            "gpsStatus": 1,
                                            "transitStatus": False,
                                            "acceleration": 0,
                                            "worklanes": False,
                                            "curlane": 1,
                                            "vehicleCnt": 5,
                                            "batteryStatus": 85,
                                            "rangeStatus": 250,
                                            "failure": 0
                                        }
                                    }
                                }
                            }
                        },
                        "publishFileNameTxt": ""
                    }
                }
            }
        },
        "crcID": "7C"
    }
    
    return message


def send_kafka_message(producer, topic, message, key=None):
    """Send a message to Kafka topic"""
    try:
        future = producer.send(topic, value=message, key=key)
        record_metadata = future.get(timeout=10)
        return True
    except KafkaError as e:
        print(f"Failed to send message to Kafka: {e}")
        return False


def generate_car_data(duration, origin, destination, osrm_url, movement_mode='one-way', 
                     kafka_bootstrap_servers=None, kafka_topic=None, kafka_username=None, kafka_password=None,
                     vehicle_id=None, probe_name=None):
    """Generates dummy car movement data and sends it to Kafka every 1 second."""

    start_time = time.time()
    end_time = start_time + (duration * 3600)  # Convert hours to seconds

    # Use provided Kafka configuration or defaults
    kafka_bootstrap_servers = kafka_bootstrap_servers or "123.143.232.180:19092"
    kafka_topic = kafka_topic or "vehicle-driving-data"
    kafka_username = kafka_username or "iov"
    kafka_password = kafka_password or "iov"
    vehicle_id = vehicle_id or "ETRI_VT60_ID01"
    probe_name = probe_name or "CITSOBE-0001"

    print(f"Kafka Configuration:")
    print(f"  Bootstrap Servers: {kafka_bootstrap_servers}")
    print(f"  Topic: {kafka_topic}")
    print(f"  Username: {kafka_username}")
    print(f"  Vehicle ID: {vehicle_id}")
    print(f"  Probe Name: {probe_name}")

    # Check if all required parameters are set
    if not all([kafka_bootstrap_servers, kafka_topic, kafka_username, kafka_password, vehicle_id, probe_name]):
        print("Error: Missing required Kafka configuration.")
        print(f"Bootstrap Servers: {'✓' if kafka_bootstrap_servers else '✗'}")
        print(f"Topic: {'✓' if kafka_topic else '✗'}")
        print(f"Username: {'✓' if kafka_username else '✗'}")
        print(f"Password: {'✓' if kafka_password else '✗'}")
        print(f"Vehicle ID: {'✓' if vehicle_id else '✗'}")
        print(f"Probe Name: {'✓' if probe_name else '✗'}")
        print("\nPlease provide Kafka configuration parameters.")
        return

    print(f"Connecting to Kafka at: {kafka_bootstrap_servers}")

    # Initialize Kafka producer
    producer = create_kafka_producer(kafka_bootstrap_servers, kafka_username, kafka_password)
    if not producer:
        print("Failed to create Kafka producer. Exiting.")
        return

    # Get route from OSRM using the provided coordinates
    print(f"Fetching route from OSRM server at {osrm_url}")
    print(f"Origin: {origin[0]}, {origin[1]}")
    print(f"Destination: {destination[0]}, {destination[1]}")

    route_points, route_duration, route_distance, step_locations = get_route_from_osrm(
        origin, destination, osrm_url)

    if not route_points or not step_locations:
        print("Failed to get route from OSRM. Exiting.")
        producer.close()
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
    
    # Clean up any existing route file and set initial route in the route manager
    route_file = "current_route.json"
    if os.path.exists(route_file):
        os.remove(route_file)
        print(f"🗑️ Cleaned up existing route file: {route_file}")
    
    # Clean up any existing pause signal file at the start
    if os.path.exists('car_pause_signal.txt'):
        os.remove('car_pause_signal.txt')
        print("🧹 Cleaned up existing pause signal file")
    
    # Set initial route in the route manager
    route_manager.set_initial_route(route_points, step_locations, osrm_url, movement_mode)
    
    # Calculate total route time based on generated points
    total_route_time = len(all_route_points)  # 1 second per point
    
    current_time = start_time
    point_index = 0
    cycle_count = 0
    last_route_check = 0
    last_position = None
    pause_start_time = None
    waypoint_pause_active = False
    current_waypoints = []
    reached_waypoints = set()

    while current_time <= end_time:
        # Check for pause signal
        is_paused = os.path.exists('car_pause_signal.txt')
        
        if is_paused:
            if pause_start_time is None:
                pause_start_time = current_time
                # Check if this is a waypoint pause
                try:
                    with open('car_pause_signal.txt', 'r') as f:
                        pause_content = f.read().strip()
                        if pause_content.startswith('WAYPOINT_PAUSE:'):
                            waypoint_name = pause_content.split(':', 1)[1]
                            print(f"🛑 Car paused at waypoint: {waypoint_name} at {time.strftime('%H:%M:%S')}")
                        else:
                            print(f"🛑 Car paused at {time.strftime('%H:%M:%S')}")
                except:
                    print(f"🛑 Car paused at {time.strftime('%H:%M:%S')}")
            
            # When paused, keep streaming the same position
            if last_position is not None:
                latitude, longitude = last_position['location']
                speed = 0.0  # Car is stopped
                heading = last_position.get('heading', 0)
                    
                # Create Kafka message for paused state
                message = create_vehicle_message(
                    vehicle_id=vehicle_id,
                    probe_name=probe_name,
                    longitude=longitude,
                    latitude=latitude,
                    speed_kmh=speed,
                    heading=heading
                )

                # Send the paused data to Kafka
                try:
                    if send_kafka_message(producer, kafka_topic, message, key=vehicle_id):
                        if int(current_time) % 30 == 0:  # Print every 30 seconds when paused
                            print(f"🛑 Car paused at {latitude:.6f}, {longitude:.6f} - {time.strftime('%H:%M:%S')}")
                except Exception as e:
                    print(f"Error sending paused data to Kafka: {e}")
                
                current_time += 1
                time.sleep(1)
                continue
            else:
                # If no last position, just wait
                current_time += 1
                time.sleep(1)
                continue
        else:
            # Car is not paused
            if pause_start_time is not None:
                pause_duration = current_time - pause_start_time
                if waypoint_pause_active:
                    print(f"▶️ Car resumed from waypoint pause after {pause_duration:.0f} seconds at {time.strftime('%H:%M:%S')}")
                    waypoint_pause_active = False
                else:
                    print(f"▶️ Car resumed after {pause_duration:.0f} seconds at {time.strftime('%H:%M:%S')}")
                pause_start_time = None

        # Check for route updates every iteration (immediate response)
        current_route_points, current_step_locations, route_was_updated, update_timestamp = route_manager.get_current_route_data(reset_update_flag=True)
        
        # Debug: Print route check status only when there's an actual update
        if route_was_updated:
            print(f"🔍 Route check at {time.strftime('%H:%M:%S')}: updated={route_was_updated}, points={len(current_route_points) if current_route_points else 0}")
        
        if route_was_updated and current_route_points and update_timestamp > last_route_check:
            print(f"🔄 ROUTE UPDATE DETECTED at {time.strftime('%H:%M:%S')}! Switching to new route with {len(current_route_points)} points")
            print(f"🔄 Old route had {len(all_route_points)} points, current point_index: {point_index}")
            
            # Update the last route check timestamp to prevent repeated processing
            last_route_check = update_timestamp
            
            # Load user-specified waypoints from route file for auto-pause functionality
            current_waypoints = []
            reached_waypoints = set()
            
            # Try to load waypoints from the route file
            try:
                import json
                if os.path.exists('current_route.json'):
                    with open('current_route.json', 'r') as f:
                        route_data = json.load(f)
                        user_waypoints = route_data.get('user_waypoints', [])
                        
                        for i, waypoint in enumerate(user_waypoints):
                            waypoint_info = {
                                'location': (waypoint['lat'], waypoint['lng']),
                                'name': waypoint.get('name', f'Waypoint {i+1}'),
                                'waypoint_index': i
                            }
                            current_waypoints.append(waypoint_info)
                        
                        print(f"🎯 Loaded {len(current_waypoints)} user-specified waypoints for auto-pause:")
                        for wp in current_waypoints:
                            print(f"   - {wp['name']} at ({wp['location'][0]:.6f}, {wp['location'][1]:.6f})")
            except Exception as e:
                print(f"Warning: Could not load user waypoints: {e}")
            
            # Store current position before switching
            if point_index < len(all_route_points):
                current_lat, current_lon = all_route_points[point_index]['location']
                print(f"🔄 Current position: {current_lat:.6f}, {current_lon:.6f}")
            else:
                current_lat, current_lon = all_route_points[-1]['location'] if all_route_points else (0, 0)
                print(f"🔄 Using last position: {current_lat:.6f}, {current_lon:.6f}")
            
            # Rebuild all_route_points with the new route
            print(f"🔄 Rebuilding route points from {len(current_route_points)} OSRM points...")
            all_route_points = []
            
            # Always use one-way mode for new routes to ensure immediate application
            for i, point in enumerate(current_route_points):
                step_index = min(i // max(1, len(current_route_points) // len(current_step_locations)), len(current_step_locations) - 1)
                current_step = current_step_locations[step_index] if step_index < len(current_step_locations) else current_step_locations[-1]
                
                all_route_points.append({
                    'location': point,
                    'speed_kmh': current_step.get('speed_kmh', 30),
                    'instruction': current_step.get('instruction', 'continue'),
                    'step_index': step_index,
                    'intermediate_index': i % max(1, len(current_route_points) // len(current_step_locations)),
                    'step_duration': current_step.get('duration', 0),
                    'step_distance': current_step.get('distance', 0),
                    'step_name': current_step.get('name', ''),
                    'direction': 'forward'
                })
            
            print(f"🔄 Built {len(all_route_points)} route points from new route")
            
            # Find the closest point on the new route to continue smoothly
            if all_route_points and current_lat != 0 and current_lon != 0:
                min_distance = float('inf')
                closest_index = 0
                
                for i, route_point in enumerate(all_route_points):
                    new_lat, new_lon = route_point['location']
                    # Calculate distance using simple Euclidean distance
                    distance = math.sqrt((current_lat - new_lat)**2 + (current_lon - new_lon)**2)
                    if distance < min_distance:
                        min_distance = distance
                        closest_index = i
                
                # Start from the closest point on the new route
                point_index = closest_index
                print(f"✓ Switched to new route, continuing from point {point_index + 1}/{len(all_route_points)} (closest to current position)")
            else:
                # Start from beginning if no current position
                point_index = 0
                print(f"✓ Switched to new route, starting from beginning")
            
            # Reset cycle count and update total route time
            cycle_count = 0
            total_route_time = len(all_route_points)
            
            # Force movement mode to one-way for new routes to ensure they complete
            movement_mode = 'one-way'
            print(f"✅ ROUTE SWITCH COMPLETE: {len(all_route_points)} points, mode: {movement_mode}, starting at point {point_index + 1}")

        # Calculate elapsed time and determine current point
        elapsed_time = current_time - start_time
        
        # For one-way mode, stop when we reach the destination
        if movement_mode == 'one-way':
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
        
        # Ensure point_index is within bounds
        if point_index >= len(all_route_points):
            point_index = len(all_route_points) - 1
        
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
        
        # Store current position for pause functionality
        last_position = {
            'location': (latitude, longitude),
            'speed': speed,
            'heading': heading,
            'step_index': current_point.get('step_index', 0),
            'intermediate_index': current_point.get('intermediate_index', 0)
        }
        
        # Check if car has reached a user-specified waypoint and should auto-pause
        if current_waypoints and not waypoint_pause_active:
            for waypoint in current_waypoints:
                waypoint_id = f"{waypoint['name']}_{waypoint['waypoint_index']}"
                
                # Check if we haven't reached this waypoint yet
                if waypoint_id not in reached_waypoints:
                    # Calculate distance to waypoint location
                    wp_lat, wp_lon = waypoint['location']
                    distance = math.sqrt((latitude - wp_lat)**2 + (longitude - wp_lon)**2)
                    
                    # If within reasonable distance (about 200 meters in degrees)
                    if distance < 0.002:  # Approximately 200 meters
                        print(f"🎯 USER WAYPOINT REACHED: {waypoint['name']} at {time.strftime('%H:%M:%S')}")
                        print(f"   Location: ({latitude:.6f}, {longitude:.6f})")
                        print(f"   Distance to waypoint: {distance:.6f} degrees")
                        print(f"   Auto-pausing car for user waypoint...")
                        
                        # Create pause signal file for waypoint pause
                        with open('car_pause_signal.txt', 'w') as f:
                            f.write(f'WAYPOINT_PAUSE:{waypoint["name"]}')
                        
                        waypoint_pause_active = True
                        reached_waypoints.add(waypoint_id)
                        pause_start_time = current_time
                        
                        print(f"🛑 Car auto-paused at user waypoint: {waypoint['name']}")
                        break

        # Debug output every 60 seconds
        if int(current_time) % 60 == 0:
            step_info = current_point.get('instruction', 'moving')
            direction = current_point.get('direction', 'unknown')
            progress_info = f"Point {point_index + 1}/{len(all_route_points)}"
            if movement_mode == 'round-trip':
                progress_info += f" (Cycle {cycle_count + 1})"
            print(f"Debug: {progress_info} ({direction}): {step_info} - {latitude:.6f}, {longitude:.6f} - {speed} km/h")

        # Create Kafka message with vehicle data
        message = create_vehicle_message(
            vehicle_id=vehicle_id,
            probe_name=probe_name,
            longitude=longitude,
            latitude=latitude,
            speed_kmh=speed,
            heading=heading
        )

        # Send the data to Kafka
        try:
            if send_kafka_message(producer, kafka_topic, message, key=vehicle_id):
                if int(current_time) % 60 == 0:  # Print every 60 seconds
                    progress_info = f"Point {point_index + 1}/{len(all_route_points)}"
                    if movement_mode == 'round-trip':
                        progress_info += f" (Cycle {cycle_count + 1})"
                    print(
                        f"✓ Sent data to Kafka at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time))} - {progress_info}")
        except Exception as e:
            print(f"❌ Error sending data to Kafka: {e}")
            producer.close()
            return

        # Only increment point_index for one-way mode, round-trip uses time-based calculation
        if movement_mode == 'one-way':
            point_index += 1
        
        current_time += 1
        time.sleep(1)

    # Close the Kafka producer
    producer.close()
    
    # Clean up pause signal file
    if os.path.exists('car_pause_signal.txt'):
        os.remove('car_pause_signal.txt')
        print("🧹 Cleaned up pause signal file")
    
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
    parser.add_argument("--kafka-bootstrap-servers", type=str, default="123.143.232.180:19092",
                        help="Kafka bootstrap servers (default: 123.143.232.180:19092)")
    parser.add_argument("--kafka-topic", type=str, default="vehicle-driving-data",
                        help="Kafka topic name (default: vehicle-driving-data)")
    parser.add_argument("--kafka-username", type=str, default="iov",
                        help="Kafka SASL username (default: iov)")
    parser.add_argument("--kafka-password", type=str, default="iov",
                        help="Kafka SASL password (default: iov)")
    parser.add_argument("--vehicle-id", type=str, default="ETRI_VT60_ID01",
                        help="Vehicle ID (default: ETRI_VT60_ID01)")
    parser.add_argument("--probe-name", type=str, default="CITSOBE-0001",
                        help="Probe name (default: CITSOBE-0001)")

    args = parser.parse_args()

    origin = (float(args.origin[0]), float(args.origin[1]))
    destination = (float(args.destination[0]), float(args.destination[1]))

    generate_car_data(args.duration, origin, destination, args.osrm_url, args.movement_mode,
                     kafka_bootstrap_servers=args.kafka_bootstrap_servers, kafka_topic=args.kafka_topic,
                     kafka_username=args.kafka_username, kafka_password=args.kafka_password,
                     vehicle_id=args.vehicle_id, probe_name=args.probe_name)
