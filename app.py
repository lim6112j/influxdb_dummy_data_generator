from flask import Flask, render_template, jsonify, request
import os
import requests
import time
import subprocess
import threading
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient
from dynamic_route_manager import route_manager

app = Flask(__name__)

# Load environment variables
load_dotenv()

# Global variable to track the running data generation process
running_process = None


@app.route('/')
def index():
    google_maps_api_key = os.getenv('GOOGLE_MAPS_API_KEY', 'YOUR_API_KEY')
    return render_template('index.html', google_maps_api_key=google_maps_api_key)


@app.route('/api/car-data')
def get_car_data():
    """Fetch car data from InfluxDB and return as JSON"""

    try:
        # Get InfluxDB configuration from request parameters
        influxdb_url = request.args.get(
            'influxdb_url', 'http://43.201.26.186:8086')
        influxdb_token = request.args.get('influxdb_token', '')
        influxdb_org = request.args.get('influxdb_org', 'ciel mobility')
        influxdb_bucket = request.args.get('influxdb_bucket', 'location')
        influxdb_measurement = request.args.get(
            'influxdb_measurement', 'locReports')
        influxdb_tag_name = request.args.get('influxdb_tag_name', 'device_id')
        influxdb_tag_value = request.args.get(
            'influxdb_tag_value', 'ETRI_VT60_ID01')

        print(f"Connecting to InfluxDB: {influxdb_url}")
        print(f"Organization: {influxdb_org}, Bucket: {influxdb_bucket}")

        client = InfluxDBClient(
            url=influxdb_url, token=influxdb_token, org=influxdb_org)
        query_api = client.query_api()

        # Query to get the last 500 car data points with step information
        query = f'''
        from(bucket: "{influxdb_bucket}")
          |> range(start: -10m)
          |> filter(fn: (r) => r["_measurement"] == "{influxdb_measurement}")
          |> filter(fn: (r) => r["{influxdb_tag_name}"] == "{influxdb_tag_value}")
          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
          |> sort(columns: ["_time"])
          |> unique(column: "_time")
          |> limit(n: 500)
        '''

        print(f"Executing query...")
        result = query_api.query(query=query)

        car_data = []
        for table in result:
            for record in table.records:
                car_data.append({
                    'time': record.get_time().isoformat(),
                    'lat': record.values.get('lat'),
                    'lng': record.values.get('lng'),
                    'speed': record.values.get('speed'),
                    'angle': record.values.get('angle'),
                    'alt': record.values.get('alt'),
                    'get_date': record.values.get('get_date'),
                    'step_index': record.values.get('step_index'),
                    'instruction': record.values.get('instruction'),
                    'intermediate_index': record.values.get('intermediate_index'),
                    'cycle_count': record.values.get('cycle_count'),
                    'step_duration': record.values.get('step_duration'),
                    'step_distance': record.values.get('step_distance'),
                    'step_name': record.values.get('step_name'),
                    'point_sequence': record.values.get('point_sequence')
                })

        print(f"Found {len(car_data)} data points")
        client.close()
        return jsonify(car_data)

    except Exception as e:
        print(f"Error in get_car_data: {str(e)}")
        if 'client' in locals():
            client.close()
        return jsonify({'error': str(e)}), 500


@app.route('/api/route')
def get_route():
    """Get the full route geometry from OSRM for drawing on the map"""
    try:
        # Get route parameters from query string
        origin_lat = request.args.get('origin_lat', type=float)
        origin_lon = request.args.get('origin_lon', type=float)
        dest_lat = request.args.get('dest_lat', type=float)
        dest_lon = request.args.get('dest_lon', type=float)
        osrm_url = request.args.get('osrm_url', 'http://localhost:5001')

        if any(param is None for param in [origin_lat, origin_lon, dest_lat, dest_lon]):
            return jsonify({'error': 'Missing required parameters: origin_lat, origin_lon, dest_lat, dest_lon'}), 400

        # Format coordinates for OSRM (longitude,latitude)
        origin_str = f"{origin_lon},{origin_lat}"
        destination_str = f"{dest_lon},{dest_lat}"

        # OSRM route API endpoint
        url = f"{osrm_url}/route/v1/driving/{origin_str};{destination_str}"
        params = {
            'overview': 'full',
            'geometries': 'geojson',
            'steps': 'true'
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if data['code'] != 'Ok':
            return jsonify({'error': f"OSRM error: {data.get('message', 'Unknown error')}"}), 500

        route = data['routes'][0]

        # Extract route geometry (coordinates are in [longitude, latitude] format)
        coordinates = route['geometry']['coordinates']
        # Convert to [latitude, longitude] for Google Maps
        route_points = [[coord[1], coord[0]] for coord in coordinates]

        return jsonify({
            'route_points': route_points,
            'duration': route['duration'],
            'distance': route['distance']
        })

    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'Error connecting to OSRM server: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/waypoints')
def get_waypoints():
    """Get major waypoints for the car to pass through"""
    # This is a mock implementation - replace with your actual waypoint logic
    waypoints = [
        {"lat": 35.8450, "lng": 128.5200, "name": "Checkpoint 1"},
        {"lat": 35.8500, "lng": 128.5800, "name": "Checkpoint 2"},
        {"lat": 35.8600, "lng": 128.6200, "name": "Checkpoint 3"},
        {"lat": 35.8519, "lng": 128.6727, "name": "Final Destination"}
    ]

    return jsonify({
        'waypoints': waypoints,
        'message': 'Waypoints retrieved successfully'
    })


@app.route('/api/status/osrm')
def check_osrm_status():
    """Check if OSRM server is running and accessible"""
    try:
        osrm_url = request.args.get('osrm_url', 'http://localhost:5001')

        # Try to get a simple route to test OSRM connectivity
        test_url = f"{osrm_url}/route/v1/driving/128.48,35.84;128.4827,35.8419"
        params = {'overview': 'false', 'steps': 'false'}

        response = requests.get(test_url, params=params, timeout=5)

        if response.status_code == 200:
            data = response.json()
            if data.get('code') == 'Ok':
                return jsonify({
                    'status': 'online',
                    'message': 'OSRM server is running and accessible',
                    'url': osrm_url,
                    'response_time_ms': response.elapsed.total_seconds() * 1000
                })
            else:
                return jsonify({
                    'status': 'error',
                    'message': f"OSRM returned error: {data.get('message', 'Unknown error')}",
                    'url': osrm_url
                })
        else:
            return jsonify({
                'status': 'error',
                'message': f"OSRM server returned status {response.status_code}",
                'url': osrm_url
            })

    except requests.exceptions.ConnectionError:
        return jsonify({
            'status': 'offline',
            'message': 'Cannot connect to OSRM server',
            'url': osrm_url
        })
    except requests.exceptions.Timeout:
        return jsonify({
            'status': 'timeout',
            'message': 'OSRM server connection timeout',
            'url': osrm_url
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Error checking OSRM status: {str(e)}",
            'url': osrm_url
        })


@app.route('/api/status/influxdb')
def check_influxdb_status():
    """Check if InfluxDB is accessible and configured properly"""
    try:
        # Get configuration from query parameters with defaults
        influxdb_url = request.args.get(
            'influxdb_url', 'http://43.201.26.186:8086')
        influxdb_token = request.args.get('influxdb_token', '')
        influxdb_org = request.args.get('influxdb_org', 'ciel mobility')
        influxdb_bucket = request.args.get('influxdb_bucket', 'location')
        influxdb_measurement = request.args.get(
            'influxdb_measurement', 'locReports')
        influxdb_tag_name = request.args.get('influxdb_tag_name', 'device_id')
        influxdb_tag_value = request.args.get(
            'influxdb_tag_value', 'ETRI_VT60_ID01')

        if not all([influxdb_url, influxdb_org, influxdb_bucket, influxdb_measurement, influxdb_tag_name, influxdb_tag_value]):
            missing = []
            if not influxdb_url:
                missing.append('influxdb_url')
            if not influxdb_org:
                missing.append('influxdb_org')
            if not influxdb_bucket:
                missing.append('influxdb_bucket')
            if not influxdb_measurement:
                missing.append('influxdb_measurement')
            if not influxdb_tag_name:
                missing.append('influxdb_tag_name')
            if not influxdb_tag_value:
                missing.append('influxdb_tag_value')

            return jsonify({
                'status': 'misconfigured',
                'message': f"Missing required parameters: {', '.join(missing)}",
                'url': influxdb_url or 'Not configured'
            })

        # Try to connect to InfluxDB
        client = InfluxDBClient(
            url=influxdb_url, token=influxdb_token, org=influxdb_org)

        # Test connection with a simple query
        query_api = client.query_api()
        test_query = f'buckets() |> filter(fn: (r) => r.name == "{
            influxdb_bucket}") |> limit(n: 1)'

        start_time = time.time()
        result = query_api.query(query=test_query)
        response_time = (time.time() - start_time) * 1000

        # Check if bucket exists
        bucket_found = False
        for table in result:
            if len(table.records) > 0:
                bucket_found = True
                break

        client.close()

        if bucket_found:
            return jsonify({
                'status': 'online',
                'message': 'InfluxDB is accessible and bucket exists',
                'url': influxdb_url,
                'org': influxdb_org,
                'bucket': influxdb_bucket,
                'response_time_ms': response_time
            })
        else:
            return jsonify({
                'status': 'warning',
                'message': f'InfluxDB is accessible but bucket "{influxdb_bucket}" not found',
                'url': influxdb_url,
                'org': influxdb_org,
                'bucket': influxdb_bucket,
                'response_time_ms': response_time
            })

    except Exception as e:
        error_msg = str(e)
        if 'unauthorized' in error_msg.lower():
            status = 'unauthorized'
            message = 'InfluxDB authentication failed - check token'
        elif 'connection' in error_msg.lower():
            status = 'offline'
            message = 'Cannot connect to InfluxDB server'
        else:
            status = 'error'
            message = f'InfluxDB error: {error_msg}'

        return jsonify({
            'status': status,
            'message': message,
            'url': influxdb_url or 'Not configured'
        })


@app.route('/api/start-generation', methods=['POST'])
def start_generation():
    """Start the car data generation script with given coordinates"""

    try:
        data = request.get_json()
        origin_lat = data.get('origin_lat')
        origin_lon = data.get('origin_lon')
        dest_lat = data.get('dest_lat')
        dest_lon = data.get('dest_lon')
        duration = data.get('duration', 1)  # Default 1 hour
        osrm_url = data.get('osrm_url', 'http://localhost:5001')
        movement_mode = data.get('movement_mode', 'one-way')  # Default one-way

        # InfluxDB configuration
        influxdb_url = data.get('influxdb_url', 'http://43.201.26.186:8086')
        influxdb_token = data.get('influxdb_token', '')
        influxdb_org = data.get('influxdb_org', 'ciel mobility')
        influxdb_bucket = data.get('influxdb_bucket', 'location')
        influxdb_measurement = data.get('influxdb_measurement', 'locReports')
        influxdb_tag_name = data.get('influxdb_tag_name', 'device_id')
        influxdb_tag_value = data.get('influxdb_tag_value') or data.get(
            'vehicle_id', 'ETRI_VT60_ID01')
        waypoint_distance_threshold = data.get(
            'waypoint_distance_threshold', 0.002)

        if any(param is None for param in [origin_lat, origin_lon, dest_lat, dest_lon]):
            return jsonify({'error': 'Missing required coordinates'}), 400

        # Build command to run generate_car_data.py (it will clear existing data by default)
        cmd = [
            'python', 'generate_car_data.py',
            '--duration', str(duration),
            '--origin', str(origin_lat), str(origin_lon),
            '--destination', str(dest_lat), str(dest_lon),
            '--osrm-url', osrm_url,
            '--movement-mode', movement_mode,
            '--influxdb-url', influxdb_url,
            '--influxdb-token', influxdb_token,
            '--influxdb-org', influxdb_org,
            '--influxdb-bucket', influxdb_bucket,
            '--influxdb-measurement', influxdb_measurement,
            '--influxdb-tag-name', influxdb_tag_name,
            '--influxdb-tag-value', influxdb_tag_value,
            '--waypoint-distance-threshold', str(waypoint_distance_threshold)
        ]

        # Start the script as a subprocess so we can control it
        def run_script():
            global running_process
            try:
                print(f"Starting data generation: {' '.join(cmd)}")
                running_process = subprocess.Popen(cmd)
                running_process.wait()  # Wait for process to complete
                print("Data generation completed")
                running_process = None
            except subprocess.CalledProcessError as e:
                print(f"Data generation failed: {e}")
                running_process = None
            except Exception as e:
                print(f"Error running data generation: {e}")
                running_process = None

        thread = threading.Thread(target=run_script)
        thread.daemon = True
        thread.start()

        return jsonify({
            'message': 'Data generation started (existing data will be cleared)',
            'command': ' '.join(cmd)
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/stop-generation', methods=['POST'])
def stop_generation():
    """Stop the running car data generation script"""
    global running_process

    try:
        if running_process and running_process.poll() is None:
            # Process is still running, terminate it
            running_process.terminate()

            # Wait a bit for graceful termination
            try:
                running_process.wait(timeout=5)
                print("Data generation process terminated gracefully")
            except subprocess.TimeoutExpired:
                # Force kill if it doesn't terminate gracefully
                running_process.kill()
                running_process.wait()
                print("Data generation process force killed")

            running_process = None
            return jsonify({'message': 'Data generation stopped'})
        else:
            return jsonify({'message': 'No data generation process running'})

    except Exception as e:
        print(f"Error stopping data generation: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/pause-car', methods=['POST'])
def pause_car():
    """Pause the car movement (keep streaming current position)"""
    try:
        # Create or update pause signal file
        with open('car_pause_signal.txt', 'w') as f:
            f.write('PAUSED')

        print("Car pause signal sent")
        return jsonify({
            'message': 'Car paused successfully',
            'paused': True
        })

    except Exception as e:
        print(f"Error pausing car: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/resume-car', methods=['POST'])
def resume_car():
    """Resume the car movement"""
    try:
        # Remove pause signal file
        import os
        if os.path.exists('car_pause_signal.txt'):
            os.remove('car_pause_signal.txt')

        print("Car resume signal sent")
        return jsonify({
            'message': 'Car resumed successfully',
            'paused': False
        })

    except Exception as e:
        print(f"Error resuming car: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/generation-status')
def get_generation_status():
    """Check if data generation process is currently running and if car is paused"""
    global running_process

    try:
        is_running = running_process is not None and running_process.poll() is None

        # Check if car is paused and get pause reason
        import os
        is_paused = os.path.exists('car_pause_signal.txt')
        pause_reason = None
        waypoint_name = None

        if is_paused:
            try:
                with open('car_pause_signal.txt', 'r') as f:
                    pause_content = f.read().strip()
                    if pause_content.startswith('WAYPOINT_PAUSE:'):
                        pause_reason = 'waypoint'
                        waypoint_name = pause_content.split(':', 1)[1]
                    else:
                        pause_reason = 'manual'
            except:
                pause_reason = 'manual'

        message = f'Data generation is {
            "running" if is_running else "not running"}'
        if is_paused:
            if pause_reason == 'waypoint':
                message += f' (car paused at waypoint: {waypoint_name})'
            else:
                message += ' (car paused)'

        return jsonify({
            'running': is_running,
            'paused': is_paused,
            'pause_reason': pause_reason,
            'waypoint_name': waypoint_name,
            'message': message
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/update-route', methods=['POST'])
def update_route():
    """Update the car's route with new waypoints while streaming"""
    try:
        data = request.get_json()
        waypoints = data.get('waypoints', [])
        # Use provided OSRM URL or fall back to the one stored in route manager
        osrm_url = data.get(
            'osrm_url') or route_manager.osrm_url or 'http://localhost:5001'

        print(f"🔄 /api/update-route called with {len(waypoints)} waypoints")
        print(f"🔄 Request data: {data}")
        print(f"🔄 Using OSRM URL: {osrm_url}")

        if not waypoints:
            return jsonify({'error': 'No waypoints provided'}), 400

        # Validate waypoints format
        for i, waypoint in enumerate(waypoints):
            print(f"🔄 Waypoint {i+1}: {waypoint}")
            if not isinstance(waypoint, dict) or 'lat' not in waypoint or 'lng' not in waypoint:
                return jsonify({'error': f'Invalid waypoint {i+1}: must have lat and lng keys'}), 400

        # Get current car position from the latest data point
        # Use default InfluxDB configuration if not provided
        influxdb_url = data.get('influxdb_url', 'http://43.201.26.186:8086')
        influxdb_token = data.get(
            'influxdb_token') or 'iYd5PF2P-ezGnT49aeHh5Qmc-_-jdIFFqFLvm5ZMeFvpDMNq9DnNL6xwxSIsqk1dh6LZAX206Nn28GENRNZLHg=='
        influxdb_org = data.get('influxdb_org', 'ciel mobility')
        influxdb_bucket = data.get('influxdb_bucket', 'location')
        influxdb_measurement = data.get('influxdb_measurement', 'locReports')
        influxdb_tag_name = data.get('influxdb_tag_name', 'device_id')
        influxdb_tag_value = data.get('influxdb_tag_value') or data.get(
            'vehicle_id', 'ETRI_VT60_ID01')

        print(f"🔄 Using InfluxDB config: URL={influxdb_url}, Org={influxdb_org}, Bucket={
              influxdb_bucket}, Measurement={influxdb_measurement}, Tag={influxdb_tag_name}={influxdb_tag_value}")
        print(f"🔄 Token provided: {'Yes' if data.get(
            'influxdb_token') else 'No (using default)'}")

        try:
            client = InfluxDBClient(
                url=influxdb_url, token=influxdb_token, org=influxdb_org)
            query_api = client.query_api()

            # Get the latest car position
            query = f'''
            from(bucket: "{influxdb_bucket}")
              |> range(start: -1h)
              |> filter(fn: (r) => r["_measurement"] == "{influxdb_measurement}")
              |> filter(fn: (r) => r["{influxdb_tag_name}"] == "{influxdb_tag_value}")
              |> filter(fn: (r) => r["_field"] == "lat" or r["_field"] == "lng")
              |> last()
              |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            '''

            result = query_api.query(query=query)
            client.close()

            current_lat = None
            current_lon = None

            for table in result:
                for record in table.records:
                    current_lat = record.values.get('lat')
                    current_lon = record.values.get('lng')
                    break

            if current_lat is None or current_lon is None:
                return jsonify({'error': 'No current car position found'}), 404

        except Exception as influx_error:
            error_msg = str(influx_error)
            if "401" in error_msg or "unauthorized" in error_msg.lower():
                return jsonify({'error': 'InfluxDB authentication failed. Please check your token and permissions.'}), 401
            elif "connection" in error_msg.lower():
                return jsonify({'error': 'Cannot connect to InfluxDB server. Please check the URL.'}), 503
            else:
                return jsonify({'error': f'InfluxDB error: {error_msg}'}), 500

        # Update the route in the route manager - this will affect actual car movement
        success = route_manager.update_route_from_current(
            (current_lat, current_lon), waypoints, osrm_url
        )

        # Update the stored OSRM URL in route manager if a new one was provided
        if data.get('osrm_url'):
            route_manager.osrm_url = osrm_url

        if not success:
            return jsonify({'error': 'Failed to update route - check server logs for details'}), 500

        # Get the updated route data for response
        route_points, step_locations, _, _ = route_manager.get_current_route_data()

        # Convert route points to Google Maps format for frontend
        route_points_gm = [[point[0], point[1]] for point in route_points]

        # Calculate approximate duration and distance
        total_distance = sum(step.get('distance', 0)
                             for step in step_locations)
        total_duration = sum(step.get('duration', 0)
                             for step in step_locations)

        return jsonify({
            'route_points': route_points_gm,
            'duration': total_duration,
            'distance': total_distance,
            'current_position': {'lat': current_lat, 'lng': current_lon},
            'waypoints': waypoints,
            'message': 'Route updated and activated immediately - car is now following this path',
            'auto_applied': True,
            'success': True
        })

    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'Error connecting to OSRM server: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/append-route', methods=['POST'])
def append_route():
    """Append new waypoints to the car's current route while streaming"""
    try:
        data = request.get_json()
        new_waypoints = data.get('waypoints', [])
        # Use provided OSRM URL or fall back to the one stored in route manager
        osrm_url = data.get(
            'osrm_url') or route_manager.osrm_url or 'http://localhost:5001'

        print(
            f"➕ /api/append-route called with {len(new_waypoints)} new waypoints")
        print(f"➕ Request data: {data}")
        print(f"➕ Using OSRM URL: {osrm_url}")

        if not new_waypoints:
            return jsonify({'error': 'No waypoints provided'}), 400

        # Validate waypoints format
        for i, waypoint in enumerate(new_waypoints):
            print(f"➕ New waypoint {i+1}: {waypoint}")
            if not isinstance(waypoint, dict) or 'lat' not in waypoint or 'lng' not in waypoint:
                return jsonify({'error': f'Invalid waypoint {i+1}: must have lat and lng keys'}), 400

        # Get current car position from the latest data point
        # Use default InfluxDB configuration if not provided
        influxdb_url = data.get('influxdb_url', 'http://43.201.26.186:8086')
        influxdb_token = data.get(
            'influxdb_token') or 'iYd5PF2P-ezGnT49aeHh5Qmc-_-jdIFFqFLvm5ZMeFvpDMNq9DnNL6xwxSIsqk1dh6LZAX206Nn28GENRNZLHg=='
        influxdb_org = data.get('influxdb_org', 'ciel mobility')
        influxdb_bucket = data.get('influxdb_bucket', 'location')
        influxdb_measurement = data.get('influxdb_measurement', 'locReports')
        influxdb_tag_name = data.get('influxdb_tag_name', 'device_id')
        influxdb_tag_value = data.get('influxdb_tag_value') or data.get(
            'vehicle_id', 'ETRI_VT60_ID01')

        print(f"➕ Using InfluxDB config: URL={influxdb_url}, Org={influxdb_org}, Bucket={
              influxdb_bucket}, Measurement={influxdb_measurement}, Tag={influxdb_tag_name}={influxdb_tag_value}")
        print(f"➕ Token provided: {'Yes' if data.get(
            'influxdb_token') else 'No (using default)'}")

        try:
            client = InfluxDBClient(
                url=influxdb_url, token=influxdb_token, org=influxdb_org)
            query_api = client.query_api()

            # Get the latest car position
            query = f'''
            from(bucket: "{influxdb_bucket}")
              |> range(start: -1h)
              |> filter(fn: (r) => r["_measurement"] == "{influxdb_measurement}")
              |> filter(fn: (r) => r["{influxdb_tag_name}"] == "{influxdb_tag_value}")
              |> filter(fn: (r) => r["_field"] == "lat" or r["_field"] == "lng")
              |> last()
              |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            '''

            result = query_api.query(query=query)
            client.close()

            current_lat = None
            current_lon = None

            for table in result:
                for record in table.records:
                    current_lat = record.values.get('lat')
                    current_lon = record.values.get('lng')
                    break

            if current_lat is None or current_lon is None:
                return jsonify({'error': 'No current car position found'}), 404

        except Exception as influx_error:
            error_msg = str(influx_error)
            if "401" in error_msg or "unauthorized" in error_msg.lower():
                return jsonify({'error': 'InfluxDB authentication failed. Please check your token and permissions.'}), 401
            elif "connection" in error_msg.lower():
                return jsonify({'error': 'Cannot connect to InfluxDB server. Please check the URL.'}), 503
            else:
                return jsonify({'error': f'InfluxDB error: {error_msg}'}), 500

        # Append to the route in the route manager - this will affect actual car movement
        success = route_manager.append_route_from_current(
            (current_lat, current_lon), new_waypoints, osrm_url
        )

        # Update the stored OSRM URL in route manager if a new one was provided
        if data.get('osrm_url'):
            route_manager.osrm_url = osrm_url

        if not success:
            return jsonify({'error': 'Failed to append route - check server logs for details'}), 500

        # Get the updated route data for response
        route_points, step_locations, _, _ = route_manager.get_current_route_data()

        # Convert route points to Google Maps format for frontend
        route_points_gm = [[point[0], point[1]] for point in route_points]

        # Calculate approximate duration and distance
        total_distance = sum(step.get('distance', 0)
                             for step in step_locations)
        total_duration = sum(step.get('duration', 0)
                             for step in step_locations)

        # Load all waypoints to return in response
        all_waypoints = []
        try:
            import json
            if os.path.exists('current_route.json'):
                with open('current_route.json', 'r') as f:
                    route_data = json.load(f)
                    all_waypoints = route_data.get('user_waypoints', [])
        except Exception as e:
            print(f"Warning: Could not load all waypoints for response: {e}")
            all_waypoints = new_waypoints  # Fallback to just the new waypoints

        return jsonify({
            'route_points': route_points_gm,
            'duration': total_duration,
            'distance': total_distance,
            'current_position': {'lat': current_lat, 'lng': current_lon},
            'waypoints': all_waypoints,
            'new_waypoints': new_waypoints,
            'total_waypoints': len(all_waypoints),
            'added_waypoints': len(new_waypoints),
            'message': f'Route extended with {len(new_waypoints)} new waypoints - car will visit all {len(all_waypoints)} waypoints',
            'auto_applied': True,
            'success': True
        })

    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'Error connecting to OSRM server: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/append-route-optimized', methods=['POST'])
def append_route_optimized():
    """Append new waypoints to the car's current route with optimization while maintaining start-end order"""
    try:
        data = request.get_json()
        new_waypoints = data.get('waypoints', [])
        # Use provided OSRM URL or fall back to the one stored in route manager
        osrm_url = data.get(
            'osrm_url') or route_manager.osrm_url or 'http://localhost:5001'

        print(
            f"🚀 /api/append-route-optimized called with {len(new_waypoints)} new waypoints")
        print(f"🚀 Request data: {data}")
        print(f"🚀 Using OSRM URL: {osrm_url}")

        if not new_waypoints:
            return jsonify({'error': 'No waypoints provided'}), 400

        # Validate waypoints format
        for i, waypoint in enumerate(new_waypoints):
            print(f"🚀 New waypoint {i+1}: {waypoint}")
            if not isinstance(waypoint, dict) or 'lat' not in waypoint or 'lng' not in waypoint:
                return jsonify({'error': f'Invalid waypoint {i+1}: must have lat and lng keys'}), 400

        # Get current car position from the latest data point
        # Use default InfluxDB configuration if not provided
        influxdb_url = data.get('influxdb_url', 'http://43.201.26.186:8086')
        influxdb_token = data.get(
            'influxdb_token') or 'iYd5PF2P-ezGnT49aeHh5Qmc-_-jdIFFqFLvm5ZMeFvpDMNq9DnNL6xwxSIsqk1dh6LZAX206Nn28GENRNZLHg=='
        influxdb_org = data.get('influxdb_org', 'ciel mobility')
        influxdb_bucket = data.get('influxdb_bucket', 'location')
        influxdb_measurement = data.get('influxdb_measurement', 'locReports')
        influxdb_tag_name = data.get('influxdb_tag_name', 'device_id')
        influxdb_tag_value = data.get('influxdb_tag_value') or data.get(
            'vehicle_id', 'ETRI_VT60_ID01')

        print(f"🚀 Using InfluxDB config: URL={influxdb_url}, Org={influxdb_org}, Bucket={
              influxdb_bucket}, Measurement={influxdb_measurement}, Tag={influxdb_tag_name}={influxdb_tag_value}")
        print(f"🚀 Token provided: {'Yes' if data.get(
            'influxdb_token') else 'No (using default)'}")

        try:
            client = InfluxDBClient(
                url=influxdb_url, token=influxdb_token, org=influxdb_org)
            query_api = client.query_api()

            # Get the latest car position
            query = f'''
            from(bucket: "{influxdb_bucket}")
              |> range(start: -1h)
              |> filter(fn: (r) => r["_measurement"] == "{influxdb_measurement}")
              |> filter(fn: (r) => r["{influxdb_tag_name}"] == "{influxdb_tag_value}")
              |> filter(fn: (r) => r["_field"] == "lat" or r["_field"] == "lng")
              |> last()
              |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            '''

            result = query_api.query(query=query)
            client.close()

            current_lat = None
            current_lon = None

            for table in result:
                for record in table.records:
                    current_lat = record.values.get('lat')
                    current_lon = record.values.get('lng')
                    break

            if current_lat is None or current_lon is None:
                return jsonify({'error': 'No current car position found'}), 404

        except Exception as influx_error:
            error_msg = str(influx_error)
            if "401" in error_msg or "unauthorized" in error_msg.lower():
                return jsonify({'error': 'InfluxDB authentication failed. Please check your token and permissions.'}), 401
            elif "connection" in error_msg.lower():
                return jsonify({'error': 'Cannot connect to InfluxDB server. Please check the URL.'}), 503
            else:
                return jsonify({'error': f'InfluxDB error: {error_msg}'}), 500

        # Append and optimize the route in the route manager - this will affect actual car movement
        success = route_manager.append_route_optimized_from_current(
            (current_lat, current_lon), new_waypoints, osrm_url
        )

        # Update the stored OSRM URL in route manager if a new one was provided
        if data.get('osrm_url'):
            route_manager.osrm_url = osrm_url

        if not success:
            return jsonify({'error': 'Failed to append and optimize route - check server logs for details'}), 500

        # Get the updated route data for response
        route_points, step_locations, _, _ = route_manager.get_current_route_data()

        # Convert route points to Google Maps format for frontend
        route_points_gm = [[point[0], point[1]] for point in route_points]

        # Calculate approximate duration and distance
        total_distance = sum(step.get('distance', 0)
                             for step in step_locations)
        total_duration = sum(step.get('duration', 0)
                             for step in step_locations)

        # Load all optimized waypoints to return in response
        all_waypoints = []
        try:
            import json
            if os.path.exists('current_route.json'):
                with open('current_route.json', 'r') as f:
                    route_data = json.load(f)
                    all_waypoints = route_data.get('user_waypoints', [])
        except Exception as e:
            print(f"Warning: Could not load all waypoints for response: {e}")
            all_waypoints = new_waypoints  # Fallback to just the new waypoints

        return jsonify({
            'route_points': route_points_gm,
            'duration': total_duration,
            'distance': total_distance,
            'current_position': {'lat': current_lat, 'lng': current_lon},
            'waypoints': all_waypoints,
            'new_waypoints': new_waypoints,
            'total_waypoints': len(all_waypoints),
            'added_waypoints': len(new_waypoints),
            'optimized': True,
            'optimization_method': 'nearest_neighbor_with_fixed_endpoints',
            'message': f'Route optimally extended with {len(new_waypoints)} new waypoints - car will visit all {len(all_waypoints)} waypoints in optimized order',
            'auto_applied': True,
            'success': True
        })

    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'Error connecting to OSRM server: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/append-dispatch-engine', methods=['POST'])
def append_dispatch_engine():
    """Append new waypoints using dispatch engine optimization for pickup/dropoff routes"""
    try:
        data = request.get_json()
        new_demands = data.get('demands', [])
        algorithm = data.get('algorithm', 2)

        print(
            f"🚚 /api/append-dispatch-engine called with {len(new_demands)} new demands")
        print(f"🚚 Request data: {data}")
        print(f"🚚 Using algorithm: {algorithm}")

        if not new_demands:
            return jsonify({'error': 'No demands provided'}), 400

        # Validate demands format
        for i, demand in enumerate(new_demands):
            print(f"🚚 New demand {i+1}: {demand}")
            if not isinstance(demand, dict) or 'lat' not in demand or 'lng' not in demand:
                return jsonify({'error': f'Invalid demand {i+1}: must have lat and lng keys'}), 400

        # Get current car position from the latest data point
        # Use default InfluxDB configuration if not provided
        influxdb_url = data.get('influxdb_url', 'http://43.201.26.186:8086')
        influxdb_token = data.get(
            'influxdb_token') or 'iYd5PF2P-ezGnT49aeHh5Qmc-_-jdIFFqFLvm5ZMeFvpDMNq9DnNL6xwxSIsqk1dh6LZAX206Nn28GENRNZLHg=='
        influxdb_org = data.get('influxdb_org', 'ciel mobility')
        influxdb_bucket = data.get('influxdb_bucket', 'location')
        influxdb_measurement = data.get('influxdb_measurement', 'locReports')
        influxdb_tag_name = data.get('influxdb_tag_name', 'device_id')
        influxdb_tag_value = data.get('influxdb_tag_value') or data.get('vehicle_id', 'ETRI_VT60_ID01')

        print(f"🚚 Using InfluxDB config: URL={influxdb_url}, Org={influxdb_org}, Bucket={
              influxdb_bucket}, Measurement={influxdb_measurement}, Tag={influxdb_tag_name}={influxdb_tag_value}")

        try:
            client = InfluxDBClient(
                url=influxdb_url, token=influxdb_token, org=influxdb_org)
            query_api = client.query_api()

            # Get the latest car position
            query = f'''
            from(bucket: "{influxdb_bucket}")
              |> range(start: -1h)
              |> filter(fn: (r) => r["_measurement"] == "{influxdb_measurement}")
              |> filter(fn: (r) => r["{influxdb_tag_name}"] == "{influxdb_tag_value}")
              |> filter(fn: (r) => r["_field"] == "lat" or r["_field"] == "lng")
              |> last()
              |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            '''

            result = query_api.query(query=query)
            client.close()

            current_lat = None
            current_lon = None

            for table in result:
                for record in table.records:
                    current_lat = record.values.get('lat')
                    current_lon = record.values.get('lng')
                    break

            if current_lat is None or current_lon is None:
                return jsonify({'error': 'No current car position found'}), 404

        except Exception as influx_error:
            error_msg = str(influx_error)
            if "401" in error_msg or "unauthorized" in error_msg.lower():
                return jsonify({'error': 'InfluxDB authentication failed. Please check your token and permissions.'}), 401
            elif "connection" in error_msg.lower():
                return jsonify({'error': 'Cannot connect to InfluxDB server. Please check the URL.'}), 503
            else:
                return jsonify({'error': f'InfluxDB error: {error_msg}'}), 500

        # Get waypoints from request body (these are the current major waypoints)
        request_waypoints = data.get('waypoints', [])
        print(f"🚚 Received {len(request_waypoints)} waypoints from request body")
        
        # Prepare waypoints for dispatch engine: [current_car_location, waypoint1, waypoint2, ...]
        dispatch_waypoints = []

        # 1. Add current car position as first waypoint
        dispatch_waypoints.append({
            "lng": str(current_lon),
            "lat": str(current_lat),
            "metadata": {"type": "current_position"}
        })
        print(f"🚚 Added current car location: ({current_lat}, {current_lon})")

        # 2. Add waypoints from request body
        for i, wp in enumerate(request_waypoints):
            dispatch_waypoints.append({
                "lng": str(wp['lng']),
                "lat": str(wp['lat']),
                "metadata": {"name": wp.get('name', f'Waypoint {i+1}'), "type": "existing_waypoint"}
            })
            print(f"🚚 Added waypoint {i+1}: {wp.get('name', f'Waypoint {i+1}')} at ({wp['lat']}, {wp['lng']})")

        # 3. Add new demands as additional waypoints
        for i, demand in enumerate(new_demands):
            dispatch_waypoints.append({
                "lng": str(demand['lng']),
                "lat": str(demand['lat']),
                "metadata": {"name": f"Demand {i+1}", "type": "new_demand"}
            })
            print(f"🚚 Added demand {i+1}: ({demand['lat']}, {demand['lng']})")

        # For dispatch engine, we send all points as waypoints (no separate demands)
        dispatch_demands = []
        
        print(f"🚚 Total waypoints for dispatch engine: {len(dispatch_waypoints)} (current location + {len(request_waypoints)} waypoints + {len(new_demands)} demands)")

        # Call dispatch engine service
        dispatch_url = "http://13.209.84.184:8765/dispatch-engine-servicei/osrm"
        dispatch_payload = {
            "waypoints": dispatch_waypoints,
            "demands": dispatch_demands,
            "algorithm": algorithm
        }

        print(f"🚚 Calling dispatch engine at: {dispatch_url}")
        print(f"🚚 Payload: {dispatch_payload}")

        try:
            dispatch_response = requests.post(
                dispatch_url,
                json=dispatch_payload,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            
            print(f"🚚 Dispatch engine HTTP status: {dispatch_response.status_code}")
            
            if dispatch_response.status_code != 200:
                error_text = dispatch_response.text
                print(f"❌ Dispatch engine error response: {error_text}")
                return jsonify({'error': f'Dispatch engine returned {dispatch_response.status_code}: {error_text}'}), 500
            
            dispatch_result = dispatch_response.json()
            print(f"🚚 Dispatch engine response: {dispatch_result}")

        except requests.exceptions.RequestException as e:
            print(f"❌ Error calling dispatch engine: {e}")
            return jsonify({'error': f'Error calling dispatch engine service: {str(e)}'}), 500
        except ValueError as e:
            print(f"❌ Error parsing dispatch engine JSON response: {e}")
            return jsonify({'error': f'Invalid JSON response from dispatch engine: {str(e)}'}), 500

        # Extract optimized route from dispatch engine response
        # The response should contain optimized waypoints including pickup/dropoff points
        if dispatch_result.get('code') != 'Ok':
            return jsonify({'error': f'Dispatch engine error: {dispatch_result.get("message", "Unknown error")}'}), 500

        # Convert dispatch engine response to our waypoint format
        optimized_waypoints = []

        # Handle dispatch engine response format
        if 'routes' in dispatch_result and dispatch_result['routes']:
            # Extract waypoints from the optimized route
            route = dispatch_result['routes'][0]
            
            # Get waypoints from the route response
            if 'waypoints' in dispatch_result:
                # Use top-level waypoints if available
                route_waypoints = dispatch_result['waypoints']
            elif 'waypoints' in route:
                # Use route-level waypoints
                route_waypoints = route['waypoints']
            else:
                route_waypoints = []
            
            # Process waypoints, skipping the current position (first waypoint)
            for i, wp in enumerate(route_waypoints):
                if i == 0:
                    continue  # Skip current position
                
                # Handle different waypoint formats
                if 'location' in wp:
                    # Waypoint has location array [lng, lat]
                    lat = float(wp['location'][1])
                    lng = float(wp['location'][0])
                elif 'lat' in wp and 'lng' in wp:
                    # Waypoint has direct lat/lng fields
                    lat = float(wp['lat'])
                    lng = float(wp['lng'])
                else:
                    print(f"⚠️ Skipping waypoint {i} with unknown format: {wp}")
                    continue
                
                # Get waypoint name from metadata or use default
                name = wp.get('metadata', {}).get('name', f'Optimized Point {i}')
                if not name or name == '':
                    name = f'Dispatch Point {i}'
                
                optimized_waypoints.append({
                    'lat': lat,
                    'lng': lng,
                    'name': name
                })
                
                print(f"🚚 Added optimized waypoint {i}: {name} at ({lat:.6f}, {lng:.6f})")
        
        # Fallback: if no route waypoints found, use the demands as waypoints
        if not optimized_waypoints:
            print("🚚 No waypoints found in dispatch response, using demands as fallback")
            for i, demand in enumerate(new_demands):
                optimized_waypoints.append({
                    'lat': float(demand['lat']),
                    'lng': float(demand['lng']),
                    'name': f'Demand Point {i+1}'
                })

        print(f"🚚 Extracted {len(optimized_waypoints)
                             } optimized waypoints from dispatch engine")
        
        # Debug: Print the full dispatch response for troubleshooting
        print(f"🚚 Full dispatch response structure:")
        print(f"   - Code: {dispatch_result.get('code')}")
        print(f"   - Routes count: {len(dispatch_result.get('routes', []))}")
        if 'routes' in dispatch_result and dispatch_result['routes']:
            route = dispatch_result['routes'][0]
            print(f"   - Route distance: {route.get('distance', 'N/A')}")
            print(f"   - Route duration: {route.get('duration', 'N/A')}")
            print(f"   - Route waypoints: {len(route.get('waypoints', []))}")
        print(f"   - Top-level waypoints: {len(dispatch_result.get('waypoints', []))}")

        if not optimized_waypoints:
            print("🚚 No optimized waypoints from dispatch engine, using fallback approach")
            # Fallback: combine existing waypoints + new demands in simple order
            optimized_waypoints = []
            
            # Add existing waypoints from request
            for i, wp in enumerate(request_waypoints):
                optimized_waypoints.append({
                    'lat': float(wp['lat']),
                    'lng': float(wp['lng']),
                    'name': wp.get('name', f'Waypoint {i+1}')
                })
            
            # Add new demands
            for i, demand in enumerate(new_demands):
                optimized_waypoints.append({
                    'lat': float(demand['lat']),
                    'lng': float(demand['lng']),
                    'name': f'Demand {i+1}'
                })
            
            print(f"🚚 Using fallback with {len(optimized_waypoints)} waypoints")
            
            if not optimized_waypoints:
                return jsonify({'error': 'No waypoints to process (dispatch engine failed and no fallback data)'}), 500

        # Use provided OSRM URL or fall back to the one stored in route manager
        osrm_url = data.get(
            'osrm_url') or route_manager.osrm_url or 'http://localhost:5001'

        # Update the route in the route manager with optimized waypoints
        success = route_manager.update_route_from_current(
            (current_lat, current_lon), optimized_waypoints, osrm_url
        )

        # Update the stored OSRM URL in route manager if a new one was provided
        if data.get('osrm_url'):
            route_manager.osrm_url = osrm_url

        if not success:
            return jsonify({'error': 'Failed to update route with dispatch engine optimization - check server logs for details'}), 500

        # Get the updated route data for response
        route_points, step_locations, _, _ = route_manager.get_current_route_data()

        # Convert route points to Google Maps format for frontend
        route_points_gm = [[point[0], point[1]] for point in route_points]

        # Calculate approximate duration and distance
        total_distance = sum(step.get('distance', 0)
                             for step in step_locations)
        total_duration = sum(step.get('duration', 0)
                             for step in step_locations)

        return jsonify({
            'route_points': route_points_gm,
            'duration': total_duration,
            'distance': total_distance,
            'current_position': {'lat': current_lat, 'lng': current_lon},
            'waypoints': optimized_waypoints,
            'original_demands': new_demands,
            'total_waypoints': len(optimized_waypoints),
            'added_demands': len(new_demands),
            'optimized': True,
            'optimization_method': 'dispatch_engine',
            'algorithm': algorithm,
            'dispatch_response': dispatch_result,
            'message': f'Route optimized by dispatch engine with {len(new_demands)} demands - car will follow optimized pickup/dropoff sequence',
            'auto_applied': True,
            'success': True
        })

    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'Error connecting to dispatch engine: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/influxdb-config')
def get_influxdb_config():
    """Get default InfluxDB configuration"""
    try:
        config = {
            'url': 'http://43.201.26.186:8086',
            'org': 'ciel mobility',
            'bucket': 'location',
            'measurement': 'locReports',
            'tag_name': 'device_id',
            'tag_value': 'ETRI_VT60_ID01'
            # Note: Token is not included for security reasons
        }

        return jsonify(config)

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/route-status')
def get_route_status():
    """Get current route status and data for frontend polling"""
    try:
        # Get current route data from route manager
        route_points, step_locations, was_updated, update_timestamp = route_manager.get_current_route_data()

        if not route_points:
            return jsonify({
                'route_updated': False,
                'update_timestamp': 0,
                'route_points': [],
                'waypoints': [],
                'distance': 0,
                'duration': 0,
                'message': 'No route data available'
            })

        # Convert route points to Google Maps format for frontend
        route_points_gm = [[point[0], point[1]] for point in route_points]

        # Calculate approximate duration and distance
        total_distance = sum(step.get('distance', 0)
                             for step in step_locations)
        total_duration = sum(step.get('duration', 0)
                             for step in step_locations)

        # Extract waypoints from step locations (simplified)
        waypoints = []
        # Sample every few steps as waypoints
        for i, step in enumerate(step_locations[::max(1, len(step_locations)//4)]):
            if step.get('name') and step['name'].strip():
                waypoints.append({
                    'lat': step['location'][0],
                    'lng': step['location'][1],
                    'name': step['name']
                })

        return jsonify({
            'route_updated': was_updated,
            'update_timestamp': update_timestamp,
            'route_points': route_points_gm,
            'waypoints': waypoints,
            'distance': total_distance,
            'duration': total_duration,
            'message': 'Route status retrieved successfully'
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/car-data-stream')
def stream_car_data():
    """Stream car data from InfluxDB in real-time"""
    from flask import Response
    import json
    import time

    # Extract request parameters BEFORE entering the generator function
    influxdb_url = request.args.get(
        'influxdb_url', 'http://43.201.26.186:8086')
    influxdb_token = request.args.get('influxdb_token', '')
    influxdb_org = request.args.get('influxdb_org', 'ciel mobility')
    influxdb_bucket = request.args.get('influxdb_bucket', 'location')
    influxdb_measurement = request.args.get(
        'influxdb_measurement', 'locReports')
    influxdb_tag_name = request.args.get('influxdb_tag_name', 'device_id')
    influxdb_tag_value = request.args.get(
        'influxdb_tag_value', 'ETRI_VT60_ID01')

    def generate_data():
        client = None
        try:

            print(f"🔄 Starting car data stream with InfluxDB: {influxdb_url}")
            print(f"🔄 Organization: {influxdb_org}, Bucket: {influxdb_bucket}")

            # Send initial connection message
            yield f"data: {json.dumps({'status': 'connecting', 'message': 'Connecting to InfluxDB...'})}\n\n"

            client = InfluxDBClient(
                url=influxdb_url, token=influxdb_token, org=influxdb_org)
            query_api = client.query_api()

            # Test connection
            health = client.health()
            print(f"✓ InfluxDB health status: {health.status}")
            yield f"data: {json.dumps({'status': 'connected', 'message': 'Connected to InfluxDB'})}\n\n"

            last_timestamp = None

            # Use configurable tag name and value

            while True:
                try:
                    # Query for new data since last timestamp
                    if last_timestamp:
                        # Add microsecond precision to avoid timestamp boundary issues
                        query = f'''
                        from(bucket: "{influxdb_bucket}")
                          |> range(start: -10m)
                          |> filter(fn: (r) => r["_measurement"] == "{influxdb_measurement}")
                          |> filter(fn: (r) => r["{influxdb_tag_name}"] == "{influxdb_tag_value}")
                          |> filter(fn: (r) => r["_time"] > time(v: "{last_timestamp}"))
                          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                          |> sort(columns: ["_time"])
                          |> group()
                          |> unique(column: "_time")
                        '''
                    else:
                        # Get the most recent data point to start streaming
                        query = f'''
                        from(bucket: "{influxdb_bucket}")
                          |> range(start: -10m)
                          |> filter(fn: (r) => r["_measurement"] == "{influxdb_measurement}")
                          |> filter(fn: (r) => r["{influxdb_tag_name}"] == "{influxdb_tag_value}")
                          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                          |> sort(columns: ["_time"])
                          |> group()
                          |> unique(column: "_time")
                          |> tail(n: 1)
                        '''

                    result = query_api.query(query=query)

                    new_points = []
                    processed_timestamps = set()

                    # Process all tables and records
                    for table in result:
                        for record in table.records:
                            timestamp_iso = record.get_time().isoformat()

                            # Skip if we've already processed this exact timestamp in this iteration
                            if timestamp_iso in processed_timestamps:
                                continue

                            # Only process if this timestamp is actually newer than our last one
                            if last_timestamp and timestamp_iso <= last_timestamp:
                                continue

                            processed_timestamps.add(timestamp_iso)

                            # Validate that we have the required fields
                            lat = record.values.get('lat')
                            lng = record.values.get('lng')

                            if lat is None or lng is None:
                                continue  # Skip incomplete records

                            point_data = {
                                'time': timestamp_iso,
                                'lat': lat,
                                'lng': lng,
                                'speed': record.values.get('speed', 0),
                                'angle': record.values.get('angle', 0),
                                'alt': record.values.get('alt', 0),
                                'get_date': record.values.get('get_date', ''),
                                'step_index': record.values.get('step_index', 0),
                                'instruction': record.values.get('instruction', 'moving'),
                                'intermediate_index': record.values.get('intermediate_index', 0),
                                'cycle_count': record.values.get('cycle_count', 0),
                                'step_duration': record.values.get('step_duration', 0),
                                'step_distance': record.values.get('step_distance', 0),
                                'step_name': record.values.get('step_name', ''),
                                'point_sequence': record.values.get('point_sequence', 0)
                            }
                            new_points.append(point_data)
                            last_timestamp = timestamp_iso

                    if new_points:
                        # Send new data points
                        for point in new_points:
                            yield f"data: {json.dumps(point)}\n\n"
                        print(f"📡 Streamed {len(new_points)} new data points")
                    else:
                        # Send heartbeat less frequently to reduce noise
                        yield f"data: {json.dumps({'heartbeat': True, 'timestamp': time.time()})}\n\n"

                    time.sleep(1)  # Check for new data every second

                except Exception as e:
                    error_msg = str(e)
                    print(f"❌ Error in streaming loop: {e}")

                    # Handle specific InfluxDB errors
                    if "no column" in error_msg and "_value" in error_msg:
                        # This is expected when bucket is empty - just wait for data
                        yield f"data: {json.dumps({'status': 'waiting', 'message': 'Waiting for data to be generated...', 'timestamp': time.time()})}\n\n"
                        # Wait shorter time when waiting for data
                        time.sleep(2)
                    elif "unauthorized" in error_msg.lower():
                        yield f"data: {json.dumps({'error': 'InfluxDB authentication failed', 'details': 'Check your token and permissions'})}\n\n"
                        time.sleep(5)  # Wait longer on auth error
                    else:
                        yield f"data: {json.dumps({'error': f'Streaming error: {error_msg}'})}\n\n"
                        time.sleep(3)  # Wait moderate time on other errors

        except Exception as e:
            print(f"❌ Error in stream setup: {e}")
            yield f"data: {json.dumps({'error': f'Stream setup error: {str(e)}'})}\n\n"
        finally:
            if client:
                try:
                    client.close()
                    print("🔌 InfluxDB client closed")
                except:
                    pass

    return Response(generate_data(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache',
                             'Connection': 'keep-alive',
                             'Access-Control-Allow-Origin': '*',
                             'Access-Control-Allow-Headers': 'Cache-Control'})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
