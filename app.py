from flask import Flask, render_template, jsonify, request
import os
import requests
import time
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient

app = Flask(__name__)

# Load environment variables
load_dotenv()


@app.route('/')
def index():
    google_maps_api_key = os.getenv('GOOGLE_MAPS_API_KEY', 'YOUR_API_KEY')
    return render_template('index.html', google_maps_api_key=google_maps_api_key)


@app.route('/api/car-data')
def get_car_data():
    """Fetch car data from InfluxDB and return as JSON"""

    try:
        # Load environment variables
        influxdb_url = os.getenv('INFLUXDB_URL')
        influxdb_token = os.getenv('INFLUXDB_TOKEN')
        influxdb_org = os.getenv('INFLUXDB_ORG')
        influxdb_bucket = os.getenv('INFLUXDB_BUCKET')

        if not all([influxdb_url, influxdb_token, influxdb_org, influxdb_bucket]):
            return jsonify({'error': 'Missing InfluxDB configuration'}), 500

        print(f"Connecting to InfluxDB: {influxdb_url}")
        print(f"Organization: {influxdb_org}, Bucket: {influxdb_bucket}")

        client = InfluxDBClient(
            url=influxdb_url, token=influxdb_token, org=influxdb_org)
        query_api = client.query_api()

        # Query to get the last 500 car data points with step information
        query = f'''
        from(bucket: "{influxdb_bucket}")
          |> range(start: -10m)
          |> filter(fn: (r) => r["_measurement"] == "car_data")
          |> filter(fn: (r) => r["car_id"] == "1")
          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
          |> sort(columns: ["_time"])
          |> limit(n: 500)
        '''

        print(f"Executing query...")
        result = query_api.query(query=query)

        car_data = []
        for table in result:
            for record in table.records:
                car_data.append({
                    'time': record.get_time().isoformat(),
                    'latitude': record.values.get('latitude'),
                    'longitude': record.values.get('longitude'),
                    'speed': record.values.get('speed'),
                    'heading': record.values.get('heading'),
                    'step_index': record.values.get('step_index'),
                    'instruction': record.values.get('instruction'),
                    'intermediate_index': record.values.get('intermediate_index'),
                    'cycle_count': record.values.get('cycle_count'),
                    'step_duration': record.values.get('step_duration'),
                    'step_distance': record.values.get('step_distance'),
                    'step_name': record.values.get('step_name')
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
        
        if not all([origin_lat, origin_lon, dest_lat, dest_lon]):
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
        # Load environment variables
        influxdb_url = os.getenv('INFLUXDB_URL')
        influxdb_token = os.getenv('INFLUXDB_TOKEN')
        influxdb_org = os.getenv('INFLUXDB_ORG')
        influxdb_bucket = os.getenv('INFLUXDB_BUCKET')

        if not all([influxdb_url, influxdb_token, influxdb_org, influxdb_bucket]):
            missing = []
            if not influxdb_url: missing.append('INFLUXDB_URL')
            if not influxdb_token: missing.append('INFLUXDB_TOKEN')
            if not influxdb_org: missing.append('INFLUXDB_ORG')
            if not influxdb_bucket: missing.append('INFLUXDB_BUCKET')
            
            return jsonify({
                'status': 'misconfigured',
                'message': f"Missing environment variables: {', '.join(missing)}",
                'url': influxdb_url or 'Not configured'
            })

        # Try to connect to InfluxDB
        client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)
        
        # Test connection with a simple query
        query_api = client.query_api()
        test_query = f'buckets() |> filter(fn: (r) => r.name == "{influxdb_bucket}") |> limit(n: 1)'
        
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
    import subprocess
    import threading
    
    try:
        data = request.get_json()
        origin_lat = data.get('origin_lat')
        origin_lon = data.get('origin_lon')
        dest_lat = data.get('dest_lat')
        dest_lon = data.get('dest_lon')
        duration = data.get('duration', 1)  # Default 1 hour
        osrm_url = data.get('osrm_url', 'http://localhost:5001')
        movement_mode = data.get('movement_mode', 'one-way')  # Default one-way
        
        if not all([origin_lat, origin_lon, dest_lat, dest_lon]):
            return jsonify({'error': 'Missing required coordinates'}), 400
        
        # Build command to run generate_car_data.py
        cmd = [
            'python', 'generate_car_data.py',
            '--duration', str(duration),
            '--origin', str(origin_lat), str(origin_lon),
            '--destination', str(dest_lat), str(dest_lon),
            '--osrm-url', osrm_url,
            '--movement-mode', movement_mode
        ]
        
        # Start the script in a separate thread so it doesn't block the web server
        def run_script():
            try:
                print(f"Starting data generation: {' '.join(cmd)}")
                subprocess.run(cmd, check=True)
                print("Data generation completed")
            except subprocess.CalledProcessError as e:
                print(f"Data generation failed: {e}")
            except Exception as e:
                print(f"Error running data generation: {e}")
        
        thread = threading.Thread(target=run_script)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'message': 'Data generation started',
            'command': ' '.join(cmd)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/car-data-stream')
def stream_car_data():
    """Stream car data from InfluxDB in real-time"""
    from flask import Response
    import json
    import time
    
    def generate_data():
        try:
            # Load environment variables
            influxdb_url = os.getenv('INFLUXDB_URL')
            influxdb_token = os.getenv('INFLUXDB_TOKEN')
            influxdb_org = os.getenv('INFLUXDB_ORG')
            influxdb_bucket = os.getenv('INFLUXDB_BUCKET')

            if not all([influxdb_url, influxdb_token, influxdb_org, influxdb_bucket]):
                yield f"data: {json.dumps({'error': 'Missing InfluxDB configuration'})}\n\n"
                return

            client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)
            query_api = client.query_api()
            
            last_timestamp = None
            
            while True:
                try:
                    # Query for new data since last timestamp
                    if last_timestamp:
                        query = f'''
                        from(bucket: "{influxdb_bucket}")
                          |> range(start: -10m)
                          |> filter(fn: (r) => r["_measurement"] == "car_data")
                          |> filter(fn: (r) => r["car_id"] == "1")
                          |> filter(fn: (r) => r["_time"] > time(v: "{last_timestamp}"))
                          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                          |> sort(columns: ["_time"])
                        '''
                    else:
                        query = f'''
                        from(bucket: "{influxdb_bucket}")
                          |> range(start: -1m)
                          |> filter(fn: (r) => r["_measurement"] == "car_data")
                          |> filter(fn: (r) => r["car_id"] == "1")
                          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                          |> sort(columns: ["_time"])
                        '''

                    result = query_api.query(query=query)
                    
                    new_points = []
                    for table in result:
                        for record in table.records:
                            point_data = {
                                'time': record.get_time().isoformat(),
                                'latitude': record.values.get('latitude'),
                                'longitude': record.values.get('longitude'),
                                'speed': record.values.get('speed'),
                                'heading': record.values.get('heading'),
                                'step_index': record.values.get('step_index'),
                                'instruction': record.values.get('instruction'),
                                'intermediate_index': record.values.get('intermediate_index'),
                                'cycle_count': record.values.get('cycle_count'),
                                'step_duration': record.values.get('step_duration'),
                                'step_distance': record.values.get('step_distance'),
                                'step_name': record.values.get('step_name')
                            }
                            new_points.append(point_data)
                            last_timestamp = record.get_time().isoformat()
                    
                    if new_points:
                        # Send new data points
                        for point in new_points:
                            yield f"data: {json.dumps(point)}\n\n"
                    
                    time.sleep(1)  # Check for new data every second
                    
                except Exception as e:
                    print(f"Error in streaming: {e}")
                    yield f"data: {json.dumps({'error': str(e)})}\n\n"
                    time.sleep(5)  # Wait longer on error
                    
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
    
    return Response(generate_data(), mimetype='text/event-stream',
                   headers={'Cache-Control': 'no-cache',
                           'Connection': 'keep-alive',
                           'Access-Control-Allow-Origin': '*'})


if __name__ == '__main__':
    app.run(debug=True, port=8080)
