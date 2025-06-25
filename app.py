from flask import Flask, render_template, jsonify
import os
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

        # Query to get the last 1000 car data points
        query = f'''
        from(bucket: "{influxdb_bucket}")
          |> range(start: -1m)
          |> filter(fn: (r) => r["_measurement"] == "car_data")
          |> filter(fn: (r) => r["car_id"] == "1")
          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
          |> sort(columns: ["_time"])
          |> limit(n: 100)
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
                    'heading': record.values.get('heading')
                })

        print(f"Found {len(car_data)} data points")
        client.close()
        return jsonify(car_data)

    except Exception as e:
        print(f"Error in get_car_data: {str(e)}")
        if 'client' in locals():
            client.close()
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
