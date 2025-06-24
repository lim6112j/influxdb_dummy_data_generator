from flask import Flask, render_template, jsonify
import os
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient

app = Flask(__name__)

# Load environment variables
load_dotenv()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/car-data')
def get_car_data():
    """Fetch car data from InfluxDB and return as JSON"""
    
    influxdb_url = os.getenv('INFLUXDB_URL')
    influxdb_token = os.getenv('INFLUXDB_TOKEN')
    influxdb_org = os.getenv('INFLUXDB_ORG')
    influxdb_bucket = os.getenv('INFLUXDB_BUCKET')
    
    client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)
    query_api = client.query_api()
    
    # Query to get the last 1000 car data points
    query = f'''
    from(bucket: "{influxdb_bucket}")
      |> range(start: -24h)
      |> filter(fn: (r) => r["_measurement"] == "car_data")
      |> filter(fn: (r) => r["car_id"] == "1")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"])
      |> limit(n: 1000)
    '''
    
    try:
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
        
        client.close()
        return jsonify(car_data)
        
    except Exception as e:
        client.close()
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
