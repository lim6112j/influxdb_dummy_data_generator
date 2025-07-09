import pytest
import json
from unittest.mock import patch, MagicMock
from app import app


@pytest.fixture
def client():
    """Create a test client for the Flask app"""
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


def test_index_route(client):
    """Test the index route returns the HTML template"""
    response = client.get('/')
    assert response.status_code == 200
    assert b'html' in response.data


def test_get_car_data_route(client):
    """Test the car data API endpoint"""
    with patch('app.InfluxDBClient') as mock_client:
        mock_query_api = MagicMock()
        mock_client.return_value.query_api.return_value = mock_query_api
        mock_query_api.query.return_value = []
        
        response = client.get('/api/car-data')
        assert response.status_code == 200
        assert response.content_type == 'application/json'


def test_get_route_with_valid_params(client):
    """Test the route API with valid parameters"""
    with patch('requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'code': 'Ok',
            'routes': [{
                'geometry': {'coordinates': [[0, 0], [1, 1]]},
                'duration': 3600,
                'distance': 10000
            }]
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        response = client.get('/api/route?origin_lat=0&origin_lon=0&dest_lat=1&dest_lon=1')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert 'route_points' in data
        assert 'duration' in data
        assert 'distance' in data


def test_get_route_missing_params(client):
    """Test the route API with missing parameters"""
    response = client.get('/api/route')
    assert response.status_code == 400
    data = json.loads(response.data)
    assert 'error' in data


def test_check_osrm_status(client):
    """Test OSRM status check"""
    with patch('app.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'code': 'Ok'}
        mock_response.elapsed.total_seconds.return_value = 0.1
        mock_get.return_value = mock_response
        
        response = client.get('/api/status/osrm')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['status'] == 'online'


def test_check_influxdb_status(client):
    """Test InfluxDB status check"""
    with patch('app.InfluxDBClient') as mock_client, \
         patch('app.os.getenv') as mock_getenv:
        
        # Mock environment variables
        mock_getenv.side_effect = lambda key: {
            'INFLUXDB_URL': 'http://localhost:8086',
            'INFLUXDB_TOKEN': 'test_token',
            'INFLUXDB_ORG': 'test_org',
            'INFLUXDB_BUCKET': 'test_bucket'
        }.get(key)
        
        # Mock InfluxDB client and query
        mock_query_api = MagicMock()
        mock_client.return_value.query_api.return_value = mock_query_api
        mock_query_api.query.return_value = [MagicMock(records=[MagicMock()])]
        
        response = client.get('/api/status/influxdb')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['status'] == 'online'


def test_start_generation_valid_data(client):
    """Test starting car data generation with valid data"""
    test_data = {
        'duration': 60,
        'origin_lat': 0,
        'origin_lon': 0,
        'dest_lat': 1,
        'dest_lon': 1,
        'movement_mode': 'one-way'
    }
    
    with patch('app.threading.Thread') as mock_thread, \
         patch('app.subprocess.Popen') as mock_popen:
        
        mock_process = MagicMock()
        mock_popen.return_value = mock_process
        
        response = client.post('/api/start-generation', 
                             data=json.dumps(test_data),
                             content_type='application/json')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert 'message' in data


def test_start_generation_invalid_data(client):
    """Test starting car data generation with invalid data"""
    test_data = {'invalid': 'data'}
    
    response = client.post('/api/start-generation', 
                         data=json.dumps(test_data),
                         content_type='application/json')
    assert response.status_code == 400


def test_stop_generation(client):
    """Test stopping car data generation"""
    response = client.post('/api/stop-generation')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert 'message' in data


def test_stream_car_data(client):
    """Test car data streaming endpoint"""
    with patch('app.InfluxDBClient') as mock_client, \
         patch('app.os.getenv') as mock_getenv:
        
        # Mock environment variables
        mock_getenv.side_effect = lambda key: {
            'INFLUXDB_URL': 'http://localhost:8086',
            'INFLUXDB_TOKEN': 'test_token',
            'INFLUXDB_ORG': 'test_org',
            'INFLUXDB_BUCKET': 'test_bucket'
        }.get(key)
        
        # Mock InfluxDB client and query
        mock_query_api = MagicMock()
        mock_client.return_value.query_api.return_value = mock_query_api
        mock_query_api.query.return_value = []
        
        # Get the response but don't try to read the stream
        response = client.get('/api/car-data-stream')
        assert response.status_code == 200
        assert 'text/event-stream' in response.content_type
