import pytest
from unittest.mock import patch, MagicMock
from generate_car_data import (
    get_route_from_osrm,
    generate_intermediate_points,
    clear_existing_car_data,
    generate_car_data
)


def test_get_route_from_osrm_success():
    """Test successful OSRM route retrieval"""
    with patch('generate_car_data.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'routes': [{
                'geometry': 'test_geometry',
                'duration': 3600,
                'distance': 10000
            }]
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = get_route_from_osrm([0, 0], [1, 1], 'http://localhost:5000')
        
        assert result is not None
        assert 'geometry' in result
        assert result['geometry'] == 'test_geometry'


def test_get_route_from_osrm_failure():
    """Test OSRM route retrieval failure"""
    with patch('generate_car_data.requests.get') as mock_get:
        mock_get.side_effect = Exception("Connection error")
        
        result = get_route_from_osrm([0, 0], [1, 1], 'http://localhost:5000')
        
        assert result is None


def test_generate_intermediate_points():
    """Test generation of intermediate points between two locations"""
    start_location = [0.0, 0.0]
    end_location = [1.0, 1.0]
    duration = 60  # 1 minute
    speed_kmh = 60  # 60 km/h
    
    points = generate_intermediate_points(start_location, end_location, duration, speed_kmh)
    
    assert len(points) > 0
    assert all(isinstance(point, dict) for point in points)
    assert all('lat' in point and 'lon' in point and 'timestamp' in point for point in points)
    
    # First point should be close to start location
    first_point = points[0]
    assert abs(first_point['lat'] - start_location[0]) < 0.1
    assert abs(first_point['lon'] - start_location[1]) < 0.1
    
    # Last point should be close to end location
    last_point = points[-1]
    assert abs(last_point['lat'] - end_location[0]) < 0.1
    assert abs(last_point['lon'] - end_location[1]) < 0.1


def test_generate_intermediate_points_zero_duration():
    """Test intermediate points generation with zero duration"""
    start_location = [0.0, 0.0]
    end_location = [1.0, 1.0]
    duration = 0
    speed_kmh = 60
    
    points = generate_intermediate_points(start_location, end_location, duration, speed_kmh)
    
    # Should return at least the start point
    assert len(points) >= 1


def test_clear_existing_car_data():
    """Test clearing existing car data from InfluxDB"""
    mock_client = MagicMock()
    mock_delete_api = MagicMock()
    mock_client.delete_api.return_value = mock_delete_api
    
    clear_existing_car_data(mock_client, 'test_bucket', 'test_org')
    
    mock_client.delete_api.assert_called_once()
    mock_delete_api.delete.assert_called_once()


def test_generate_car_data_one_way():
    """Test car data generation in one-way mode"""
    with patch('generate_car_data.get_route_from_osrm') as mock_route, \
         patch('generate_car_data.generate_intermediate_points') as mock_points, \
         patch('generate_car_data.clear_existing_car_data') as mock_clear, \
         patch('generate_car_data.InfluxDBClient') as mock_client:
        
        # Mock route response
        mock_route.return_value = {
            'geometry': 'test_geometry',
            'duration': 3600,
            'distance': 10000
        }
        
        # Mock intermediate points
        mock_points.return_value = [
            {'lat': 0.0, 'lon': 0.0, 'timestamp': '2023-01-01T00:00:00Z'},
            {'lat': 0.5, 'lon': 0.5, 'timestamp': '2023-01-01T00:30:00Z'},
            {'lat': 1.0, 'lon': 1.0, 'timestamp': '2023-01-01T01:00:00Z'}
        ]
        
        # Mock InfluxDB client
        mock_write_api = MagicMock()
        mock_client.return_value.write_api.return_value = mock_write_api
        
        # Run the function
        generate_car_data(
            duration=60,
            origin=[0, 0],
            destination=[1, 1],
            osrm_url='http://localhost:5000',
            movement_mode='one-way'
        )
        
        # Verify calls
        mock_route.assert_called_once()
        mock_points.assert_called_once()
        mock_clear.assert_called_once()
        mock_write_api.write.assert_called()


def test_generate_car_data_round_trip():
    """Test car data generation in round-trip mode"""
    with patch('generate_car_data.get_route_from_osrm') as mock_route, \
         patch('generate_car_data.generate_intermediate_points') as mock_points, \
         patch('generate_car_data.clear_existing_car_data') as mock_clear, \
         patch('generate_car_data.InfluxDBClient') as mock_client:
        
        # Mock route response
        mock_route.return_value = {
            'geometry': 'test_geometry',
            'duration': 1800,  # 30 minutes
            'distance': 5000
        }
        
        # Mock intermediate points
        mock_points.return_value = [
            {'lat': 0.0, 'lon': 0.0, 'timestamp': '2023-01-01T00:00:00Z'},
            {'lat': 1.0, 'lon': 1.0, 'timestamp': '2023-01-01T00:30:00Z'}
        ]
        
        # Mock InfluxDB client
        mock_write_api = MagicMock()
        mock_client.return_value.write_api.return_value = mock_write_api
        
        # Run the function
        generate_car_data(
            duration=60,
            origin=[0, 0],
            destination=[1, 1],
            osrm_url='http://localhost:5000',
            movement_mode='round-trip'
        )
        
        # Verify route was called twice (there and back)
        assert mock_route.call_count == 2
        assert mock_points.call_count == 2


def test_generate_car_data_no_route():
    """Test car data generation when no route is found"""
    with patch('generate_car_data.get_route_from_osrm') as mock_route, \
         patch('generate_car_data.clear_existing_car_data') as mock_clear:
        
        # Mock no route found
        mock_route.return_value = None
        
        # This should handle the error gracefully
        try:
            generate_car_data(
                duration=60,
                origin=[0, 0],
                destination=[1, 1],
                osrm_url='http://localhost:5000'
            )
        except Exception as e:
            # Should not raise an unhandled exception
            pytest.fail(f"Function should handle no route gracefully, but raised: {e}")
