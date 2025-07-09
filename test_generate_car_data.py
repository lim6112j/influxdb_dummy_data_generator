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
    with patch('requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'code': 'Ok',
            'routes': [{
                'geometry': {'coordinates': [[0, 0], [1, 1]]},
                'duration': 3600,
                'distance': 10000,
                'legs': [{
                    'steps': [{
                        'maneuver': {
                            'location': [0, 0],
                            'type': 'depart'
                        },
                        'duration': 1800,
                        'distance': 5000,
                        'name': 'Test Street'
                    }, {
                        'maneuver': {
                            'location': [1, 1],
                            'type': 'arrive'
                        },
                        'duration': 1800,
                        'distance': 5000,
                        'name': 'Destination'
                    }]
                }]
            }]
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        route_points, duration, distance, step_locations = get_route_from_osrm([0, 0], [1, 1], 'http://localhost:5000')
        
        assert route_points is not None
        assert duration == 3600
        assert distance == 10000
        assert len(step_locations) == 2


def test_get_route_from_osrm_failure():
    """Test OSRM route retrieval failure"""
    with patch('requests.get') as mock_get:
        mock_get.side_effect = Exception("Connection error")
        
        route_points, duration, distance, step_locations = get_route_from_osrm([0, 0], [1, 1], 'http://localhost:5000')
        
        assert route_points is None
        assert duration is None
        assert distance is None
        assert step_locations is None


def test_generate_intermediate_points():
    """Test generation of intermediate points between two locations"""
    start_location = [0.0, 0.0]
    end_location = [1.0, 1.0]
    duration = 60  # 1 minute
    speed_kmh = 60  # 60 km/h
    
    points = generate_intermediate_points(start_location, end_location, duration, speed_kmh)
    
    assert len(points) > 0
    assert all(isinstance(point, dict) for point in points)
    assert all('latitude' in point and 'longitude' in point and 'timestamp' in point for point in points)
    
    # First point should be close to start location
    first_point = points[0]
    assert abs(first_point['latitude'] - start_location[0]) < 0.1
    assert abs(first_point['longitude'] - start_location[1]) < 0.1
    
    # Last point should be close to end location
    last_point = points[-1]
    assert abs(last_point['latitude'] - end_location[0]) < 0.1
    assert abs(last_point['longitude'] - end_location[1]) < 0.1


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
         patch('generate_car_data.clear_existing_car_data') as mock_clear, \
         patch('generate_car_data.InfluxDBClient') as mock_client, \
         patch('generate_car_data.time.sleep') as mock_sleep, \
         patch('generate_car_data.time.time') as mock_time:
        
        # Mock time to control the loop
        mock_time.side_effect = [1000, 1001, 1002, 1003]  # start_time, then increments
        mock_sleep.side_effect = [None, None, StopIteration("Test complete")]  # Stop after 3 iterations
        
        # Mock route response - return tuple as expected by generate_car_data
        mock_route.return_value = (
            [[0.0, 0.0], [0.5, 0.5], [1.0, 1.0]],  # route_points
            3600,  # route_duration
            10000,  # route_distance
            [{'location': [0.0, 0.0], 'speed_kmh': 30, 'instruction': 'depart'}]  # step_locations
        )
        
        # Mock InfluxDB client
        mock_write_api = MagicMock()
        mock_client.return_value.write_api.return_value = mock_write_api
        mock_client.return_value.health.return_value.status = 'pass'
        mock_client.return_value.buckets_api.return_value.find_bucket_by_name.return_value.name = 'test_bucket'
        
        # Run the function and expect it to be stopped by our mock
        try:
            generate_car_data(
                duration=1,  # 1 hour but will be stopped by mock
                origin=[0, 0],
                destination=[1, 1],
                osrm_url='http://localhost:5000',
                movement_mode='one-way'
            )
        except StopIteration:
            pass  # Expected from our mock
        
        # Verify calls
        mock_route.assert_called_once()
        mock_clear.assert_called_once()
        assert mock_write_api.write.call_count >= 1


def test_generate_car_data_round_trip():
    """Test car data generation in round-trip mode"""
    with patch('generate_car_data.get_route_from_osrm') as mock_route, \
         patch('generate_car_data.clear_existing_car_data') as mock_clear, \
         patch('generate_car_data.InfluxDBClient') as mock_client, \
         patch('generate_car_data.time.sleep') as mock_sleep, \
         patch('generate_car_data.time.time') as mock_time:
        
        # Mock time to control the loop
        mock_time.side_effect = [1000, 1001, 1002, 1003]  # start_time, then increments
        mock_sleep.side_effect = [None, None, StopIteration("Test complete")]  # Stop after 3 iterations
        
        # Mock route response - return tuple as expected by generate_car_data
        mock_route.return_value = (
            [[0.0, 0.0], [1.0, 1.0]],  # route_points
            1800,  # route_duration (30 minutes)
            5000,  # route_distance
            [{'location': [0.0, 0.0], 'speed_kmh': 30, 'instruction': 'depart'}]  # step_locations
        )
        
        # Mock InfluxDB client
        mock_write_api = MagicMock()
        mock_client.return_value.write_api.return_value = mock_write_api
        mock_client.return_value.health.return_value.status = 'pass'
        mock_client.return_value.buckets_api.return_value.find_bucket_by_name.return_value.name = 'test_bucket'
        
        # Run the function and expect it to be stopped by our mock
        try:
            generate_car_data(
                duration=1,  # 1 hour but will be stopped by mock
                origin=[0, 0],
                destination=[1, 1],
                osrm_url='http://localhost:5000',
                movement_mode='round-trip'
            )
        except StopIteration:
            pass  # Expected from our mock
        
        # Verify route was called twice (there and back)
        assert mock_route.call_count == 2


def test_generate_car_data_no_route():
    """Test car data generation when no route is found"""
    with patch('generate_car_data.get_route_from_osrm') as mock_route, \
         patch('generate_car_data.clear_existing_car_data') as mock_clear, \
         patch('generate_car_data.InfluxDBClient') as mock_client:
        
        # Mock InfluxDB client setup
        mock_client.return_value.health.return_value.status = 'pass'
        mock_client.return_value.buckets_api.return_value.find_bucket_by_name.return_value.name = 'test_bucket'
        
        # Mock no route found
        mock_route.return_value = (None, None, None, None)
        
        # This should handle the error gracefully
        try:
            generate_car_data(
                duration=1,
                origin=[0, 0],
                destination=[1, 1],
                osrm_url='http://localhost:5000'
            )
        except Exception as e:
            # Should not raise an unhandled exception
            pytest.fail(f"Function should handle no route gracefully, but raised: {e}")
