import threading
import time
import requests
import math
from typing import List, Dict, Optional, Tuple

class DynamicRouteManager:
    """Manages dynamic route updates for car movement simulation"""
    
    def __init__(self):
        self.lock = threading.Lock()
        self.current_route_points = []
        self.current_step_locations = []
        self.route_updated = False
        self.route_update_timestamp = 0
        self.osrm_url = "http://localhost:5001"
        self.movement_mode = "one-way"
        
    def set_initial_route(self, route_points: List, step_locations: List, osrm_url: str, movement_mode: str):
        """Set the initial route for the car"""
        with self.lock:
            self.current_route_points = route_points.copy()
            self.current_step_locations = step_locations.copy()
            self.osrm_url = osrm_url
            self.movement_mode = movement_mode
            self.route_updated = False
            self.route_update_timestamp = time.time()
            print(f"✓ Initial route set with {len(route_points)} points")
    
    def update_route_from_current(self, current_position: Tuple[float, float], waypoints: List[Dict], osrm_url: str) -> bool:
        """Update the route from current position through waypoints"""
        try:
            # Get new route from OSRM
            new_route_points, new_step_locations = self._get_route_from_osrm_with_waypoints(
                current_position, waypoints, osrm_url
            )
            
            if not new_route_points or not new_step_locations:
                print("Failed to get new route from OSRM")
                return False
            
            with self.lock:
                self.current_route_points = new_route_points
                self.current_step_locations = new_step_locations
                self.route_updated = True
                self.route_update_timestamp = time.time()
                print(f"✓ Route updated with {len(new_route_points)} points from current position at {time.strftime('%H:%M:%S')}")
                print(f"✓ Route update flag set to True, timestamp: {self.route_update_timestamp}")
                
            return True
            
        except Exception as e:
            print(f"Error updating route: {e}")
            return False
    
    def get_current_route_data(self, reset_update_flag: bool = False) -> Tuple[List, List, bool, float]:
        """Get current route data and optionally reset the update flag"""
        with self.lock:
            route_points = self.current_route_points.copy()
            step_locations = self.current_step_locations.copy()
            was_updated = self.route_updated
            update_timestamp = self.route_update_timestamp
            
            if reset_update_flag:
                self.route_updated = False
                
            return route_points, step_locations, was_updated, update_timestamp
    
    def _get_route_from_osrm_with_waypoints(self, current_position: Tuple[float, float], 
                                          waypoints: List[Dict], osrm_url: str) -> Tuple[List, List]:
        """Get route from OSRM with current position and waypoints"""
        try:
            # Build coordinate string for OSRM (longitude,latitude)
            coordinates = [f"{current_position[1]},{current_position[0]}"]  # Start from current position
            
            for waypoint in waypoints:
                coordinates.append(f"{waypoint['lng']},{waypoint['lat']}")
            
            coordinate_string = ";".join(coordinates)
            
            # OSRM route API endpoint with multiple waypoints
            url = f"{osrm_url}/route/v1/driving/{coordinate_string}"
            params = {
                'overview': 'full',
                'geometries': 'geojson',
                'steps': 'true'
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data['code'] != 'Ok':
                raise Exception(f"OSRM error: {data.get('message', 'Unknown error')}")
            
            route = data['routes'][0]
            
            # Extract route geometry (coordinates are in [longitude, latitude] format)
            coordinates = route['geometry']['coordinates']
            route_points = [(coord[1], coord[0]) for coord in coordinates]  # Convert to [lat, lon]
            
            # Extract step-by-step locations
            step_locations = []
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
            
            print(f"✓ New route calculated: {len(route_points)} points, {len(step_locations)} steps")
            return route_points, step_locations
            
        except Exception as e:
            print(f"Error getting route from OSRM: {e}")
            return [], []

# Global instance for route management
route_manager = DynamicRouteManager()
