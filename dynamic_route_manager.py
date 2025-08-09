import threading
import time
import requests
import math
import json
import os
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
        self.route_file = "current_route.json"
        self.last_file_check = 0
        
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
            
            # Save initial route to file
            self._save_route_to_file()
    
    def update_route_from_current(self, current_position: Tuple[float, float], waypoints: List[Dict], osrm_url: str) -> bool:
        """Update the route from current position through waypoints"""
        try:
            print(f"🔄 Starting route update from position {current_position} with {len(waypoints)} waypoints")
            
            # Get new route from OSRM
            new_route_points, new_step_locations = self._get_route_from_osrm_with_waypoints(
                current_position, waypoints, osrm_url
            )
            
            if not new_route_points or not new_step_locations:
                print("❌ Failed to get new route from OSRM")
                return False
            
            with self.lock:
                old_points_count = len(self.current_route_points)
                self.current_route_points = new_route_points
                self.current_step_locations = new_step_locations
                self.route_updated = True
                self.route_update_timestamp = time.time()
                
                print(f"✅ ROUTE UPDATED SUCCESSFULLY!")
                print(f"   - Old route: {old_points_count} points")
                print(f"   - New route: {len(new_route_points)} points")
                print(f"   - Update flag: {self.route_updated}")
                print(f"   - Timestamp: {self.route_update_timestamp}")
                print(f"   - Time: {time.strftime('%H:%M:%S')}")
                
                # Save updated route to file for subprocess communication
                self._save_route_to_file()
                
            return True
            
        except Exception as e:
            print(f"❌ Error updating route: {e}")
            return False
    
    def get_current_route_data(self, reset_update_flag: bool = False) -> Tuple[List, List, bool, float]:
        """Get current route data and optionally reset the update flag"""
        # Check for file updates first (for subprocess communication)
        self._check_route_file_updates()
        
        with self.lock:
            route_points = self.current_route_points.copy()
            step_locations = self.current_step_locations.copy()
            was_updated = self.route_updated
            update_timestamp = self.route_update_timestamp
            
            # Debug logging - always show current state
            print(f"🔍 Route manager: points={len(route_points)}, updated={was_updated}, timestamp={update_timestamp}")
            
            if reset_update_flag and self.route_updated:
                self.route_updated = False
                print(f"🔍 Route manager: Reset update flag to False")
                
            return route_points, step_locations, was_updated, update_timestamp
    
    def _save_route_to_file(self):
        """Save current route to file for subprocess communication"""
        try:
            route_data = {
                'route_points': self.current_route_points,
                'step_locations': self.current_step_locations,
                'route_updated': self.route_updated,
                'route_update_timestamp': self.route_update_timestamp,
                'osrm_url': self.osrm_url,
                'movement_mode': self.movement_mode
            }
            
            with open(self.route_file, 'w') as f:
                json.dump(route_data, f)
            
            print(f"💾 Route saved to file: {self.route_file}")
            
        except Exception as e:
            print(f"❌ Error saving route to file: {e}")
    
    def _check_route_file_updates(self):
        """Check if route file has been updated (for subprocess communication)"""
        try:
            # Only check file every 1 second to avoid excessive I/O
            current_time = time.time()
            if current_time - self.last_file_check < 1:
                return
            
            self.last_file_check = current_time
            
            if not os.path.exists(self.route_file):
                return
            
            # Get file modification time
            file_mtime = os.path.getmtime(self.route_file)
            
            # If file is newer than our last update, load it
            if file_mtime > self.route_update_timestamp:
                with open(self.route_file, 'r') as f:
                    route_data = json.load(f)
                
                with self.lock:
                    old_points = len(self.current_route_points)
                    self.current_route_points = route_data['route_points']
                    self.current_step_locations = route_data['step_locations']
                    self.route_updated = route_data.get('route_updated', True)
                    self.route_update_timestamp = route_data['route_update_timestamp']
                    self.osrm_url = route_data.get('osrm_url', self.osrm_url)
                    self.movement_mode = route_data.get('movement_mode', self.movement_mode)
                    
                    print(f"📁 Route loaded from file: {old_points} -> {len(self.current_route_points)} points")
                    print(f"📁 File timestamp: {file_mtime}, Route timestamp: {self.route_update_timestamp}")
                    
        except Exception as e:
            print(f"❌ Error checking route file: {e}")
    
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

def test_route_manager():
    """Simple test function to verify route manager functionality"""
    print("🧪 Testing route manager...")
    
    # Test setting initial route
    test_points = [(35.84, 128.48), (35.845, 128.485), (35.85, 128.49)]
    test_steps = [
        {'location': [35.84, 128.48], 'duration': 10, 'distance': 100, 'instruction': 'start', 'speed_kmh': 30},
        {'location': [35.845, 128.485], 'duration': 15, 'distance': 150, 'instruction': 'continue', 'speed_kmh': 35},
        {'location': [35.85, 128.49], 'duration': 20, 'distance': 200, 'instruction': 'arrive', 'speed_kmh': 25}
    ]
    
    route_manager.set_initial_route(test_points, test_steps, "http://localhost:5001", "one-way")
    
    # Test getting route data
    points, steps, updated, timestamp = route_manager.get_current_route_data()
    print(f"   Initial route: {len(points)} points, updated: {updated}")
    
    # Test updating route
    test_waypoints = [{"lat": 35.86, "lng": 128.50, "name": "Test Point"}]
    # This would normally call OSRM, but for testing we'll just simulate
    with route_manager.lock:
        route_manager.current_route_points = [(35.84, 128.48), (35.86, 128.50)]
        route_manager.current_step_locations = [
            {'location': [35.84, 128.48], 'duration': 10, 'distance': 100, 'instruction': 'start', 'speed_kmh': 30},
            {'location': [35.86, 128.50], 'duration': 25, 'distance': 250, 'instruction': 'arrive', 'speed_kmh': 40}
        ]
        route_manager.route_updated = True
        route_manager.route_update_timestamp = time.time()
    
    # Test getting updated route data
    points, steps, updated, timestamp = route_manager.get_current_route_data(reset_update_flag=True)
    print(f"   Updated route: {len(points)} points, updated: {updated}")
    
    # Test that flag was reset
    points, steps, updated, timestamp = route_manager.get_current_route_data()
    print(f"   After reset: {len(points)} points, updated: {updated}")
    
    print("✅ Route manager test completed")

if __name__ == "__main__":
    test_route_manager()
