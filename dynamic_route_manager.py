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
            self.route_updated = False  # Initial route is not considered an "update"
            self.route_update_timestamp = time.time()
            print(f"✓ Initial route set with {len(route_points)} points")
            
            # Save initial route to file (no user waypoints for initial route)
            self._save_route_to_file()
    
    def update_route_from_current(self, current_position: Tuple[float, float], waypoints: List[Dict], osrm_url: str) -> bool:
        """Update the route from current position through waypoints"""
        try:
            print(f"🔄 Starting route update from position {current_position}")
            print(f"🔄 User waypoints for auto-pause:")
            for i, wp in enumerate(waypoints):
                print(f"   {i+1}. {wp.get('name', 'Unnamed')} at ({wp['lat']}, {wp['lng']})")
            
            # Validate waypoints format
            for i, waypoint in enumerate(waypoints):
                if 'lat' not in waypoint or 'lng' not in waypoint:
                    print(f"❌ Invalid waypoint {i+1}: missing 'lat' or 'lng' keys")
                    return False
                
                try:
                    float(waypoint['lat'])
                    float(waypoint['lng'])
                except (ValueError, TypeError):
                    print(f"❌ Invalid waypoint {i+1}: lat/lng must be numbers")
                    return False
            
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
                self.osrm_url = osrm_url  # Update OSRM URL as well
                
                print(f"✅ ROUTE UPDATED SUCCESSFULLY!")
                print(f"   - Old route: {old_points_count} points")
                print(f"   - New route: {len(new_route_points)} points")
                print(f"   - Update flag: {self.route_updated}")
                print(f"   - Timestamp: {self.route_update_timestamp}")
                print(f"   - Time: {time.strftime('%H:%M:%S')}")
                
                # Save updated route to file for subprocess communication, including user waypoints
                self._save_route_to_file(user_waypoints=waypoints)
                
            return True
            
        except Exception as e:
            print(f"❌ Error updating route: {e}")
            import traceback
            traceback.print_exc()
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
            
            # Only log when there's actually an update (remove periodic logging)
            if was_updated:
                print(f"🔍 Route manager: points={len(route_points)}, updated={was_updated}, timestamp={update_timestamp}")
            
            if reset_update_flag and self.route_updated:
                self.route_updated = False
                print(f"🔍 Route manager: Reset update flag to False")
                
            return route_points, step_locations, was_updated, update_timestamp
    
    def _save_route_to_file(self, user_waypoints=None):
        """Save current route to file for subprocess communication"""
        try:
            route_data = {
                'route_points': self.current_route_points,
                'step_locations': self.current_step_locations,
                'route_updated': False,  # Always save as False to prevent infinite loops
                'route_update_timestamp': self.route_update_timestamp,
                'osrm_url': self.osrm_url,
                'movement_mode': self.movement_mode,
                'user_waypoints': user_waypoints or []  # Store user-specified waypoints
            }
            
            with open(self.route_file, 'w') as f:
                json.dump(route_data, f)
            
            print(f"💾 Route saved to file: {self.route_file} (timestamp: {self.route_update_timestamp})")
            
        except Exception as e:
            print(f"❌ Error saving route to file: {e}")
    
    def _check_route_file_updates(self):
        """Check if route file has been updated (for subprocess communication)"""
        try:
            # Only check file every 10 seconds to reduce I/O and logging
            current_time = time.time()
            if current_time - self.last_file_check < 10:
                return
            
            self.last_file_check = current_time
            
            if not os.path.exists(self.route_file):
                return
            
            # Get file modification time
            file_mtime = os.path.getmtime(self.route_file)
            
            # If file is newer than our last update, load it
            # Add a small tolerance (1 second) to avoid timestamp precision issues
            if file_mtime > (self.route_update_timestamp + 1):
                with open(self.route_file, 'r') as f:
                    route_data = json.load(f)
                
                with self.lock:
                    old_points = len(self.current_route_points)
                    new_points = len(route_data['route_points'])
                    
                    # Only update if the route actually changed significantly
                    if (abs(new_points - old_points) > 10 or 
                        route_data['route_update_timestamp'] > (self.route_update_timestamp + 2)):
                        
                        self.current_route_points = route_data['route_points']
                        self.current_step_locations = route_data['step_locations']
                        self.route_updated = True  # Mark as updated when loaded from file
                        self.route_update_timestamp = route_data['route_update_timestamp']
                        self.osrm_url = route_data.get('osrm_url', self.osrm_url)
                        self.movement_mode = route_data.get('movement_mode', self.movement_mode)
                        
                        print(f"📁 Route loaded from file: {old_points} -> {new_points} points")
                    
        except Exception as e:
            # Only log file errors occasionally to avoid spam
            if int(current_time) % 60 == 0:  # Reduced frequency to every 60 seconds
                print(f"❌ Error checking route file: {e}")
    
    def _get_route_from_osrm_with_waypoints(self, current_position: Tuple[float, float], 
                                          waypoints: List[Dict], osrm_url: str) -> Tuple[List, List]:
        """Get route from OSRM with current position and waypoints"""
        try:
            print(f"🗺️ Building route from current position {current_position} through {len(waypoints)} waypoints")
            
            # Build coordinate string for OSRM (longitude,latitude)
            # current_position is (lat, lon), so we need to swap for OSRM
            coordinates = [f"{current_position[1]},{current_position[0]}"]  # Start from current position
            print(f"🗺️ Starting point: {current_position[1]},{current_position[0]} (lng,lat)")
            
            # waypoints have 'lat' and 'lng' keys, convert to OSRM format (lng,lat)
            for i, waypoint in enumerate(waypoints):
                coord_str = f"{waypoint['lng']},{waypoint['lat']}"
                coordinates.append(coord_str)
                print(f"🗺️ Waypoint {i+1}: {coord_str} (lng,lat) - {waypoint.get('name', 'Unnamed')}")
            
            coordinate_string = ";".join(coordinates)
            print(f"🗺️ OSRM coordinate string: {coordinate_string}")
            
            # OSRM route API endpoint with multiple waypoints
            url = f"{osrm_url}/route/v1/driving/{coordinate_string}"
            params = {
                'overview': 'full',
                'geometries': 'geojson',
                'steps': 'true'
            }
            
            print(f"🗺️ Requesting route from OSRM: {url}")
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data['code'] != 'Ok':
                print(f"❌ OSRM returned error: {data.get('message', 'Unknown error')}")
                raise Exception(f"OSRM error: {data.get('message', 'Unknown error')}")
            
            route = data['routes'][0]
            print(f"✓ OSRM route received: {route['distance']/1000:.2f}km, {route['duration']/60:.1f}min")
            
            # Extract route geometry (coordinates are in [longitude, latitude] format)
            geometry_coordinates = route['geometry']['coordinates']
            route_points = [(coord[1], coord[0]) for coord in geometry_coordinates]  # Convert to (lat, lon)
            print(f"✓ Extracted {len(route_points)} route geometry points")
            
            # Extract step-by-step locations
            step_locations = []
            step_count = 0
            for leg_idx, leg in enumerate(route['legs']):
                print(f"🗺️ Processing leg {leg_idx + 1}/{len(route['legs'])}: {leg['distance']/1000:.2f}km, {leg['duration']/60:.1f}min")
                for step_idx, step in enumerate(leg['steps']):
                    # Get maneuver location (intersection/turn point)
                    maneuver_location = step['maneuver']['location']
                    step_info = {
                        'location': (maneuver_location[1], maneuver_location[0]),  # (lat, lon)
                        'duration': step['duration'],  # seconds
                        'distance': step['distance'],  # meters
                        'instruction': step['maneuver']['type'],
                        'name': step.get('name', ''),
                        'speed_kmh': 30  # Default speed
                    }
                    
                    # Calculate speed for this step
                    if step['duration'] > 0:
                        speed_ms = step['distance'] / step['duration']
                        step_info['speed_kmh'] = max(10, min(80, round(speed_ms * 3.6, 2)))  # Clamp between 10-80 km/h
                    
                    step_locations.append(step_info)
                    step_count += 1
                    
                    if step_count <= 5:  # Log first few steps for debugging
                        print(f"   Step {step_count}: {step_info['instruction']} at ({step_info['location'][0]:.6f}, {step_info['location'][1]:.6f}) - {step_info['speed_kmh']} km/h")
            
            print(f"✅ New route calculated successfully:")
            print(f"   - Route points: {len(route_points)}")
            print(f"   - Step locations: {len(step_locations)}")
            print(f"   - Total distance: {route['distance']/1000:.2f}km")
            print(f"   - Total duration: {route['duration']/60:.1f}min")
            
            return route_points, step_locations
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Network error getting route from OSRM: {e}")
            return [], []
        except Exception as e:
            print(f"❌ Error getting route from OSRM: {e}")
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
