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

    def append_route_from_current(self, current_position: Tuple[float, float], new_waypoints: List[Dict], osrm_url: str) -> bool:
        """Append new waypoints to the current route from current position"""
        try:
            print(f"➕ Starting route append from position {current_position}")
            print(f"➕ New waypoints to append:")
            for i, wp in enumerate(new_waypoints):
                print(f"   {i+1}. {wp.get('name', 'Unnamed')} at ({wp['lat']}, {wp['lng']})")
            
            # Validate waypoints format
            for i, waypoint in enumerate(new_waypoints):
                if 'lat' not in waypoint or 'lng' not in waypoint:
                    print(f"❌ Invalid waypoint {i+1}: missing 'lat' or 'lng' keys")
                    return False
                
                try:
                    float(waypoint['lat'])
                    float(waypoint['lng'])
                except (ValueError, TypeError):
                    print(f"❌ Invalid waypoint {i+1}: lat/lng must be numbers")
                    return False
            
            # Load existing waypoints from route file
            existing_waypoints = []
            try:
                import json
                if os.path.exists(self.route_file):
                    with open(self.route_file, 'r') as f:
                        route_data = json.load(f)
                        existing_waypoints = route_data.get('user_waypoints', [])
                        print(f"➕ Found {len(existing_waypoints)} existing waypoints")
            except Exception as e:
                print(f"Warning: Could not load existing waypoints: {e}")
            
            # Combine existing waypoints with new ones
            all_waypoints = existing_waypoints + new_waypoints
            print(f"➕ Total waypoints after append: {len(all_waypoints)}")
            
            # Get new route from OSRM with all waypoints
            new_route_points, new_step_locations = self._get_route_from_osrm_with_waypoints(
                current_position, all_waypoints, osrm_url
            )
            
            if not new_route_points or not new_step_locations:
                print("❌ Failed to get appended route from OSRM")
                return False
            
            with self.lock:
                old_points_count = len(self.current_route_points)
                self.current_route_points = new_route_points
                self.current_step_locations = new_step_locations
                self.route_updated = True
                self.route_update_timestamp = time.time()
                self.osrm_url = osrm_url  # Update OSRM URL as well
                
                print(f"✅ ROUTE APPENDED SUCCESSFULLY!")
                print(f"   - Old route: {old_points_count} points")
                print(f"   - New route: {len(new_route_points)} points")
                print(f"   - Added waypoints: {len(new_waypoints)}")
                print(f"   - Total waypoints: {len(all_waypoints)}")
                print(f"   - Update flag: {self.route_updated}")
                print(f"   - Timestamp: {self.route_update_timestamp}")
                print(f"   - Time: {time.strftime('%H:%M:%S')}")
                
                # Save updated route to file for subprocess communication, including all waypoints
                self._save_route_to_file(user_waypoints=all_waypoints)
                
            return True
            
        except Exception as e:
            print(f"❌ Error appending route: {e}")
            import traceback
            traceback.print_exc()
            return False

    def append_route_optimized_from_current(self, current_position: Tuple[float, float], new_waypoints: List[Dict], osrm_url: str) -> bool:
        """Append new waypoints to the current route with optimization while maintaining start-end order"""
        try:
            print(f"🚀 Starting optimized route append from position {current_position}")
            print(f"🚀 New waypoints to append and optimize:")
            for i, wp in enumerate(new_waypoints):
                print(f"   {i+1}. {wp.get('name', 'Unnamed')} at ({wp['lat']}, {wp['lng']})")
            
            # Validate waypoints format
            for i, waypoint in enumerate(new_waypoints):
                if 'lat' not in waypoint or 'lng' not in waypoint:
                    print(f"❌ Invalid waypoint {i+1}: missing 'lat' or 'lng' keys")
                    return False
                
                try:
                    float(waypoint['lat'])
                    float(waypoint['lng'])
                except (ValueError, TypeError):
                    print(f"❌ Invalid waypoint {i+1}: lat/lng must be numbers")
                    return False
            
            # Load existing waypoints from route file
            existing_waypoints = []
            try:
                import json
                if os.path.exists(self.route_file):
                    with open(self.route_file, 'r') as f:
                        route_data = json.load(f)
                        existing_waypoints = route_data.get('user_waypoints', [])
                        print(f"🚀 Found {len(existing_waypoints)} existing waypoints")
            except Exception as e:
                print(f"Warning: Could not load existing waypoints: {e}")
            
            # Combine existing waypoints with new ones
            all_waypoints = existing_waypoints + new_waypoints
            print(f"🚀 Total waypoints before optimization: {len(all_waypoints)}")
            
            # If we have more than 2 waypoints, optimize the route
            if len(all_waypoints) > 2:
                print(f"🚀 Optimizing route through {len(all_waypoints)} waypoints...")
                optimized_waypoints = self._optimize_waypoint_order(current_position, all_waypoints, osrm_url)
                if optimized_waypoints:
                    all_waypoints = optimized_waypoints
                    print(f"✅ Route optimization completed")
                else:
                    print(f"⚠️ Route optimization failed, using original order")
            else:
                print(f"🚀 Skipping optimization for {len(all_waypoints)} waypoints (too few to optimize)")
            
            # Get optimized route from OSRM with all waypoints
            new_route_points, new_step_locations = self._get_route_from_osrm_with_waypoints(
                current_position, all_waypoints, osrm_url
            )
            
            if not new_route_points or not new_step_locations:
                print("❌ Failed to get optimized route from OSRM")
                return False
            
            with self.lock:
                old_points_count = len(self.current_route_points)
                self.current_route_points = new_route_points
                self.current_step_locations = new_step_locations
                self.route_updated = True
                self.route_update_timestamp = time.time()
                self.osrm_url = osrm_url  # Update OSRM URL as well
                
                print(f"✅ OPTIMIZED ROUTE APPENDED SUCCESSFULLY!")
                print(f"   - Old route: {old_points_count} points")
                print(f"   - New route: {len(new_route_points)} points")
                print(f"   - Added waypoints: {len(new_waypoints)}")
                print(f"   - Total optimized waypoints: {len(all_waypoints)}")
                print(f"   - Update flag: {self.route_updated}")
                print(f"   - Timestamp: {self.route_update_timestamp}")
                print(f"   - Time: {time.strftime('%H:%M:%S')}")
                
                # Save updated route to file for subprocess communication, including all optimized waypoints
                self._save_route_to_file(user_waypoints=all_waypoints)
                
            return True
            
        except Exception as e:
            print(f"❌ Error appending optimized route: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _optimize_waypoint_order(self, start_position: Tuple[float, float], waypoints: List[Dict], osrm_url: str) -> List[Dict]:
        """Optimize the order of waypoints to minimize total travel time while maintaining start-end constraints"""
        try:
            if len(waypoints) <= 2:
                return waypoints  # No optimization needed for 2 or fewer waypoints
            
            print(f"🧮 Starting waypoint optimization for {len(waypoints)} waypoints")
            
            # Extract first and last waypoints to maintain start-end order
            if len(waypoints) >= 2:
                first_waypoint = waypoints[0]
                last_waypoint = waypoints[-1]
                middle_waypoints = waypoints[1:-1]
                print(f"🧮 Fixed start: {first_waypoint.get('name', 'Start')}")
                print(f"🧮 Fixed end: {last_waypoint.get('name', 'End')}")
                print(f"🧮 Optimizing {len(middle_waypoints)} middle waypoints")
            else:
                # If only one waypoint, no optimization needed
                return waypoints
            
            if not middle_waypoints:
                # If no middle waypoints, return as-is
                return waypoints
            
            # Create distance matrix for middle waypoints
            # Points to consider: start_position, first_waypoint, middle_waypoints, last_waypoint
            all_points = [start_position, (first_waypoint['lat'], first_waypoint['lng'])]
            all_points.extend([(wp['lat'], wp['lng']) for wp in middle_waypoints])
            all_points.append((last_waypoint['lat'], last_waypoint['lng']))
            
            print(f"🧮 Building distance matrix for {len(all_points)} points")
            distance_matrix = self._build_distance_matrix(all_points, osrm_url)
            
            if not distance_matrix:
                print("❌ Failed to build distance matrix, using original order")
                return waypoints
            
            # Use nearest neighbor heuristic to optimize middle waypoints
            # Start from first_waypoint (index 1), visit all middle waypoints, end at last_waypoint
            optimized_middle_indices = self._nearest_neighbor_optimization(
                distance_matrix, 
                start_idx=1,  # first_waypoint
                end_idx=len(all_points) - 1,  # last_waypoint
                middle_indices=list(range(2, len(all_points) - 1))  # middle waypoints
            )
            
            # Reconstruct optimized waypoint list
            optimized_waypoints = [first_waypoint]
            for idx in optimized_middle_indices:
                # Convert back to original middle_waypoints index
                middle_idx = idx - 2  # Adjust for start_position and first_waypoint
                optimized_waypoints.append(middle_waypoints[middle_idx])
            optimized_waypoints.append(last_waypoint)
            
            print(f"✅ Waypoint optimization completed:")
            for i, wp in enumerate(optimized_waypoints):
                print(f"   {i+1}. {wp.get('name', 'Unnamed')} at ({wp['lat']}, {wp['lng']})")
            
            return optimized_waypoints
            
        except Exception as e:
            print(f"❌ Error optimizing waypoints: {e}")
            return waypoints  # Return original order on error

    def _build_distance_matrix(self, points: List[Tuple[float, float]], osrm_url: str) -> List[List[float]]:
        """Build a distance matrix between all points using OSRM"""
        try:
            n = len(points)
            matrix = [[0.0] * n for _ in range(n)]
            
            # Build coordinate string for OSRM table service
            coordinates = []
            for lat, lng in points:
                coordinates.append(f"{lng},{lat}")  # OSRM uses lng,lat format
            
            coordinate_string = ";".join(coordinates)
            
            # Use OSRM table service to get distance matrix
            url = f"{osrm_url}/table/v1/driving/{coordinate_string}"
            params = {
                'annotations': 'duration,distance'
            }
            
            print(f"🧮 Requesting distance matrix from OSRM: {len(points)} points")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data['code'] != 'Ok':
                print(f"❌ OSRM table error: {data.get('message', 'Unknown error')}")
                return None
            
            # Extract duration matrix (in seconds)
            durations = data['durations']
            
            for i in range(n):
                for j in range(n):
                    if i != j and durations[i][j] is not None:
                        matrix[i][j] = durations[i][j]
                    else:
                        matrix[i][j] = 0.0 if i == j else float('inf')
            
            print(f"✅ Distance matrix built successfully: {n}x{n}")
            return matrix
            
        except Exception as e:
            print(f"❌ Error building distance matrix: {e}")
            return None

    def _nearest_neighbor_optimization(self, distance_matrix: List[List[float]], start_idx: int, end_idx: int, middle_indices: List[int]) -> List[int]:
        """Use nearest neighbor heuristic to optimize the order of middle waypoints"""
        try:
            if not middle_indices:
                return []
            
            print(f"🧮 Optimizing {len(middle_indices)} middle waypoints using nearest neighbor")
            
            # Start from the start_idx, visit all middle waypoints, end at end_idx
            current_idx = start_idx
            unvisited = set(middle_indices)
            optimized_order = []
            
            while unvisited:
                # Find nearest unvisited waypoint
                nearest_idx = None
                nearest_distance = float('inf')
                
                for idx in unvisited:
                    distance = distance_matrix[current_idx][idx]
                    if distance < nearest_distance:
                        nearest_distance = distance
                        nearest_idx = idx
                
                if nearest_idx is not None:
                    optimized_order.append(nearest_idx)
                    unvisited.remove(nearest_idx)
                    current_idx = nearest_idx
                else:
                    # Fallback: add remaining waypoints in original order
                    optimized_order.extend(sorted(unvisited))
                    break
            
            # Calculate total distance for comparison
            total_distance = distance_matrix[start_idx][optimized_order[0]] if optimized_order else 0
            for i in range(len(optimized_order) - 1):
                total_distance += distance_matrix[optimized_order[i]][optimized_order[i + 1]]
            if optimized_order:
                total_distance += distance_matrix[optimized_order[-1]][end_idx]
            
            print(f"✅ Nearest neighbor optimization completed")
            print(f"   - Optimized order: {optimized_order}")
            print(f"   - Total estimated time: {total_distance/60:.1f} minutes")
            
            return optimized_order
            
        except Exception as e:
            print(f"❌ Error in nearest neighbor optimization: {e}")
            return middle_indices  # Return original order on error
    
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
