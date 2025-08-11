import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class VehicleDataProducer:
    def __init__(self):
        """Initialize Kafka producer with configuration from .env file"""
        self.bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS', '123.143.232.180:19092')
        self.security_protocol = os.getenv('SECURITY_PROTOCOL', 'SASL_PLAINTEXT')
        self.sasl_mechanism = os.getenv('SASL.MECANISM', 'PLAIN')
        self.sasl_username = 'iov'  # From SASL.JASS.CONFIG
        self.sasl_password = 'iov'  # From SASL.JASS.CONFIG
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            security_protocol=self.security_protocol,
            sasl_mechanism=self.sasl_mechanism,
            sasl_plain_username=self.sasl_username,
            sasl_plain_password=self.sasl_password,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
    
    def create_login_message(self, vehicle_id="ETRI_VT60_ID04", sender_ip="192.168.1.100", 
                            destination_ip="192.168.1.200", password="vehicle_password"):
        """Create a login request message in the kuk11-2-1-LI.json format"""
        message_time = int(time.time() * 1000)  # Current timestamp in milliseconds
        
        message = {
            "messageTime": message_time,
            "messageType": 1,
            "dataTxt": {
                "authenticationInfo": "F2",
                "dataPacketNbr": 1,
                "dataPacketPriorityCd": 5,
                "pdu": {
                    "login": {
                        "senderTxt": sender_ip,
                        "destinationTxt": destination_ip,
                        "userNameTxt": vehicle_id,
                        "passwordTxt": password,
                        "encodingRules": "1.2.840.113549.1.1.1",
                        "heartbeatDurationMaxQty": 300,
                        "responseTimeOutQty": 30,
                        "initiatorCd": 1,
                        "datagramSizeQty": 1024
                    }
                }
            },
            "crcID": "7C"
        }
        
        return message

    def create_vehicle_message(self, vehicle_id="ETRI_VT60_ID04", probe_name="CITSOBE-0001", 
                              longitude=126.9780, latitude=37.5665, speed=5000, heading=9000):
        """Create a vehicle message in the kuk11-2.3-dv.json format"""
        current_time = datetime.now()
        message_time = int(time.time() * 1000)  # Current timestamp in milliseconds
        
        message = {
            "messageTime": message_time,
            "messageType": 0,
            "dataTxt": {
                "authenticationInfo": "F2",
                "dataPacketNbr": 1,
                "dataPacketPriorityCd": 5,
                "pdu": {
                    "userNameTxt": "VEHICLE001",
                    "frED": 0,
                    "publication": {
                        "publishGuaranteedBool": True,
                        "format": {
                            "data": {
                                "publishSerialNbr": 1,
                                "publishSerialCnt": 1,
                                "publishLatePublicationFlagBool": False,
                                "publicationType": {
                                    "publishManagementCd": 1,
                                    "publicationData": {
                                        "endApplicationMessageId": "MSG001",
                                        "endApplicationMessageMsg": {
                                            "probeID": {
                                                "name": probe_name,
                                                "id": "RANDOM123"
                                            },
                                            "vehicleID": vehicle_id,
                                            "vehicleType": "BUS",
                                            "timeInfo": {
                                                "year": current_time.year,
                                                "month": current_time.month,
                                                "day": current_time.day,
                                                "hour": current_time.hour,
                                                "minute": current_time.minute,
                                                "second": current_time.second,
                                                "millisecond": current_time.microsecond // 1000,
                                                "alivecount": 1
                                            },
                                            "thePosition": {
                                                "longitude": longitude,
                                                "latitude": latitude,
                                                "elevation": 5000,
                                                "heading": heading,
                                                "speed": speed,
                                                "posAccuracy": 500
                                            },
                                            "vehicleEvents": {
                                                "hazardLights": False,
                                                "absActivated": False,
                                                "hardBraking": False,
                                                "lightsChanged": 0,
                                                "flatTire": 0,
                                                "disabledVehicle": False,
                                                "getOnDown": False,
                                                "trouble": False,
                                                "hardDeceleration": False,
                                                "hardStop": False,
                                                "hardTurn": False,
                                                "uTurn": False
                                            },
                                            "serviceEvents": {
                                                "roadWork": False,
                                                "waypointArrived": False,
                                                "waypoinDeparture": False,
                                                "eta": 300,
                                                "serviceDoorOpen": False,
                                                "LiftOn": False
                                            },
                                            "vehicleStatus": {
                                                "lights": False,
                                                "lightBar": False,
                                                "brakeStatus": 0,
                                                "throttlePos": 0,
                                                "gpsStatus": 1,
                                                "transitStatus": False,
                                                "acceleration": 0,
                                                "worklanes": False,
                                                "curlane": 1,
                                                "vehicleCnt": 5,
                                                "batteryStatus": 85,
                                                "rangeStatus": 250,
                                                "failure": 0
                                            }
                                        }
                                    }
                                }
                            },
                            "publishFileNameTxt": ""
                        }
                    }
                }
            },
            "crcID": "7C"
        }
        
        return message
    
    def send_message(self, topic, message, key=None):
        """Send a message to Kafka topic"""
        try:
            future = self.producer.send(topic, value=message, key=key)
            record_metadata = future.get(timeout=10)
            print(f"Message sent to topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
            return True
        except KafkaError as e:
            print(f"Failed to send message: {e}")
            return False
    
    def send_logout_request(self, vehicle_id="ETRI_VT60_ID04", logout_code=1):
        """Send a logout request message to Kafka"""
        message_time = int(time.time() * 1000)  # Current timestamp in milliseconds
        
        message = {
            "messageTime": message_time,
            "messageType": 2,
            "dataTxt": {
                "authenticationInfo": "F2",
                "dataPacketNbr": 1,
                "dataPacketPriorityCd": 5,
                "pdu": {
                    "logout": {
                        "userNameTxt": vehicle_id,
                        "logoutCd": logout_code
                    }
                }
            },
            "crcID": "7C"
        }
        
        # Use vehicle-auth-request topic for authentication messages
        if self.send_message("vehicle-auth-request", message, key=vehicle_id):
            print(f"Logout request sent for vehicle {vehicle_id} to vehicle-auth-request topic")
            return True
        else:
            print(f"Failed to send logout request for vehicle {vehicle_id}")
            return False

    def send_login_request(self, vehicle_id="ETRI_VT60_ID04", sender_ip="192.168.1.100", 
                          destination_ip="192.168.1.200", password="vehicle_password"):
        """Send a login request message to Kafka"""
        message = self.create_login_message(vehicle_id, sender_ip, destination_ip, password)
        
        # Use vehicle-auth-request topic for authentication messages
        if self.send_message("vehicle-auth-request", message, key=vehicle_id):
            print(f"Login request sent for vehicle {vehicle_id} to vehicle-auth-request topic")
            return True
        else:
            print(f"Failed to send login request for vehicle {vehicle_id}")
            return False

    def simulate_vehicle_movement(self, topic, vehicle_id="ETRI_VT60_ID04", duration=60, interval=5):
        """Simulate vehicle movement by sending periodic updates"""
        print(f"Starting vehicle simulation for {duration} seconds...")
        
        # Starting position (Seoul coordinates from the JSON)
        base_longitude = 126.9780
        base_latitude = 37.5665
        
        start_time = time.time()
        message_count = 0
        
        while time.time() - start_time < duration:
            # Simulate slight movement
            longitude = base_longitude + random.uniform(-0.001, 0.001)
            latitude = base_latitude + random.uniform(-0.001, 0.001)
            speed = random.randint(3000, 7000)  # Speed in units used in the JSON
            heading = random.randint(0, 36000)  # Heading in decidegrees
            
            message = self.create_vehicle_message(
                vehicle_id=vehicle_id,
                longitude=longitude,
                latitude=latitude,
                speed=speed,
                heading=heading
            )
            
            if self.send_message(topic, message, key=vehicle_id):
                message_count += 1
                print(f"Sent message {message_count} for vehicle {vehicle_id}")
            
            time.sleep(interval)
        
        print(f"Simulation completed. Sent {message_count} messages.")
    
    def list_available_topics(self):
        """List all available Kafka topics"""
        try:
            from kafka import KafkaAdminClient
            
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                security_protocol=self.security_protocol,
                sasl_mechanism=self.sasl_mechanism,
                sasl_plain_username=self.sasl_username,
                sasl_plain_password=self.sasl_password
            )
            
            metadata = admin_client.list_topics()
            topics = list(metadata)
            
            print(f"📋 Available Kafka Topics ({len(topics)} total):")
            for i, topic in enumerate(topics, 1):
                print(f"{i:2d}. {topic}")
            
            admin_client.close()
            return topics
            
        except Exception as e:
            print(f"❌ Error listing topics: {e}")
            return []
    
    def close(self):
        """Close the Kafka producer"""
        self.producer.close()

def main():
    """Main function to demonstrate the Kafka producer"""
    producer = VehicleDataProducer()
    
    try:
        print("\n🚀 Kafka Vehicle Data Producer")
        print("=" * 50)
        print("1. List available topics")
        print("2. Send login request")
        print("3. Send logout request") 
        print("4. Send single vehicle message")
        print("5. Simulate vehicle movement")
        print("6. Exit")
        
        while True:
            choice = input("\nEnter your choice (1-6): ").strip()
            
            if choice == '1':
                producer.list_available_topics()
                
            elif choice == '2':
                print("Sending login request...")
                producer.send_login_request("ETRI_VT60_ID04")
                
            elif choice == '3':
                print("Sending logout request...")
                producer.send_logout_request("ETRI_VT60_ID04")
                
            elif choice == '4':
                print("Sending single vehicle message...")
                message = producer.create_vehicle_message()
                producer.send_message("vehicle-driving-data", message, key="ETRI_VT60_ID04")
                
            elif choice == '5':
                duration = int(input("Enter duration in seconds (default 30): ") or "30")
                interval = int(input("Enter interval in seconds (default 2): ") or "2")
                print(f"\nStarting vehicle movement simulation for {duration} seconds...")
                producer.simulate_vehicle_movement("vehicle-driving-data", "ETRI_VT60_ID04", duration=duration, interval=interval)
                
            elif choice == '6':
                print("👋 Goodbye!")
                break
                
            else:
                print("❌ Invalid choice. Please enter 1-6.")
        
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()
        print("Producer closed.")

if __name__ == "__main__":
    main()
