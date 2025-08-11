import json
import time
import os
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType
from kafka.errors import KafkaError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class KafkaTopicManager:
    def __init__(self):
        """Initialize Kafka client with configuration from .env file"""
        self.bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS', '123.143.232.180:19092')
        self.security_protocol = os.getenv('SECURITY_PROTOCOL', 'SASL_PLAINTEXT')
        self.sasl_mechanism = os.getenv('SASL.MECANISM', 'PLAIN')
        self.sasl_username = 'iov'  # From SASL.JASS.CONFIG
        self.sasl_password = 'iov'  # From SASL.JASS.CONFIG
        
        # Initialize Kafka admin client
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            security_protocol=self.security_protocol,
            sasl_mechanism=self.sasl_mechanism,
            sasl_plain_username=self.sasl_username,
            sasl_plain_password=self.sasl_password
        )
    
    def list_topics(self):
        """List all available Kafka topics"""
        try:
            metadata = self.admin_client.list_topics()
            topics = list(metadata)
            print(f"\n📋 Available Kafka Topics ({len(topics)} total):")
            print("=" * 50)
            
            for i, topic in enumerate(topics, 1):
                print(f"{i:2d}. {topic}")
            
            return topics
        except Exception as e:
            print(f"❌ Error listing topics: {e}")
            return []
    
    def get_topic_info(self, topic_name):
        """Get detailed information about a specific topic"""
        try:
            metadata = self.admin_client.describe_topics([topic_name])
            topic_metadata = metadata[topic_name]
            
            print(f"\n📊 Topic Information: {topic_name}")
            print("=" * 50)
            print(f"Partitions: {len(topic_metadata.partitions)}")
            
            for partition in topic_metadata.partitions:
                print(f"  Partition {partition.partition}: Leader={partition.leader}, Replicas={len(partition.replicas)}")
            
            return topic_metadata
        except Exception as e:
            print(f"❌ Error getting topic info: {e}")
            return None
    
    def create_consumer(self, topic_name, group_id=None):
        """Create a Kafka consumer for the specified topic"""
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=self.bootstrap_servers,
                security_protocol=self.security_protocol,
                sasl_mechanism=self.sasl_mechanism,
                sasl_plain_username=self.sasl_username,
                sasl_plain_password=self.sasl_password,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                group_id=group_id or f"topic-manager-{int(time.time())}",
                auto_offset_reset='latest',  # Start from latest messages
                enable_auto_commit=True
            )
            return consumer
        except Exception as e:
            print(f"❌ Error creating consumer: {e}")
            return None
    
    def consume_messages(self, topic_name, max_messages=10, timeout_seconds=30):
        """Consume and display messages from the selected topic"""
        print(f"\n🔄 Starting to consume messages from topic: {topic_name}")
        print(f"📊 Will show up to {max_messages} messages (timeout: {timeout_seconds}s)")
        print("=" * 70)
        
        consumer = self.create_consumer(topic_name)
        if not consumer:
            return
        
        try:
            message_count = 0
            start_time = time.time()
            
            print("⏳ Waiting for messages... (Press Ctrl+C to stop)")
            
            for message in consumer:
                if message_count >= max_messages:
                    print(f"\n✅ Reached maximum message limit ({max_messages})")
                    break
                
                if time.time() - start_time > timeout_seconds:
                    print(f"\n⏰ Timeout reached ({timeout_seconds}s)")
                    break
                
                message_count += 1
                timestamp = datetime.fromtimestamp(message.timestamp / 1000) if message.timestamp else datetime.now()
                
                print(f"\n📨 Message #{message_count}")
                print(f"🕒 Timestamp: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"🔑 Key: {message.key}")
                print(f"📍 Partition: {message.partition}, Offset: {message.offset}")
                print("📄 Value:")
                
                # Pretty print JSON if possible
                try:
                    if isinstance(message.value, dict):
                        print(json.dumps(message.value, indent=2, ensure_ascii=False))
                    else:
                        print(message.value)
                except Exception as e:
                    print(f"Raw value: {message.value}")
                
                print("-" * 50)
            
            if message_count == 0:
                print("📭 No messages received within the timeout period")
            else:
                print(f"\n✅ Consumed {message_count} messages from topic '{topic_name}'")
                
        except KeyboardInterrupt:
            print(f"\n🛑 Stopped by user. Consumed {message_count} messages.")
        except Exception as e:
            print(f"❌ Error consuming messages: {e}")
        finally:
            consumer.close()
    
    def send_test_message(self, topic_name, message_data=None):
        """Send a test message to the selected topic"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                security_protocol=self.security_protocol,
                sasl_mechanism=self.sasl_mechanism,
                sasl_plain_username=self.sasl_username,
                sasl_plain_password=self.sasl_password,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            
            if message_data is None:
                # Create a default test message
                message_data = {
                    "test_message": True,
                    "timestamp": int(time.time() * 1000),
                    "sent_by": "kafka-topic-manager",
                    "message": "This is a test message from Kafka Topic Manager"
                }
            
            future = producer.send(topic_name, value=message_data, key="test-key")
            record_metadata = future.get(timeout=10)
            
            print(f"✅ Test message sent to topic '{topic_name}'")
            print(f"📍 Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            
            producer.close()
            return True
            
        except Exception as e:
            print(f"❌ Error sending test message: {e}")
            return False
    
    def send_vehicle_login_message(self, topic_name, vehicle_id="ETRI_VT60_ID04", 
                                  sender_ip="192.168.1.100", destination_ip="192.168.1.200", 
                                  password="vehicle_password"):
        """Send a vehicle login request message based on kuk11-2-1-LI.json format"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                security_protocol=self.security_protocol,
                sasl_mechanism=self.sasl_mechanism,
                sasl_plain_username=self.sasl_username,
                sasl_plain_password=self.sasl_password,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            
            message_time = int(time.time() * 1000)
            
            login_message = {
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
            
            future = producer.send(topic_name, value=login_message, key=vehicle_id)
            record_metadata = future.get(timeout=10)
            
            print(f"✅ Vehicle login message sent to topic '{topic_name}'")
            print(f"🚗 Vehicle ID: {vehicle_id}")
            print(f"📍 Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            
            producer.close()
            return True
            
        except Exception as e:
            print(f"❌ Error sending vehicle login message: {e}")
            return False
    
    def send_vehicle_logout_message(self, topic_name, vehicle_id="ETRI_VT60_ID04", logout_code=1):
        """Send a vehicle logout request message based on kuk11-2-1-LO.json format"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                security_protocol=self.security_protocol,
                sasl_mechanism=self.sasl_mechanism,
                sasl_plain_username=self.sasl_username,
                sasl_plain_password=self.sasl_password,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            
            message_time = int(time.time() * 1000)
            
            logout_message = {
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
            
            future = producer.send(topic_name, value=logout_message, key=vehicle_id)
            record_metadata = future.get(timeout=10)
            
            print(f"✅ Vehicle logout message sent to topic '{topic_name}'")
            print(f"🚗 Vehicle ID: {vehicle_id}")
            print(f"📍 Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            
            producer.close()
            return True
            
        except Exception as e:
            print(f"❌ Error sending vehicle logout message: {e}")
            return False
    
    def send_vehicle_control_message(self, topic_name, vehicle_id="ETRI_VT60_ID04", service_id=1001):
        """Send a vehicle control message based on kuk11-3.2-VC.json format"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                security_protocol=self.security_protocol,
                sasl_mechanism=self.sasl_mechanism,
                sasl_plain_username=self.sasl_username,
                sasl_plain_password=self.sasl_password,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            
            message_time = int(time.time() * 1000)
            
            control_message = {
                "messageTime": message_time,
                "messageType": 8,
                "dataTxt": {
                    "vehicleID": vehicle_id,
                    "serviceId": service_id,
                    "startYear": 2024,
                    "startTime": 480,
                    "durationTime": 60,
                    "priority": 1,
                    "serviceType": {
                        "vehicleControl": [
                            {
                                "ctrlType": 0,
                                "ctrlValue": True
                            },
                            {
                                "ctrlType": 1,
                                "ctrlValue": False
                            },
                            {
                                "ctrlType": 2,
                                "ctrlValue": True
                            },
                            {
                                "ctrlType": 3,
                                "ctrlValue": True
                            }
                        ]
                    }
                },
                "crcID": "7C"
            }
            
            future = producer.send(topic_name, value=control_message, key=vehicle_id)
            record_metadata = future.get(timeout=10)
            
            print(f"✅ Vehicle control message sent to topic '{topic_name}'")
            print(f"🚗 Vehicle ID: {vehicle_id}")
            print(f"📍 Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            
            producer.close()
            return True
            
        except Exception as e:
            print(f"❌ Error sending vehicle control message: {e}")
            return False
    
    def send_auth_request_to_topic(self, message_type="login", vehicle_id="ETRI_VT60_ID04"):
        """Send authentication request to vehicle-auth-request topic specifically"""
        auth_topic = "vehicle-auth-request"
        
        print(f"\n🔐 Sending {message_type} request to '{auth_topic}' topic...")
        
        if message_type == "login":
            return self.send_vehicle_login_message(auth_topic, vehicle_id)
        elif message_type == "logout":
            return self.send_vehicle_logout_message(auth_topic, vehicle_id)
        else:
            print(f"❌ Unknown message type: {message_type}")
            return False
    
    def close(self):
        """Close the admin client"""
        try:
            self.admin_client.close()
        except:
            pass

def main():
    """Main interactive function"""
    manager = KafkaTopicManager()
    
    try:
        while True:
            print("\n" + "=" * 60)
            print("🚀 Kafka Topic Manager")
            print("=" * 60)
            print("1. List all topics")
            print("2. Select topic and consume messages")
            print("3. Send test message to topic")
            print("4. Send vehicle authentication messages")
            print("5. Quick send to vehicle-auth-request topic")
            print("6. Get topic information")
            print("7. Exit")
            print("-" * 60)
            
            choice = input("Enter your choice (1-7): ").strip()
            
            if choice == '1':
                topics = manager.list_topics()
                if not topics:
                    print("❌ No topics found or error occurred")
                    
            elif choice == '2':
                topics = manager.list_topics()
                if not topics:
                    print("❌ No topics available")
                    continue
                
                try:
                    topic_num = int(input(f"\nSelect topic number (1-{len(topics)}): "))
                    if 1 <= topic_num <= len(topics):
                        selected_topic = topics[topic_num - 1]
                        print(f"\n🎯 Selected topic: {selected_topic}")
                        
                        # Get consumption parameters
                        try:
                            max_msgs = int(input("Max messages to consume (default 10): ") or "10")
                            timeout = int(input("Timeout in seconds (default 30): ") or "30")
                        except ValueError:
                            max_msgs, timeout = 10, 30
                        
                        manager.consume_messages(selected_topic, max_msgs, timeout)
                    else:
                        print("❌ Invalid topic number")
                except ValueError:
                    print("❌ Please enter a valid number")
                    
            elif choice == '3':
                topics = manager.list_topics()
                if not topics:
                    print("❌ No topics available")
                    continue
                
                try:
                    topic_num = int(input(f"\nSelect topic number (1-{len(topics)}): "))
                    if 1 <= topic_num <= len(topics):
                        selected_topic = topics[topic_num - 1]
                        print(f"\n🎯 Selected topic: {selected_topic}")
                        manager.send_test_message(selected_topic)
                    else:
                        print("❌ Invalid topic number")
                except ValueError:
                    print("❌ Please enter a valid number")
                    
            elif choice == '4':
                topics = manager.list_topics()
                if not topics:
                    print("❌ No topics available")
                    continue
                
                try:
                    topic_num = int(input(f"\nSelect topic number (1-{len(topics)}): "))
                    if 1 <= topic_num <= len(topics):
                        selected_topic = topics[topic_num - 1]
                        print(f"\n🎯 Selected topic: {selected_topic}")
                        
                        print("\n🚗 Vehicle Authentication Messages:")
                        print("1. Send vehicle login message")
                        print("2. Send vehicle logout message") 
                        print("3. Send vehicle control message")
                        print("4. Back to main menu")
                        
                        auth_choice = input("Enter your choice (1-4): ").strip()
                        
                        if auth_choice == '1':
                            vehicle_id = input("Enter vehicle ID (default: ETRI_VT60_ID04): ").strip() or "ETRI_VT60_ID04"
                            sender_ip = input("Enter sender IP (default: 192.168.1.100): ").strip() or "192.168.1.100"
                            destination_ip = input("Enter destination IP (default: 192.168.1.200): ").strip() or "192.168.1.200"
                            password = input("Enter password (default: vehicle_password): ").strip() or "vehicle_password"
                            
                            manager.send_vehicle_login_message(selected_topic, vehicle_id, sender_ip, destination_ip, password)
                            
                        elif auth_choice == '2':
                            vehicle_id = input("Enter vehicle ID (default: ETRI_VT60_ID04): ").strip() or "ETRI_VT60_ID04"
                            logout_code = int(input("Enter logout code (default: 1): ").strip() or "1")
                            
                            manager.send_vehicle_logout_message(selected_topic, vehicle_id, logout_code)
                            
                        elif auth_choice == '3':
                            vehicle_id = input("Enter vehicle ID (default: ETRI_VT60_ID04): ").strip() or "ETRI_VT60_ID04"
                            service_id = int(input("Enter service ID (default: 1001): ").strip() or "1001")
                            
                            manager.send_vehicle_control_message(selected_topic, vehicle_id, service_id)
                            
                        elif auth_choice == '4':
                            continue
                        else:
                            print("❌ Invalid choice")
                    else:
                        print("❌ Invalid topic number")
                except ValueError:
                    print("❌ Please enter a valid number")
                    
            elif choice == '5':
                print("\n🚀 Quick send to vehicle-auth-request topic:")
                print("1. Send login request")
                print("2. Send logout request")
                print("3. Back to main menu")
                
                quick_choice = input("Enter your choice (1-3): ").strip()
                
                if quick_choice == '1':
                    vehicle_id = input("Enter vehicle ID (default: ETRI_VT60_ID04): ").strip() or "ETRI_VT60_ID04"
                    manager.send_auth_request_to_topic("login", vehicle_id)
                elif quick_choice == '2':
                    vehicle_id = input("Enter vehicle ID (default: ETRI_VT60_ID04): ").strip() or "ETRI_VT60_ID04"
                    manager.send_auth_request_to_topic("logout", vehicle_id)
                elif quick_choice == '3':
                    continue
                else:
                    print("❌ Invalid choice")
                    
            elif choice == '6':
                topics = manager.list_topics()
                if not topics:
                    print("❌ No topics available")
                    continue
                
                try:
                    topic_num = int(input(f"\nSelect topic number (1-{len(topics)}): "))
                    if 1 <= topic_num <= len(topics):
                        selected_topic = topics[topic_num - 1]
                        manager.get_topic_info(selected_topic)
                    else:
                        print("❌ Invalid topic number")
                except ValueError:
                    print("❌ Please enter a valid number")
                    
            elif choice == '7':
                print("👋 Goodbye!")
                break
                
            else:
                print("❌ Invalid choice. Please enter 1-5.")
                
    except KeyboardInterrupt:
        print("\n\n🛑 Interrupted by user")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        manager.close()

if __name__ == "__main__":
    main()
