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
            print("4. Get topic information")
            print("5. Exit")
            print("-" * 60)
            
            choice = input("Enter your choice (1-5): ").strip()
            
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
                        manager.get_topic_info(selected_topic)
                    else:
                        print("❌ Invalid topic number")
                except ValueError:
                    print("❌ Please enter a valid number")
                    
            elif choice == '5':
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
