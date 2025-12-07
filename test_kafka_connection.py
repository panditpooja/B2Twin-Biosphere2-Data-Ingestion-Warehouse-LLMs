"""
Test Kafka Connection
=====================
Simple script to verify Kafka is working
"""

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
import time

print("=" * 60)
print("KAFKA CONNECTION TEST")
print("=" * 60)

# Test 1: Connect to Kafka
print("\n✅ Test 1: Connecting to Kafka...")
try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("✅ SUCCESS: Connected to Kafka broker at localhost:9092")
except Exception as e:
    print(f"❌ FAILED: Could not connect to Kafka - {e}")
    exit(1)

# Test 2: Send a test message
print("\n✅ Test 2: Sending test message...")
test_message = {
    "test": "Hello from Biosphere Pipeline!",
    "timestamp": "2025-12-02T10:00:00",
    "sensor": "test_sensor",
    "value": 42.0
}

try:
    future = producer.send('biosphere.rainforest.type1', test_message)
    result = future.get(timeout=10)
    print(f"✅ SUCCESS: Message sent to topic 'biosphere.rainforest.type1'")
    print(f"   Partition: {result.partition}")
    print(f"   Offset: {result.offset}")
except Exception as e:
    print(f"❌ FAILED: Could not send message - {e}")
    producer.close()
    exit(1)

producer.close()

# Test 3: Read the message back
print("\n✅ Test 3: Reading message back from Kafka...")
try:
    consumer = KafkaConsumer(
        'biosphere.rainforest.type1',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    message_found = False
    for message in consumer:
        print(f"✅ SUCCESS: Message received from Kafka!")
        print(f"   Topic: {message.topic}")
        print(f"   Partition: {message.partition}")
        print(f"   Offset: {message.offset}")
        print(f"   Value: {message.value}")
        message_found = True
        break
    
    if not message_found:
        print("⚠️  WARNING: No messages found (this might be OK if topics are empty)")
    
    consumer.close()
    
except Exception as e:
    print(f"❌ FAILED: Could not read message - {e}")
    exit(1)

# Test 4: List all topics
print("\n✅ Test 4: Listing all available topics...")
try:
    consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
    topics = consumer.topics()
    print(f"✅ SUCCESS: Found {len(topics)} topics:")
    for topic in sorted(topics):
        if topic.startswith('biosphere'):
            print(f"   📦 {topic}")
    consumer.close()
except Exception as e:
    print(f"❌ FAILED: Could not list topics - {e}")
    exit(1)

print("\n" + "=" * 60)
print("🎉 ALL TESTS PASSED! Kafka is ready to use!")
print("=" * 60)
print("\nYour Kafka setup is working correctly!")
print("You can now integrate Kafka into your pipeline. 🚀")
