"""
Demo Script: Generate and Publish Sample Sensor Data
====================================================
This script creates realistic sensor data and publishes it to Kafka
so you can see what real messages look like!
"""

import pandas as pd
from datetime import datetime
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from streaming.kafka_producer import BiosphereKafkaProducer
from config.config import PipelineConfig

print("=" * 70)
print("KAFKA PRODUCER DEMO - Publishing Sample Sensor Data")
print("=" * 70)
print()

# Create sample data for type1 (sensors that sync together)
print("📊 Creating sample data for 'type1' category...")
type1_data = pd.DataFrame({
    'unique_id': [1, 2, 3, 4, 5],
    'timestamp': [datetime.now().isoformat()] * 5,
    'ahur_air_temperature': [23.5, 24.1, 22.8, 23.9, 24.5],
    'ahur_relative_humidity': [65.2, 63.8, 67.1, 64.5, 62.9],
    'ahur_co2_concentration': [410.8, 415.3, 408.2, 412.7, 418.1],
    'ahur_air_pressure': [101.3, 101.2, 101.4, 101.3, 101.2]
})
print(f"✅ Created {len(type1_data)} sensor readings for type1")
print()

# Create sample data for type2 (different sensors that sync together)
print("📊 Creating sample data for 'type2' category...")
type2_data = pd.DataFrame({
    'unique_id': [101, 102, 103],
    'timestamp': [datetime.now().isoformat()] * 3,
    'rhur_soil_moisture': [45.2, 47.8, 43.5],
    'rhur_soil_temperature': [18.3, 19.1, 17.9],
    'rhur_light_intensity': [850.5, 920.3, 780.1]
})
print(f"✅ Created {len(type2_data)} sensor readings for type2")
print()

# Create sample data for less50 (small sensor group)
print("📊 Creating sample data for 'less50' category...")
less50_data = pd.DataFrame({
    'unique_id': [201, 202],
    'timestamp': [datetime.now().isoformat()] * 2,
    'sensor_a': [12.5, 13.1],
    'sensor_b': [98.3, 97.8],
    'sensor_c': [250.7, 252.3]
})
print(f"✅ Created {len(less50_data)} sensor readings for less50")
print()

# Initialize Kafka producer
print("🔌 Connecting to Kafka...")
try:
    producer = BiosphereKafkaProducer()
    print("✅ Connected to Kafka successfully!")
    print()
except Exception as e:
    print(f"❌ Failed to connect to Kafka: {e}")
    print("Make sure Kafka is running: docker-compose up -d")
    exit(1)

# Publish type1 data
print("=" * 70)
print("PUBLISHING TYPE1 DATA (Temperature sensors)")
print("=" * 70)
result = producer.publish_cleaned_data(
    category='type1',
    data=type1_data,
    metadata={
        'table_count': 3,
        'source_tables': ['table1', 'table2', 'table3'],
        'csv_file': 'demo_type1.csv',
        'db_table': 'demo_type1'
    }
)
print(f"✅ Published {result['success']} messages to 'biosphere.rainforest.type1'")
print()

# Publish type2 data
print("=" * 70)
print("PUBLISHING TYPE2 DATA (Soil sensors)")
print("=" * 70)
result = producer.publish_cleaned_data(
    category='type2',
    data=type2_data,
    metadata={
        'table_count': 2,
        'source_tables': ['soil_table1', 'soil_table2'],
        'csv_file': 'demo_type2.csv',
        'db_table': 'demo_type2'
    }
)
print(f"✅ Published {result['success']} messages to 'biosphere.rainforest.type2'")
print()

# Publish less50 data
print("=" * 70)
print("PUBLISHING LESS50 DATA (Small sensor group)")
print("=" * 70)
result = producer.publish_cleaned_data(
    category='less50',
    data=less50_data,
    metadata={
        'table_count': 1,
        'source_tables': ['small_group_table'],
        'csv_file': 'demo_less50.csv',
        'db_table': 'demo_less50'
    }
)
print(f"✅ Published {result['success']} messages to 'biosphere.rainforest.less50'")
print()

# Close producer
producer.close()

print("=" * 70)
print("🎉 DEMO COMPLETE!")
print("=" * 70)
print()
print("📺 Now run the consumer to see these messages:")
print("   python src/streaming/consumers/simple_consumer.py")
print()
print("You should see REAL sensor data with:")
print("  ✅ Event IDs (unique tracking numbers)")
print("  ✅ Categories (type1, type2, less50)")
print("  ✅ Sensor readings (temperature, humidity, etc.)")
print("  ✅ Metadata (when processed, which tables used)")
print()
print("These are what your LLM and Omniverse teams will receive! 🚀")
print()
