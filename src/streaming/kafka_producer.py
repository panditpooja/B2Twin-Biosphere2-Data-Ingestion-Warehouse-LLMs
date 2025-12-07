"""
Kafka Producer for Biosphere Pipeline
======================================
Publishes cleaned sensor data to Kafka topics
"""

import json
import logging
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys
import os

# Add parent directory to path for config import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import PipelineConfig

logger = logging.getLogger(__name__)


class BiosphereKafkaProducer:
    """
    Kafka producer for publishing cleaned Biosphere sensor data.
    
    Features:
    - Publishes DataFrame data to appropriate Kafka topics
    - Converts data to structured JSON messages
    - Handles errors and retries
    - Thread-safe for concurrent publishing
    """
    
    def __init__(self, bootstrap_servers: str = None):
        """
        Initialize Kafka producer.
        
        Args:
            bootstrap_servers: Kafka broker address (default from config)
        """
        self.bootstrap_servers = bootstrap_servers or PipelineConfig.KAFKA_BOOTSTRAP_SERVERS
        self.producer = None
        self._initialize_producer()
    
    def _initialize_producer(self):
        """Initialize Kafka producer with configuration"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                **PipelineConfig.KAFKA_PRODUCER_CONFIG
            )
            logger.info(f"✅ Kafka producer initialized: {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka producer: {e}")
            raise
    
    def publish_cleaned_data(
        self, 
        category: str, 
        data: pd.DataFrame,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, int]:
        """
        Publish cleaned sensor data to Kafka topic.
        
        Args:
            category: Data category (type1, type2, less50, between50and100, other)
            data: Cleaned pandas DataFrame with sensor data
            metadata: Optional metadata to include in messages
        
        Returns:
            Dictionary with success/failure counts
        """
        if not PipelineConfig.KAFKA_ENABLE_PRODUCER:
            logger.info("⏭️  Kafka producer disabled in config, skipping publish")
            return {"success": 0, "failed": 0, "skipped": len(data)}
        
        if data.empty:
            logger.warning(f"⚠️  No data to publish for category '{category}'")
            return {"success": 0, "failed": 0, "skipped": 0}
        
        # Get topic name from config
        topic = PipelineConfig.KAFKA_TOPICS.get(category)
        if not topic:
            logger.error(f"❌ No topic configured for category '{category}'")
            return {"success": 0, "failed": len(data), "skipped": 0}
        
        logger.info(f"📤 Publishing {len(data)} records to topic '{topic}'...")
        
        success_count = 0
        failed_count = 0
        
        # Determine if this is "other" category with multiple timestamps
        is_other_category = category in ['less50', 'between50and100', 'other']
        
        # Convert DataFrame to records
        records = data.to_dict(orient='records')
        
        for record in records:
            try:
                message = self._build_message(record, category, is_other_category, metadata)
                
                # Send to Kafka
                future = self.producer.send(topic, value=message)
                result = future.get(timeout=10)  # Wait for acknowledgment
                
                success_count += 1
                
                if success_count % 100 == 0:
                    logger.info(f"  ✅ Published {success_count}/{len(data)} messages...")
                
            except KafkaError as e:
                logger.error(f"  ❌ Kafka error publishing message: {e}")
                failed_count += 1
            except Exception as e:
                logger.error(f"  ❌ Error publishing message: {e}")
                failed_count += 1
        
        # Flush to ensure all messages are sent
        self.producer.flush()
        
        logger.info(f"✅ Published to '{topic}': {success_count} success, {failed_count} failed")
        
        return {
            "success": success_count,
            "failed": failed_count,
            "skipped": 0
        }
    
    def _build_message(
        self, 
        record: Dict[str, Any], 
        category: str,
        is_other_category: bool,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Build structured JSON message from record.
        
        Args:
            record: Single row from DataFrame as dictionary
            category: Data category
            is_other_category: Whether this is 'other' category with multiple timestamps
            metadata: Optional metadata
        
        Returns:
            Structured message dictionary
        """
        # Generate unique event ID
        event_id = str(uuid.uuid4())
        
        # Extract unique_id and timestamp(s)
        unique_id = record.get('unique_id')
        
        # Build sensor data (exclude unique_id and timestamp fields)
        sensors = {}
        
        if is_other_category:
            # For 'other' category, each sensor has its own timestamp
            for key, value in record.items():
                if key == 'unique_id':
                    continue
                elif key.startswith('timestamp_'):
                    # Skip timestamp fields for now, will handle below
                    continue
                else:
                    # Find corresponding timestamp
                    timestamp_key = f'timestamp_{key}'
                    sensor_timestamp = record.get(timestamp_key)
                    
                    sensors[key] = {
                        "value": value,
                        "timestamp": str(sensor_timestamp) if sensor_timestamp else None
                    }
        else:
            # For type1/type2, single timestamp for all sensors
            timestamp = record.get('timestamp')
            
            for key, value in record.items():
                if key not in ['unique_id', 'timestamp']:
                    sensors[key] = value
        
        # Build message
        message = {
            "event_id": event_id,
            "unique_id": unique_id,
            "category": category
        }
        
        # Add timestamp for non-other categories
        if not is_other_category:
            message["timestamp"] = str(record.get('timestamp'))
        
        message["sensors"] = sensors
        
        # Add metadata
        message["metadata"] = {
            "processed_at": datetime.now().isoformat(),
            "pipeline_version": "1.0",
            "source": "biosphere_pipeline"
        }
        
        # Add custom metadata if provided
        if metadata:
            message["metadata"].update(metadata)
        
        return message
    
    def close(self):
        """Close Kafka producer connection"""
        if self.producer:
            self.producer.close()
            logger.info("🔒 Kafka producer closed")


def create_producer() -> BiosphereKafkaProducer:
    """
    Factory function to create Kafka producer.
    
    Returns:
        Configured BiosphereKafkaProducer instance
    """
    return BiosphereKafkaProducer()


# Example usage
if __name__ == "__main__":
    # Test the producer
    logging.basicConfig(level=logging.INFO)
    
    # Create test data
    test_data = pd.DataFrame({
        'unique_id': [1, 2, 3],
        'timestamp': pd.to_datetime(['2025-12-02 10:00:00', '2025-12-02 10:01:00', '2025-12-02 10:02:00']),
        'ahur_temperature': [23.5, 23.6, 23.7],
        'ahur_humidity': [65.2, 65.1, 65.0]
    })
    
    producer = create_producer()
    
    try:
        result = producer.publish_cleaned_data('type1', test_data)
        print(f"\n📊 Results: {result}")
    finally:
        producer.close()
