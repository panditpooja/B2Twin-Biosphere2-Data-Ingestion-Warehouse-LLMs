"""
Simple Consumer Example
========================
Reads messages from Kafka and displays them.
Good starting point for testing and understanding the data format.
"""

import logging
import json
from typing import Dict, Any, List
import sys
import os

# Add parent directories to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.config import PipelineConfig
from streaming.consumers.base_consumer import BaseKafkaConsumer

logger = logging.getLogger(__name__)


class SimpleDisplayConsumer(BaseKafkaConsumer):
    """
    Simple consumer that displays incoming sensor data.
    Useful for monitoring and debugging.
    """
    
    def __init__(self, topics: List[str] = None):
        """
        Initialize display consumer.
        
        Args:
            topics: List of topics to subscribe to (default: all)
        """
        if topics is None:
            topics = list(PipelineConfig.KAFKA_TOPICS.values())
        
        super().__init__(
            topics=topics,
            group_id=f"{PipelineConfig.KAFKA_CONSUMER_GROUP_PREFIX}_display"
        )
        self.message_count = 0
    
    def process_message(self, message: Dict[str, Any]) -> bool:
        """Display the message in a nice format"""
        try:
            self.message_count += 1
            
            print("\n" + "=" * 70)
            print(f"📨 MESSAGE #{self.message_count}")
            print("=" * 70)
            
            # Basic info
            print(f"Event ID:    {message.get('event_id')}")
            print(f"Category:    {message.get('category')}")
            print(f"Unique ID:   {message.get('unique_id')}")
            
            # Timestamp
            if 'timestamp' in message:
                print(f"Timestamp:   {message.get('timestamp')}")
            
            # Sensors
            sensors = message.get('sensors', {})
            print(f"\nSensor Data: ({len(sensors)} sensors)")
            print("-" * 70)
            
            for sensor_name, sensor_value in sensors.items():
                if isinstance(sensor_value, dict):
                    # 'other' category format
                    value = sensor_value.get('value')
                    timestamp = sensor_value.get('timestamp')
                    print(f"  {sensor_name:40} {value:>10} @ {timestamp}")
                else:
                    # type1/type2 format
                    print(f"  {sensor_name:40} {sensor_value:>10}")
            
            # Metadata
            metadata = message.get('metadata', {})
            print(f"\nMetadata:")
            print(f"  Processed at:   {metadata.get('processed_at')}")
            print(f"  Pipeline ver:   {metadata.get('pipeline_version')}")
            print(f"  Source:         {metadata.get('source')}")
            if 'table_count' in metadata:
                print(f"  Table count:    {metadata.get('table_count')}")
            
            print("=" * 70)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error displaying message: {e}", exc_info=True)
            return False


def main():
    """Run the display consumer"""
    logging.basicConfig(
        level=logging.WARNING,  # Set to WARNING to reduce noise
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print("=" * 70)
    print("KAFKA MESSAGE VIEWER")
    print("=" * 70)
    print("Listening for sensor data from all topics...")
    print("Press Ctrl+C to stop")
    print("=" * 70)
    
    # Create consumer
    consumer = SimpleDisplayConsumer()
    
    try:
        # Start consuming
        consumer.start()
    except KeyboardInterrupt:
        print("\n\n⚠️  Stopped by user")
    finally:
        consumer.close()
        print("\n👋 Viewer closed")


if __name__ == "__main__":
    main()
