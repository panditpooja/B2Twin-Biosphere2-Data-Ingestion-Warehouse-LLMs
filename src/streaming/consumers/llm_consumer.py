"""
LLM Consumer Example
====================
Example consumer that sends sensor data to LLM for analysis.

This is a template for the LLM team to customize.
"""

import logging
from typing import Dict, Any, List
import sys
import os

# Add parent directories to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.config import PipelineConfig
from streaming.consumers.base_consumer import BaseKafkaConsumer

logger = logging.getLogger(__name__)


class LLMConsumer(BaseKafkaConsumer):
    """
    Consumer that processes sensor data and sends to LLM for analysis.
    
    This is a TEMPLATE - LLM team should customize this!
    """
    
    def __init__(self, topics: List[str] = None):
        """
        Initialize LLM consumer.
        
        Args:
            topics: List of topics to subscribe to (default: all biosphere topics)
        """
        if topics is None:
            # Subscribe to all biosphere topics by default
            topics = list(PipelineConfig.KAFKA_TOPICS.values())
        
        super().__init__(
            topics=topics,
            group_id=f"{PipelineConfig.KAFKA_CONSUMER_GROUP_PREFIX}_llm_analysis"
        )
    
    def process_message(self, message: Dict[str, Any]) -> bool:
        """
        Process sensor data and send to LLM.
        
        Args:
            message: Sensor data message from Kafka
        
        Returns:
            True if processing successful
        """
        try:
            # Extract relevant data
            category = message.get('category')
            timestamp = message.get('timestamp')
            sensors = message.get('sensors', {})
            unique_id = message.get('unique_id')
            
            logger.info(f"📊 Processing message for LLM analysis:")
            logger.info(f"   Category: {category}")
            logger.info(f"   Unique ID: {unique_id}")
            logger.info(f"   Timestamp: {timestamp}")
            logger.info(f"   Sensors: {len(sensors)} sensor readings")
            
            # ⚠️ TODO: LLM team should implement this!
            # Build prompt for LLM
            prompt = self._build_llm_prompt(message)
            logger.debug(f"   Prompt preview: {prompt[:200]}...")
            
            # ⚠️ TODO: LLM team should implement this!
            # Send to LLM API (OpenAI, Claude, etc.)
            # response = call_llm_api(prompt)
            # logger.info(f"   🤖 LLM Response: {response}")
            
            # For now, just log that we would send to LLM
            logger.info("   ⚠️  LLM API call not implemented yet (template only)")
            logger.info("   💡 LLM team: Implement call_llm_api() here!")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error processing message for LLM: {e}", exc_info=True)
            return False
    
    def _build_llm_prompt(self, message: Dict[str, Any]) -> str:
        """
        Build LLM prompt from sensor data.
        
        Args:
            message: Sensor data message
        
        Returns:
            Formatted prompt string
        """
        category = message.get('category')
        timestamp = message.get('timestamp')
        sensors = message.get('sensors', {})
        
        # Build prompt
        prompt = f"""
Biosphere 2 Rainforest Environmental Data Analysis

Data Category: {category}
Timestamp: {timestamp}

Sensor Readings:
"""
        
        # Add sensor data
        for sensor_name, sensor_value in sensors.items():
            if isinstance(sensor_value, dict):
                # For 'other' category with individual timestamps
                value = sensor_value.get('value')
                sensor_timestamp = sensor_value.get('timestamp')
                prompt += f"  - {sensor_name}: {value} (recorded at {sensor_timestamp})\n"
            else:
                # For type1/type2 with shared timestamp
                prompt += f"  - {sensor_name}: {sensor_value}\n"
        
        prompt += """

Please analyze this environmental data and provide:
1. Assessment of current conditions
2. Any unusual patterns or anomalies
3. Recommendations for biosphere operators
4. Potential concerns for ecosystem health
"""
        
        return prompt


def main():
    """Run the LLM consumer"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger.info("=" * 60)
    logger.info("LLM CONSUMER - Starting...")
    logger.info("=" * 60)
    logger.info("⚠️  NOTE: This is a TEMPLATE implementation")
    logger.info("💡 LLM team should customize the process_message() method")
    logger.info("=" * 60)
    
    # Create consumer
    consumer = LLMConsumer()
    
    try:
        # Start consuming (runs indefinitely)
        consumer.start()
    except KeyboardInterrupt:
        logger.info("\n⚠️  Stopped by user")
    finally:
        consumer.close()
        logger.info("👋 LLM consumer shut down")


if __name__ == "__main__":
    main()
