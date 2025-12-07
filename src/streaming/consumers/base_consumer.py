"""
Base Kafka Consumer for Biosphere Pipeline
===========================================
Abstract base class for all Kafka consumers
"""

import json
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import sys
import os

# Add parent directories to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.config import PipelineConfig

logger = logging.getLogger(__name__)


class BaseKafkaConsumer(ABC):
    """
    Abstract base class for Kafka consumers.
    
    Provides:
    - Connection management
    - Error handling
    - Offset management
    - Logging
    
    Subclasses must implement process_message() method.
    """
    
    def __init__(
        self,
        topics: List[str],
        group_id: str,
        bootstrap_servers: str = None,
        auto_commit: bool = False
    ):
        """
        Initialize base consumer.
        
        Args:
            topics: List of Kafka topics to subscribe to
            group_id: Consumer group ID
            bootstrap_servers: Kafka broker address (default from config)
            auto_commit: Whether to auto-commit offsets (default: False for manual control)
        """
        self.topics = topics
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers or PipelineConfig.KAFKA_BOOTSTRAP_SERVERS
        self.auto_commit = auto_commit
        self.consumer = None
        self.running = False
        
        self._initialize_consumer()
    
    def _initialize_consumer(self):
        """Initialize Kafka consumer with configuration"""
        try:
            consumer_config = PipelineConfig.KAFKA_CONSUMER_CONFIG.copy()
            consumer_config['enable_auto_commit'] = self.auto_commit
            
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                **consumer_config
            )
            logger.info(f"✅ Consumer '{self.group_id}' initialized")
            logger.info(f"   Subscribed to topics: {self.topics}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize consumer: {e}")
            raise
    
    @abstractmethod
    def process_message(self, message: Dict[str, Any]) -> bool:
        """
        Process a single message from Kafka.
        
        Must be implemented by subclasses.
        
        Args:
            message: Deserialized message from Kafka
        
        Returns:
            True if processing successful, False otherwise
        """
        pass
    
    def start(self, max_messages: Optional[int] = None):
        """
        Start consuming messages.
        
        Args:
            max_messages: Maximum number of messages to process (None = infinite)
        """
        if not PipelineConfig.KAFKA_ENABLE_CONSUMERS:
            logger.info("⏭️  Kafka consumers disabled in config, exiting")
            return
        
        logger.info(f"🚀 Starting consumer '{self.group_id}'...")
        self.running = True
        messages_processed = 0
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                try:
                    # Log message details
                    logger.debug(f"📨 Received message from {message.topic} [partition {message.partition}, offset {message.offset}]")
                    
                    # Process the message
                    success = self.process_message(message.value)
                    
                    if success:
                        messages_processed += 1
                        
                        # Manually commit offset if not auto-committing
                        if not self.auto_commit:
                            self.consumer.commit()
                        
                        if messages_processed % 100 == 0:
                            logger.info(f"  ✅ Processed {messages_processed} messages...")
                    else:
                        logger.warning(f"  ⚠️  Failed to process message at offset {message.offset}")
                    
                    # Check if we've hit max messages
                    if max_messages and messages_processed >= max_messages:
                        logger.info(f"  🎯 Reached max messages ({max_messages}), stopping...")
                        break
                
                except Exception as e:
                    logger.error(f"  ❌ Error processing message: {e}", exc_info=True)
                    # Continue processing next message
        
        except KeyboardInterrupt:
            logger.info("⚠️  Consumer interrupted by user")
        except Exception as e:
            logger.error(f"❌ Consumer error: {e}", exc_info=True)
        finally:
            self.running = False
            logger.info(f"🏁 Consumer stopped. Total messages processed: {messages_processed}")
    
    def stop(self):
        """Stop the consumer"""
        logger.info("🛑 Stopping consumer...")
        self.running = False
    
    def close(self):
        """Close consumer connection"""
        if self.consumer:
            self.consumer.close()
            logger.info("🔒 Consumer closed")


# Example consumer implementation
class ExampleConsumer(BaseKafkaConsumer):
    """Example consumer that just logs messages"""
    
    def __init__(self, topics: List[str]):
        super().__init__(
            topics=topics,
            group_id=f"{PipelineConfig.KAFKA_CONSUMER_GROUP_PREFIX}_example"
        )
    
    def process_message(self, message: Dict[str, Any]) -> bool:
        """Log the message"""
        try:
            logger.info(f"📊 Message received:")
            logger.info(f"   Event ID: {message.get('event_id')}")
            logger.info(f"   Category: {message.get('category')}")
            logger.info(f"   Unique ID: {message.get('unique_id')}")
            logger.info(f"   Sensors: {list(message.get('sensors', {}).keys())}")
            return True
        except Exception as e:
            logger.error(f"Error logging message: {e}")
            return False


if __name__ == "__main__":
    # Test the base consumer
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create example consumer for type1 topic
    topics = [PipelineConfig.KAFKA_TOPICS['type1']]
    consumer = ExampleConsumer(topics)
    
    try:
        # Process up to 10 messages for testing
        consumer.start(max_messages=10)
    finally:
        consumer.close()
