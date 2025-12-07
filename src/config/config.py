"""
Configuration management for Biosphere Pipeline
==============================================
Centralized configuration for all pipeline components
"""

import os
from datetime import timedelta

class PipelineConfig:
    """Centralized configuration for the Biosphere pipeline"""
    
    # Database configurations
    MYSQL_USER = "root"
    MYSQL_PASSWORD = "pooja"
    MYSQL_HOST = "localhost"
    MYSQL_DB = "biosphere_staging"
    
    # Oracle configurations
    ORACLE_USER = "b2twin"
    ORACLE_PASSWORD = None  # Will be set at runtime via getpass
    ORACLE_HOST = "biosphere2data-prod.c0xzlo6s7duc.us-west-2.rds.amazonaws.com"
    ORACLE_SERVICE = "bio2prd"
    ORACLE_PORT = 1521
    ORACLE_SCHEMA = "BIO2CONTROLSALL"
    
    # Oracle client path
    ORACLE_CLIENT_PATH = r"C:\Users\ual-laptop\Downloads\POC 6\biosphere_pipeline\instantclient-basic-windows.x64-23.9.0.25.07\instantclient_23_9"
    
    # File paths
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_DIR = os.path.join(BASE_DIR, "data")
    LOGS_DIR = os.path.join(BASE_DIR, "logs")
    TABLE_CONFIG_CSV = os.path.join(DATA_DIR, "tables_list", "B2 Controls History_Config_summary 16OCT25(Sheet1).csv")
    ROW_COUNT_CSV = os.path.join(DATA_DIR, "tables_list", "Table_Row_Count.csv")
    JOINED_TABLES_DIR = os.path.join(DATA_DIR, "joined_tables")
    
    # Pipeline settings
    ROLLING_WINDOW_DAYS = 30
    TIMESTAMP_BUFFER_SECONDS = 10
    MAJOR_IDS = ['type1', 'type2', 'less50', 'between50and100', 'other']
    ID_COLUMN_NAME = 'Id'  # Column name for table identifiers in CSV
    
    # Logging settings
    LOG_LEVEL = "INFO"
    LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
    
    # Kafka configurations
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC_PREFIX = "biosphere.rainforest"
    KAFKA_CONSUMER_GROUP_PREFIX = "biosphere_consumer"
    
    # Kafka topic names by category
    KAFKA_TOPICS = {
        'type1': f"{KAFKA_TOPIC_PREFIX}.type1",
        'type2': f"{KAFKA_TOPIC_PREFIX}.type2",
        'less50': f"{KAFKA_TOPIC_PREFIX}.less50",
        'between50and100': f"{KAFKA_TOPIC_PREFIX}.between50and100",
        'other': f"{KAFKA_TOPIC_PREFIX}.other"
    }
    
    # Kafka producer settings
    KAFKA_PRODUCER_CONFIG = {
        'acks': 'all',  # Wait for all replicas to acknowledge
        'retries': 3,
        'max_in_flight_requests_per_connection': 5,
        'compression_type': 'gzip',
        'linger_ms': 10,  # Batch messages for 10ms
        'batch_size': 16384,  # 16KB batches
    }
    
    # Kafka consumer settings
    KAFKA_CONSUMER_CONFIG = {
        'auto_offset_reset': 'earliest',  # Start from beginning if no offset
        'enable_auto_commit': False,  # Manual commit for reliability
        'max_poll_records': 500,
        'session_timeout_ms': 30000,
    }
    
    # Event bus settings
    KAFKA_ENABLE_PRODUCER = True  # Enable Kafka publishing
    KAFKA_ENABLE_CONSUMERS = True  # Enable Kafka consumers
    
    @classmethod
    def get_mysql_connection_string(cls):
        """Get MySQL connection string"""
        return f"mysql+mysqlconnector://{cls.MYSQL_USER}:{cls.MYSQL_PASSWORD}@{cls.MYSQL_HOST}/{cls.MYSQL_DB}"
    
    @classmethod
    def get_oracle_dsn(cls):
        """Get Oracle DSN"""
        return f"{cls.ORACLE_HOST}:{cls.ORACLE_PORT}/{cls.ORACLE_SERVICE}"
    
    @classmethod
    def set_oracle_password(cls, password):
        """Set Oracle password at runtime"""
        cls.ORACLE_PASSWORD = password
    
    @classmethod
    def ensure_directories(cls):
        """Ensure all required directories exist"""
        directories = [cls.DATA_DIR, cls.LOGS_DIR, cls.JOINED_TABLES_DIR]
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
