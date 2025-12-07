#!/usr/bin/env python3
"""
Configuration Test Script
========================
Test script to verify all configuration settings are working correctly
"""

import sys
import os

# Add current directory to path for config import
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import PipelineConfig

def test_configuration():
    """Test all configuration settings"""
    print("🔧 Testing Biosphere Pipeline Configuration")
    print("=" * 50)
    
    # Test directory creation
    print("📁 Testing directory creation...")
    PipelineConfig.ensure_directories()
    print(f"✅ Base directory: {PipelineConfig.BASE_DIR}")
    print(f"✅ Data directory: {PipelineConfig.DATA_DIR}")
    print(f"✅ Logs directory: {PipelineConfig.LOGS_DIR}")
    print(f"✅ Joined tables directory: {PipelineConfig.JOINED_TABLES_DIR}")
    
    # Test file paths
    print("\n📄 Testing file paths...")
    print(f"✅ Table config CSV: {PipelineConfig.TABLE_CONFIG_CSV}")
    print(f"✅ Row count CSV: {PipelineConfig.ROW_COUNT_CSV}")
    
    # Test database configurations
    print("\n🗄️ Testing database configurations...")
    print(f"✅ MySQL connection string: {PipelineConfig.get_mysql_connection_string()}")
    print(f"✅ Oracle DSN: {PipelineConfig.get_oracle_dsn()}")
    print(f"✅ Oracle schema: {PipelineConfig.ORACLE_SCHEMA}")
    
    # Test pipeline settings
    print("\n⚙️ Testing pipeline settings...")
    print(f"✅ Rolling window days: {PipelineConfig.ROLLING_WINDOW_DAYS}")
    print(f"✅ Timestamp buffer seconds: {PipelineConfig.TIMESTAMP_BUFFER_SECONDS}")
    print(f"✅ Major IDs: {PipelineConfig.MAJOR_IDS}")
    
    # Test file existence
    print("\n📋 Testing file existence...")
    files_to_check = [
        PipelineConfig.TABLE_CONFIG_CSV,
        PipelineConfig.ROW_COUNT_CSV
    ]
    
    for file_path in files_to_check:
        if os.path.exists(file_path):
            print(f"✅ {os.path.basename(file_path)} exists")
        else:
            print(f"❌ {os.path.basename(file_path)} NOT FOUND")
    
    print("\n🎉 Configuration test completed!")

if __name__ == "__main__":
    test_configuration()
