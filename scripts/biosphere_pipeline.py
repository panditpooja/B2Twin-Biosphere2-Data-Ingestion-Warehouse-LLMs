#!/usr/bin/env python3
"""
Biosphere Data Pipeline
=======================
Master pipeline that orchestrates the complete data flow:
1. Extract & Stage (Oracle → MySQL)
2. Transform & Aggregate (MySQL → Joined Tables)
3. Monitoring & Logging
"""

import sys
import os
import logging
from datetime import datetime
import argparse

# Add the scripts directory to the path
current_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
sys.path.append(current_dir)

from config import PipelineConfig
from bio2Oracle import extract_and_stage_data, get_tables_from_config_csv
from join_rainforest_tables import create_categories_table

# Configure logging
def setup_logging():
    """Setup comprehensive logging for the pipeline"""
    # Ensure directories exist
    PipelineConfig.ensure_directories()
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(PipelineConfig.LOGS_DIR, f"biosphere_pipeline_{timestamp}.log")
    
    # Create handlers with proper encoding
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    console_handler = logging.StreamHandler(sys.stdout)
    
    # Set formatter
    formatter = logging.Formatter(PipelineConfig.LOG_FORMAT)
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, PipelineConfig.LOG_LEVEL),
        handlers=[file_handler, console_handler]
    )
    return logging.getLogger(__name__)

def run_extraction_staging(logger):
    """Run the extraction and staging phase"""
    logger.info("=" * 60)
    logger.info("PHASE 1: EXTRACTION & STAGING")
    logger.info("=" * 60)
    
    try:
        # Import Oracle connection logic
        import oracledb
        import getpass
        
        # Get Oracle password once
        pw = getpass.getpass(f"Enter Oracle password for user '{PipelineConfig.ORACLE_USER}': ")
        PipelineConfig.set_oracle_password(pw)
        
        # Oracle connection setup
        oracledb.init_oracle_client(lib_dir=PipelineConfig.ORACLE_CLIENT_PATH)
        
        # Get tables from config
        rainforest_tables = get_tables_from_config_csv()
        
        if not rainforest_tables:
            logger.error("No tables found in configuration. Exiting.")
            return False
        
        # Connect to Oracle and run extraction
        dsn = PipelineConfig.get_oracle_dsn()
        with oracledb.connect(user=PipelineConfig.ORACLE_USER, password=PipelineConfig.ORACLE_PASSWORD, dsn=dsn) as connection:
            logger.info("Successfully connected to Oracle database.")
            extract_and_stage_data(connection, rainforest_tables, PipelineConfig.ORACLE_SCHEMA)
        
        logger.info("Extraction & Staging completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Extraction & Staging failed: {e}")
        return False

def run_transformation_aggregation(logger):
    """Run the transformation and aggregation phase"""
    logger.info("=" * 60)
    logger.info("PHASE 2: TRANSFORMATION & AGGREGATION")
    logger.info("=" * 60)
    
    try:
        create_categories_table()
        logger.info("Transformation & Aggregation completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Transformation & Aggregation failed: {e}")
        return False

def main():
    """Main pipeline orchestrator"""
    parser = argparse.ArgumentParser(description='Biosphere Data Pipeline')
    parser.add_argument('--phase', choices=['extract', 'transform', 'all'], 
                       default='all', help='Which phase to run')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Show what would be done without executing')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging()
    
    logger.info("Starting Biosphere Data Pipeline")
    logger.info(f"Phase: {args.phase}")
    logger.info(f"Dry run: {args.dry_run}")
    
    if args.dry_run:
        logger.info("DRY RUN MODE - No actual processing will occur")
        return
    
    start_time = datetime.now()
    success = True
    
    try:
        if args.phase in ['extract', 'all']:
            success = run_extraction_staging(logger)
            if not success:
                logger.error("Pipeline failed at extraction phase")
                return
        
        if args.phase in ['transform', 'all'] and success:
            success = run_transformation_aggregation(logger)
            if not success:
                logger.error("Pipeline failed at transformation phase")
                return
        
        if success:
            end_time = datetime.now()
            duration = end_time - start_time
            logger.info("Pipeline completed successfully!")
            logger.info(f"Total duration: {duration}")
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline failed with unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
