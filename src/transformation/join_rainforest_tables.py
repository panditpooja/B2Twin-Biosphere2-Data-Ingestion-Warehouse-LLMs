import pandas as pd
from sqlalchemy import create_engine, text, inspect
from datetime import datetime
import os
import sys
import logging

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from config.config import PipelineConfig

# Import Kafka producer
try:
    sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    from streaming.kafka_producer import BiosphereKafkaProducer
    KAFKA_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Kafka producer not available: {e}")
    KAFKA_AVAILABLE = False

logger = logging.getLogger(__name__)

# Create MySQL connection
def create_mysql_connection():
    """Create MySQL database connection"""
    engine = create_engine(PipelineConfig.get_mysql_connection_string())
    return engine

def create_categories_table():
    """Fetch tables with specific IDs and perform inner joins using unique_id"""
    major_ids = PipelineConfig.MAJOR_IDS
    
    # Create database connection
    engine = create_mysql_connection()
    
    # Initialize Kafka producer if enabled
    kafka_producer = None
    if KAFKA_AVAILABLE and PipelineConfig.KAFKA_ENABLE_PRODUCER:
        try:
            kafka_producer = BiosphereKafkaProducer()
            print("✅ Kafka producer initialized")
        except Exception as e:
            print(f"⚠️  Warning: Could not initialize Kafka producer: {e}")
            print("   Continuing without Kafka publishing...")
    
    # Read the table configuration CSV
    df = pd.read_csv(PipelineConfig.ROW_COUNT_CSV)
    
    # Process each ID category
    for ids in major_ids:
        print(f"\nProcessing tables with ID: {ids}")
        
        # Get tables with this specific ID
        df_ids = df[df[PipelineConfig.ID_COLUMN_NAME] == ids]
        table_names = df_ids['Table Name'].tolist()
        
        if not table_names:
            print(f"No tables found with ID {ids}")
            continue
            
        print(f"Found {len(table_names)} tables: {table_names}")
        
        # Fetch and join tables with the same ID
        joined_data = None
        
        for i, table_name in enumerate(table_names):
            try:
                # Read the table from database
                print(f"Reading table: {table_name}")
                df_table = pd.read_sql_table(table_name.strip().lower(), engine)
                
                if i == 0:
                    # First table becomes the base - keep unique_id, timestamp, and 3rd column
                    # Get column names excluding timestamp and unique_id
                    columns_to_exclude = ['timestamp', 'unique_id']
                    available_columns = [col for col in df_table.columns if col.lower() not in [c.lower() for c in columns_to_exclude]]
                    
                    if len(available_columns) >= 1:
                        # Select unique_id, timestamp, and the 3rd column (or first available column)
                        third_column = available_columns[0]  # First available column after excluding timestamp and unique_id
                        joined_data = df_table[['unique_id', 'timestamp', third_column]].copy()
                        
                        # For 'other' type, rename timestamp column to include table name for traceability
                        if ids in ['less50', 'between50and100', 'other']:
                            joined_data = joined_data.rename(columns={
                                'timestamp': f'timestamp_{third_column}'
                            })
                        
                        print(f"Base table: {table_name} with {len(joined_data)} rows")
                        if ids in ['less50', 'between50and100', 'other']:
                            print(f"Base columns: unique_id, timestamp_{third_column}, {third_column}")
                        else:
                            print(f"Base columns: unique_id, timestamp, {third_column}")
                    else:
                        print(f"Warning: No value columns found in {table_name}, skipping")
                        continue
                else:
                    # For subsequent tables, select only the 3rd column (excluding timestamp and unique_id)
                    if len(df_table.columns) >= 3:
                        # Get column names excluding timestamp and unique_id
                        columns_to_exclude = ['timestamp', 'unique_id']
                        available_columns = [col for col in df_table.columns if col.lower() not in [c.lower() for c in columns_to_exclude]]
                        
                        if len(available_columns) >= 1:
                            third_column = available_columns[0]  # First available column after excluding timestamp and unique_id
                            
                            # For 'other' type, include timestamp; for type1/type2, exclude timestamp
                            if ids in ['less50', 'between50and100', 'other']:
                                df_table_selected = df_table[['unique_id', 'timestamp', third_column]].copy()
                                # Rename timestamp column to include table name for traceability
                                df_table_selected = df_table_selected.rename(columns={
                                    'timestamp': f'timestamp_{third_column}'
                                })
                            else:
                                df_table_selected = df_table[['unique_id', third_column]].copy()
                            
                            # Perform join with subsequent tables using unique_id
                            # Use inner join for type1/type2 (consistent row counts) and full outer join for other (variable row counts)
                            if 'unique_id' in df_table_selected.columns and 'unique_id' in joined_data.columns:
                                join_type = 'outer' if ids in ['less50', 'between50and100', 'other'] else 'inner'
                                rows_before_join = len(df_table_selected)
                                joined_data = pd.merge(joined_data, df_table_selected, on='unique_id', how=join_type)
                                rows_after_join = len(joined_data)
                                
                                print(f"Joined with {table_name}: {rows_before_join} → {rows_after_join} rows after {join_type} join")
                            else:
                                print(f"Warning: unique_id column not found in {table_name}, skipping join")
                        else:
                            print(f"Warning: No value columns found in {table_name} (excluding timestamp/unique_id), skipping join")
                    else:
                        print(f"Warning: {table_name} has less than 3 columns, skipping join")
                        
            except Exception as e:
                print(f"Error processing table {table_name}: {str(e)}")
                continue
        
        # Save the joined result
        if joined_data is not None and not joined_data.empty:
            output_filename = f"joined_tables_ids_{ids}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            output_path = os.path.join(PipelineConfig.JOINED_TABLES_DIR, output_filename)
            
            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Save to CSV
            joined_data.to_csv(output_path, index=False)
            print(f"Saved joined data to: {output_path}")
            print(f"Final joined table has {len(joined_data)} rows and {len(joined_data.columns)} columns")
            
            # Also save to database as a new table
            table_name = f"joined_rainforest_ids_{ids}"
            try:
                joined_data.to_sql(table_name, engine, if_exists='replace', index=False)
                print(f"Saved joined data to database table: {table_name}")
            except Exception as e:
                print(f"Error saving to database: {str(e)}")
            
            # 🆕 NEW: Publish to Kafka
            if kafka_producer:
                try:
                    print(f"\n📤 Publishing to Kafka topic for category '{ids}'...")
                    result = kafka_producer.publish_cleaned_data(
                        category=ids,
                        data=joined_data,
                        metadata={
                            "table_count": len(table_names),
                            "source_tables": table_names,
                            "csv_file": output_filename,
                            "db_table": table_name
                        }
                    )
                    print(f"   ✅ Kafka publish complete: {result['success']} success, {result['failed']} failed")
                except Exception as e:
                    print(f"   ⚠️  Warning: Kafka publish failed: {e}")
                    print(f"   Continuing with pipeline...")
        else:
            print(f"No data to save for ID {ids}")
    
    # Close Kafka producer
    if kafka_producer:
        kafka_producer.close()
        print("\n🔒 Kafka producer closed")

if __name__ == "__main__":
    create_categories_table()

