import oracledb
import getpass
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
from datetime import date, timedelta, datetime
import pandas as pd
import re
import sys
import os

# Add current directory to path for config import
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import PipelineConfig

# Initialize Oracle client
oracledb.init_oracle_client(lib_dir=PipelineConfig.ORACLE_CLIENT_PATH)

def get_tables_from_config_csv(file_path=None):
    """
    Reads the provided CSV manifest to get the list of tables for a specific BIOMNAME.
    """
    if file_path is None:
        file_path = PipelineConfig.TABLE_CONFIG_CSV
    
    print(f"--- Reading table configuration from: {file_path} ---")
    try:
        df_config = pd.read_csv(file_path)
        
        # Filter for rows where BIOMNAME is 'Rainforest'
        rainforest_tables_df = df_config[df_config['BIOMNAME'] == 'Rainforest']
        
        # Get the list of table names from the 'TABLE_NAME' column
        tables = rainforest_tables_df['TABLE_NAME'].tolist()
        
        print(f"Found {len(tables)} 'Rainforest' tables in the configuration file.")
        return tables
    except FileNotFoundError:
        print(f"ERROR: The configuration file was not found at '{file_path}'")
        return []
    except Exception as e:
        print(f"ERROR: Could not read or process the configuration file. {e}")
        return []

def extract_and_stage_data(connection, tables, schema_name):
    """
    Extracts data incrementally, generates a table-specific unique and sequential ID,
    maintains a 30-day rolling window in MySQL, and renames the 'value' column.
    Each table gets its own unique_id sequence starting from 1.
    """
    mysql_engine = create_engine(PipelineConfig.get_mysql_connection_string())
    
    # Note: We now track unique_id per table instead of globally

    end_date = date.today()

    for table in tables:
        # Initialize df as empty DataFrame
        df = pd.DataFrame()
        
        # --- 1. Get the last used ID for this specific table from metadata ---
        current_id = 0
        try:
            with mysql_engine.connect() as conn:
                # First try to get from metadata table
                result = conn.execute(text("""
                    SELECT last_used_id FROM staging_metadata 
                    WHERE table_name = :table_name;
                """), {'table_name': table.lower()}).scalar()
                
                if result is not None:
                    current_id = int(result)
                else:
                    # Fallback: get the maximum unique_id from the actual table
                    result = conn.execute(text(f"SELECT MAX(unique_id) FROM {table.lower()};")).scalar()
                    if result is not None:
                        current_id = int(result)
        except ProgrammingError:
            # Table doesn't exist yet, start from 0
            current_id = 0
        except Exception as e:
            print(f"Warning: Could not get last ID for table {table}. Starting from 0. Error: {e}")
            current_id = 0
        
        # --- 2. Determine the date range for the incremental pull ---
        start_timestamp = None
        try:
            with mysql_engine.connect() as conn:
                last_timestamp = conn.execute(text(f"SELECT MAX(timestamp) FROM {table.lower()};")).scalar()
                if last_timestamp:
                    # Add buffer seconds to avoid fetching the same data again
                    start_timestamp = last_timestamp + timedelta(seconds=PipelineConfig.TIMESTAMP_BUFFER_SECONDS)
                    print(f"    - Using table max timestamp: {last_timestamp}")
                    print(f"    - Start timestamp (max + {PipelineConfig.TIMESTAMP_BUFFER_SECONDS} seconds): {start_timestamp}")
                else:
                    # No data in table yet, use rolling window lookback
                    start_timestamp = datetime.combine(end_date - timedelta(days=PipelineConfig.ROLLING_WINDOW_DAYS), datetime.min.time())
                    print(f"    - No data in table, using {PipelineConfig.ROLLING_WINDOW_DAYS}-day lookback: {start_timestamp}")
        except ProgrammingError:
            start_timestamp = datetime.combine(end_date - timedelta(days=PipelineConfig.ROLLING_WINDOW_DAYS), datetime.min.time())
            print(f"    - Table doesn't exist, using {PipelineConfig.ROLLING_WINDOW_DAYS}-day lookback: {start_timestamp}")
        except Exception as e:
            print(f"    - Warning: Could not determine start timestamp for {table}. Using {PipelineConfig.ROLLING_WINDOW_DAYS}-day lookback. Error: {e}")
            start_timestamp = datetime.combine(end_date - timedelta(days=PipelineConfig.ROLLING_WINDOW_DAYS), datetime.min.time())
        
        print(f"\n--- Processing table: {table} ---")
        print(f"Extracting data from {start_timestamp} onward...")
        
        # --- 3. Generate unique column name ---
        # Check if table name contains 'ahur' (case insensitive)
        if 'ahur' in table.lower():
            # Find the position of 'ahur' and extract from there to the end
            ahur_position = table.lower().find('ahur')
            new_value_column_name = table[ahur_position:]  # From 'ahur' to end
        else:
            # Original logic for non-AHUR tables
            parts = table.split('_')
            last_part = parts[-1]
            if last_part[0].isdigit():
                new_value_column_name = '_'.join(parts[-2:])
            else:
                new_value_column_name = last_part
        
        # Clean the column name (remove special characters)
        new_value_column_name = re.sub(r'[^a-zA-Z0-9_]', '_', new_value_column_name)
        print(f"Start timestamp: {start_timestamp}")
        print(f"End date: {end_date}")
        # --- 4. Query Oracle for new data ---
        sql_query = f"""
            SELECT timestamp, ROUND(value,2) AS "{new_value_column_name}" 
            FROM {schema_name}.{table}
            WHERE timestamp > TO_DATE('{start_timestamp}', 'YYYY-MM-DD HH24:MI:SS')
              AND timestamp < TO_DATE('{end_date + timedelta(days=1)}', 'YYYY-MM-DD')
              ORDER BY timestamp
        """
        try:
            df = pd.read_sql(sql_query, connection)
            print(df.info())
            if not df.empty:
                # --- 5. Generate the new Unique ID ---
                df.columns = [c.lower() for c in df.columns]
                # Create a simple, incrementing integer ID
                df.insert(0, 'unique_id', range(current_id + 1, current_id + 1 + len(df)))
                                
                # --- 6. Append new data ---
                df.to_sql(table.lower(), mysql_engine, if_exists='append', index=False)
                print(f"    - Appended {len(df)} new rows to MySQL table '{table.lower()}'")
            else:
                print(f"    - No new data found for the date range.")
            
            # --- 7. Always maintain rolling window (regardless of new data) ---
            rolling_window_ago = date.today() - timedelta(days=PipelineConfig.ROLLING_WINDOW_DAYS)
            purge_sql = text(f"DELETE FROM {table.lower()} WHERE timestamp < '{rolling_window_ago}';")
            with mysql_engine.connect() as conn:
                result = conn.execute(purge_sql)
                conn.commit()
                if result.rowcount > 0:
                    print(f"    - Purged {result.rowcount} rows older than {rolling_window_ago}.")
                else:
                    print(f"    - No rows older than {rolling_window_ago} to purge.")

        except Exception as e:
            print(f"    - FAILED to process table {table}. Error: {e}")
            # Set df to empty DataFrame so metadata update still happens
            df = pd.DataFrame()
        
        # --- 8. Update staging metadata for this table (ALWAYS runs, even if Oracle processing failed) ---
        try:
            # Create a dedicated connection for metadata operations
            metadata_conn = mysql_engine.connect()
            try:
                # Create metadata table if it doesn't exist
                print(f"    - Creating/checking staging_metadata table...")
                metadata_conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS staging_metadata (
                        table_name VARCHAR(255) PRIMARY KEY,
                        last_run_date DATE,
                        last_run_timestamp DATETIME,
                        last_used_id BIGINT
                    );
                """))
                
                # Update or insert metadata for this table
                current_timestamp = datetime.now()
                
                metadata_values = {
                    'table_name': table.lower(),
                    'last_run_date': date.today(),
                    'last_run_timestamp': current_timestamp,
                    'last_used_id': current_id + (len(df) if not df.empty else 0)
                }
                
                print(f"    - Inserting metadata: {metadata_values}")
                result = metadata_conn.execute(text("""
                    INSERT INTO staging_metadata (table_name, last_run_date, last_run_timestamp, last_used_id) 
                    VALUES (:table_name, :last_run_date, :last_run_timestamp, :last_used_id)
                    ON DUPLICATE KEY UPDATE 
                    last_run_date = VALUES(last_run_date),
                    last_run_timestamp = VALUES(last_run_timestamp),
                    last_used_id = VALUES(last_used_id);
                """), metadata_values)
                
                # Commit the transaction explicitly
                metadata_conn.commit()
                print(f"    - Metadata insert/update result: {result.rowcount} rows affected")
                
                print(f"    - Updated metadata for {table}: last_run_timestamp={current_timestamp}, last_used_id={current_id + (len(df) if not df.empty else 0)}")
                
            finally:
                metadata_conn.close()
                
        except Exception as e:
            print(f"    - ERROR: Could not update metadata for {table}. {e}")
            import traceback
            traceback.print_exc()
        
        # Update current_id only if we successfully processed data
        if not df.empty:
            current_id += len(df)
            print(f"    - Updated unique_id range for {table}: 1 to {current_id}")
    
    print(f"\nPipeline run complete. Each table has its own unique_id sequence and metadata tracking.")

if __name__ == "__main__":
    # Get Oracle password if not already set
    if PipelineConfig.ORACLE_PASSWORD is None:
        pw = getpass.getpass(f"Enter Oracle password for user '{PipelineConfig.ORACLE_USER}': ")
        PipelineConfig.set_oracle_password(pw)
    
    # Phase 1: Get the list of tables from our config file
    rainforest_tables = get_tables_from_config_csv()
    
    if rainforest_tables:
        try:
            # Phase 2: Connect to Oracle and run the extraction
            dsn = PipelineConfig.get_oracle_dsn()
            with oracledb.connect(user=PipelineConfig.ORACLE_USER, password=PipelineConfig.ORACLE_PASSWORD, dsn=dsn) as connection:
                print("\nSuccessfully connected to Oracle database.")
                extract_and_stage_data(connection, rainforest_tables, PipelineConfig.ORACLE_SCHEMA)
        except oracledb.Error as e:
            print(f"\nError connecting to or interacting with Oracle database: {e}")