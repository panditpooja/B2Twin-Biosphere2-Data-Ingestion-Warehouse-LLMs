"""
Pipeline Monitoring Dashboard
============================
Monitor the health and performance of the Biosphere pipeline
"""

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import sys
import os

# Add config to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import PipelineConfig

def create_mysql_connection():
    """Create MySQL database connection"""
    engine = create_engine(PipelineConfig.get_mysql_connection_string())
    return engine

def get_pipeline_status():
    """Get overall pipeline status"""
    engine = create_mysql_connection()
    
    with engine.connect() as conn:
        # Get metadata information
        metadata_query = """
            SELECT 
                table_name,
                last_run_date,
                last_run_timestamp,
                last_used_id
            FROM staging_metadata 
            ORDER BY last_run_timestamp DESC
        """
        
        metadata_df = pd.read_sql(metadata_query, conn)
        
        # Get table row counts
        tables_query = """
            SELECT 
                TABLE_NAME,
                TABLE_ROWS
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = 'biosphere_staging'
            AND TABLE_NAME NOT LIKE 'staging_%'
            ORDER BY TABLE_ROWS DESC
        """
        
        tables_df = pd.read_sql(tables_query, conn)
        
        # Get joined tables status
        joined_tables_query = """
            SELECT 
                TABLE_NAME,
                TABLE_ROWS
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = 'biosphere_staging'
            AND TABLE_NAME LIKE 'joined_rainforest_%'
            ORDER BY TABLE_ROWS DESC
        """
        
        joined_df = pd.read_sql(joined_tables_query, conn)
        
    return metadata_df, tables_df, joined_df

def print_dashboard():
    """Print a formatted dashboard"""
    print("=" * 80)
    print("🌿 BIOSPHERE PIPELINE MONITORING DASHBOARD")
    print("=" * 80)
    print(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        metadata_df, tables_df, joined_df = get_pipeline_status()
        
        # Pipeline Status
        print("📊 PIPELINE STATUS")
        print("-" * 40)
        if not metadata_df.empty:
            latest_run = metadata_df['last_run_timestamp'].max()
            print(f"Latest run: {latest_run}")
            print(f"Tables processed: {len(metadata_df)}")
        else:
            print("No pipeline runs found")
        print()
        
        # Source Tables Status
        print("📋 SOURCE TABLES STATUS")
        print("-" * 40)
        if not tables_df.empty:
            print(f"Total tables: {len(tables_df)} excluding staging_metadata table.")
            # print(f"Total rows: {tables_df['TABLE_ROWS'].sum():,}")
            # print("\nTop 10 tables by row count:")
            # for _, row in tables_df.head(10).iterrows():
            #     print(f"  {row['TABLE_NAME']}: {row['TABLE_ROWS']:,} rows")
        else:
            print("No source tables found")
        print()
        
        # Joined Tables Status
        print("🔗 JOINED TABLES STATUS")
        print("-" * 40)
        if not joined_df.empty:
            print(f"Joined tables: {len(joined_df)}")
            print("\nJoined tables:")
            for _, row in joined_df.iterrows():
                print(f"  {row['TABLE_NAME']}: {row['TABLE_ROWS']:,} rows")
        else:
            print("No joined tables found")
        print()
        
    except Exception as e:
        print(f"❌ Error generating dashboard: {e}")
    
    print("=" * 80)

def check_data_freshness():
    """Check if data is fresh (updated within last 24 hours)"""
    engine = create_mysql_connection()
    
    with engine.connect() as conn:
        # Check if any tables were updated in last 24 hours
        freshness_query = """
            SELECT COUNT(*) as fresh_tables
            FROM staging_metadata 
            WHERE last_run_timestamp > DATE_SUB(NOW(), INTERVAL 24 HOUR)
        """
        
        result = conn.execute(text(freshness_query)).scalar()
        
        if result > 0:
            print(f"✅ Data is fresh: {result} tables updated in last 24 hours")
            return True
        else:
            print("⚠️  Data may be stale: No tables updated in last 24 hours")
            return False

if __name__ == "__main__":
    print_dashboard()
    print()
    check_data_freshness()
