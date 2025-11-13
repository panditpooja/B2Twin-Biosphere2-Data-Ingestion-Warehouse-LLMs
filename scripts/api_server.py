"""
Biosphere Pipeline API Server
=============================
FastAPI server for accessing joined rainforest tables data
"""

from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, text
import pandas as pd
import os
import sys
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import logging

# Add current directory to path for config import
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import PipelineConfig

# Configure logging
logging.basicConfig(
    level=getattr(logging, PipelineConfig.LOG_LEVEL),
    format=PipelineConfig.LOG_FORMAT
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Biosphere Pipeline API",
    description="API for accessing joined rainforest tables data",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection
engine = None

def get_database_connection():
    """Get database connection"""
    global engine
    if engine is None:
        engine = create_engine(PipelineConfig.get_mysql_connection_string())
    return engine

# Available table categories
TABLE_CATEGORIES = ['type1', 'type2', 'less50', 'between50and100', 'other']

def get_timestamp_column_name(category: str, engine) -> str:
    """Get the correct timestamp column name for a given category"""
    table_name = f"joined_rainforest_ids_{category}"
    
    try:
        with engine.connect() as conn:
            # Get all column names for the table
            columns_query = f"""
                SELECT COLUMN_NAME
                FROM information_schema.columns 
                WHERE table_schema = '{PipelineConfig.MYSQL_DB}' 
                AND table_name = '{table_name}'
                ORDER BY ORDINAL_POSITION
            """
            result = conn.execute(text(columns_query))
            columns = [row[0] for row in result.fetchall()]
            
            # For type1 and type2, use 'timestamp'
            if category in ['type1', 'type2']:
                if 'timestamp' in columns:
                    return 'timestamp'
                else:
                    # Fallback: find first timestamp column
                    timestamp_cols = [col for col in columns if 'timestamp' in col.lower()]
                    if timestamp_cols:
                        return timestamp_cols[0]
            
            # For less50, between50and100, other - find the first timestamp column
            else:
                timestamp_cols = [col for col in columns if col.startswith('timestamp_')]
                if timestamp_cols:
                    return timestamp_cols[0]
                elif 'timestamp' in columns:
                    return 'timestamp'
            
            # If no timestamp column found, return the first column (fallback)
            if columns:
                return columns[0]
            else:
                return 'unique_id'  # Ultimate fallback
                
    except Exception as e:
        logger.warning(f"Error getting timestamp column for {category}: {e}")
        return 'unique_id'  # Fallback

@app.on_event("startup")
async def startup_event():
    """Initialize database connection on startup"""
    try:
        get_database_connection()
        logger.info("Database connection initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database connection: {e}")

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Biosphere Pipeline API",
        "version": "1.0.0",
        "available_endpoints": {
            "tables": "/tables",
            "data": "/data/{category}",
            "health": "/health"
        },
        "table_categories": TABLE_CATEGORIES
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        engine = get_database_connection()
        # Test database connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Database connection failed: {str(e)}")

@app.get("/tables")
async def get_available_tables():
    """Get list of available joined tables"""
    try:
        engine = get_database_connection()
        tables_info = []
        
        for category in TABLE_CATEGORIES:
            table_name = f"joined_rainforest_ids_{category}"
            try:
                with engine.connect() as conn:
                    # Check if table exists and get basic info
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) as row_count 
                        FROM information_schema.tables 
                        WHERE table_schema = '{PipelineConfig.MYSQL_DB}' 
                        AND table_name = '{table_name}'
                    """))
                    
                    table_exists = result.fetchone()[0] > 0
                    
                    if table_exists:
                        # Get column information
                        columns_result = conn.execute(text(f"""
                            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                            FROM information_schema.columns 
                            WHERE table_schema = '{PipelineConfig.MYSQL_DB}' 
                            AND table_name = '{table_name}'
                            ORDER BY ORDINAL_POSITION
                        """))
                        
                        columns = [{"name": row[0], "type": row[1], "nullable": row[2]} for row in columns_result]
                        
                        # Get row count
                        count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                        row_count = count_result.fetchone()[0]
                        
                        tables_info.append({
                            "category": category,
                            "table_name": table_name,
                            "row_count": row_count,
                            "columns": columns,
                            "available": True
                        })
                    else:
                        tables_info.append({
                            "category": category,
                            "table_name": table_name,
                            "row_count": 0,
                            "columns": [],
                            "available": False
                        })
                        
            except Exception as e:
                logger.warning(f"Error checking table {table_name}: {e}")
                tables_info.append({
                    "category": category,
                    "table_name": table_name,
                    "row_count": 0,
                    "columns": [],
                    "available": False,
                    "error": str(e)
                })
        
        return {"tables": tables_info}
        
    except Exception as e:
        logger.error(f"Error getting tables info: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get tables info: {str(e)}")

@app.get("/data/{category}")
async def get_table_data(
    category: str = Path(..., description="Table category (type1, type2, less50, between50and100, other)"),
    limit: Optional[int] = Query(100, ge=1, le=10000, description="Number of records to return"),
    offset: Optional[int] = Query(0, ge=0, description="Number of records to skip"),
    start_date: Optional[str] = Query(None, description="Start date filter (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date filter (YYYY-MM-DD)"),
    unique_id: Optional[int] = Query(None, description="Filter by specific unique_id")
):
    """Get data from a specific joined table category"""
    
    if category not in TABLE_CATEGORIES:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid category. Must be one of: {TABLE_CATEGORIES}"
        )
    
    try:
        engine = get_database_connection()
        table_name = f"joined_rainforest_ids_{category}"
        
        # Get the correct timestamp column name for this category
        timestamp_column = get_timestamp_column_name(category, engine)
        
        # Build query
        base_query = f"SELECT * FROM {table_name}"
        where_conditions = []
        params = {}
        
        # Add filters
        if start_date:
            where_conditions.append(f"DATE({timestamp_column}) >= :start_date")
            params["start_date"] = start_date
            
        if end_date:
            where_conditions.append(f"DATE({timestamp_column}) <= :end_date")
            params["end_date"] = end_date
            
        if unique_id is not None:
            where_conditions.append("unique_id = :unique_id")
            params["unique_id"] = unique_id
        
        # Add WHERE clause if conditions exist
        if where_conditions:
            base_query += " WHERE " + " AND ".join(where_conditions)
        
        # Add ordering and pagination
        base_query += f" ORDER BY {timestamp_column} DESC LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset
        
        # Execute query
        with engine.connect() as conn:
            result = conn.execute(text(base_query), params)
            rows = result.fetchall()
            
            # Get column names
            columns = result.keys()
            
            # Convert to list of dictionaries
            data = [dict(zip(columns, row)) for row in rows]
            
            # Get total count for pagination
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            if where_conditions:
                count_query += " WHERE " + " AND ".join(where_conditions)
            
            count_result = conn.execute(text(count_query), {k: v for k, v in params.items() if k in ["start_date", "end_date", "unique_id"]})
            total_count = count_result.fetchone()[0]
            
            return {
                "category": category,
                "table_name": table_name,
                "data": data,
                "pagination": {
                    "limit": limit,
                    "offset": offset,
                    "total_count": total_count,
                    "has_more": offset + limit < total_count
                },
                "filters_applied": {
                    "start_date": start_date,
                    "end_date": end_date,
                    "unique_id": unique_id
                }
            }
            
    except Exception as e:
        logger.error(f"Error getting data for category {category}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get data: {str(e)}")

@app.get("/data/{category}/stats")
async def get_table_stats(
    category: str = Path(..., description="Table category")
):
    """Get statistical information about a specific table"""
    
    if category not in TABLE_CATEGORIES:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid category. Must be one of: {TABLE_CATEGORIES}"
        )
    
    try:
        engine = get_database_connection()
        table_name = f"joined_rainforest_ids_{category}"
        
        with engine.connect() as conn:
            # Get the correct timestamp column name
            timestamp_column = get_timestamp_column_name(category, engine)
            
            # Get basic stats
            stats_query = f"""
                SELECT 
                    COUNT(*) as total_records,
                    MIN({timestamp_column}) as earliest_timestamp,
                    MAX({timestamp_column}) as latest_timestamp,
                    COUNT(DISTINCT unique_id) as unique_ids_count
                FROM {table_name}
            """
            
            stats_result = conn.execute(text(stats_query))
            stats = dict(zip(stats_result.keys(), stats_result.fetchone()))
            
            # Get column statistics for numeric columns
            columns_query = f"""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM information_schema.columns 
                WHERE table_schema = '{PipelineConfig.MYSQL_DB}' 
                AND table_name = '{table_name}'
                AND DATA_TYPE IN ('int', 'bigint', 'float', 'double', 'decimal')
                AND COLUMN_NAME NOT IN ('unique_id')
            """
            
            columns_result = conn.execute(text(columns_query))
            numeric_columns = [row[0] for row in columns_result]
            
            column_stats = {}
            for col in numeric_columns:
                try:
                    col_stats_query = f"""
                        SELECT 
                            MIN({col}) as min_value,
                            MAX({col}) as max_value,
                            AVG({col}) as avg_value,
                            COUNT({col}) as non_null_count
                        FROM {table_name}
                        WHERE {col} IS NOT NULL
                    """
                    col_result = conn.execute(text(col_stats_query))
                    col_stats = dict(zip(col_result.keys(), col_result.fetchone()))
                    column_stats[col] = col_stats
                except Exception as e:
                    logger.warning(f"Error getting stats for column {col}: {e}")
                    column_stats[col] = {"error": str(e)}
            
            return {
                "category": category,
                "table_name": table_name,
                "basic_stats": stats,
                "column_stats": column_stats,
                "numeric_columns": numeric_columns
            }
            
    except Exception as e:
        logger.error(f"Error getting stats for category {category}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")

@app.get("/data/{category}/unique_ids")
async def get_unique_ids(
    category: str = Path(..., description="Table category"),
    limit: Optional[int] = Query(100, ge=1, le=1000, description="Number of unique IDs to return")
):
    """Get list of unique IDs in a specific table"""
    
    if category not in TABLE_CATEGORIES:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid category. Must be one of: {TABLE_CATEGORIES}"
        )
    
    try:
        engine = get_database_connection()
        table_name = f"joined_rainforest_ids_{category}"
        
        query = f"""
            SELECT DISTINCT unique_id 
            FROM {table_name} 
            ORDER BY unique_id 
            LIMIT :limit
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), {"limit": limit})
            unique_ids = [row[0] for row in result.fetchall()]
            
            return {
                "category": category,
                "unique_ids": unique_ids,
                "count": len(unique_ids)
            }
            
    except Exception as e:
        logger.error(f"Error getting unique IDs for category {category}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get unique IDs: {str(e)}")

@app.get("/data/{category}/time_range")
async def get_time_range(
    category: str = Path(..., description="Table category")
):
    """Get the time range covered by a specific table"""
    
    if category not in TABLE_CATEGORIES:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid category. Must be one of: {TABLE_CATEGORIES}"
        )
    
    try:
        engine = get_database_connection()
        table_name = f"joined_rainforest_ids_{category}"
        
        # Get the correct timestamp column name
        timestamp_column = get_timestamp_column_name(category, engine)
        
        query = f"""
            SELECT 
                MIN({timestamp_column}) as start_time,
                MAX({timestamp_column}) as end_time,
                COUNT(*) as total_records,
                COUNT(DISTINCT DATE({timestamp_column})) as unique_days
            FROM {table_name}
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            time_info = dict(zip(result.keys(), result.fetchone()))
            
            return {
                "category": category,
                "time_range": time_info
            }
            
    except Exception as e:
        logger.error(f"Error getting time range for category {category}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get time range: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
