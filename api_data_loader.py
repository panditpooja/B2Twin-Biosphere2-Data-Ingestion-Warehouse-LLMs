"""
API Data Loader for Biosphere 2 RAG System
Fetches analyzed sensor data from FastAPI endpoint instead of CSV files
"""

import os
import requests
import json
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Default API endpoint - can be overridden via environment variable
DEFAULT_API_URL = os.getenv("FASTAPI_URL", "http://localhost:8000")
API_DATA_ENDPOINT = f"{DEFAULT_API_URL}/api/data"


def fetch_sensor_data_from_api(api_url: Optional[str] = None) -> Dict[str, Any]:
    """
    Fetch analyzed sensor data from FastAPI endpoint
    
    Args:
        api_url: Optional custom API URL. If not provided, uses DEFAULT_API_URL
                 Can be full URL including /api/data or just base URL
        
    Returns:
        Dictionary containing sensor data in the same format as load_all_sensor_data()
        
    Raises:
        requests.RequestException: If API request fails
        ValueError: If API response is invalid
    """
    # Handle different URL formats
    if api_url:
        # If URL already includes /api/data, use it directly
        if api_url.endswith('/api/data'):
            endpoint = api_url
        # If URL ends with /, append api/data
        elif api_url.endswith('/'):
            endpoint = f"{api_url}api/data"
        # Otherwise, append /api/data
        else:
            endpoint = f"{api_url}/api/data"
    else:
        endpoint = API_DATA_ENDPOINT
    
    print(f"[API] Fetching sensor data from: {endpoint}")
    
    try:
        # Make GET request to FastAPI endpoint
        # For server deployments, we may need to handle SSL verification
        verify_ssl = os.getenv("VERIFY_SSL", "true").lower() == "true"
        
        response = requests.get(
            endpoint,
            timeout=60,  # Increased timeout for server deployments
            headers={"Content-Type": "application/json"},
            verify=verify_ssl  # Allow disabling SSL verification for self-signed certs (dev only)
        )
        
        # Check if request was successful
        response.raise_for_status()
        
        # Parse JSON response
        sensor_data = response.json()
        
        print(f"[SUCCESS] Fetched data for {len(sensor_data)} sensors from API")
        
        # Validate and transform data format if needed
        # The API should return data in the same format as load_all_sensor_data()
        # But we'll ensure compatibility
        transformed_data = {}
        
        for sensor_id, data in sensor_data.items():
            # Ensure the data has the expected structure
            transformed_data[sensor_id] = {
                "total_readings": data.get("total_readings", 0),
                "time_range": data.get("time_range", "Unknown"),
                "sample_data": data.get("sample_data", []),
                "value_stats": data.get("value_stats", {}),
                "columns": data.get("columns", []),
                "filename": data.get("filename", sensor_id),
                "sensor_description": data.get("sensor_description", sensor_id)
            }
        
        return transformed_data
        
    except requests.exceptions.ConnectionError:
        error_msg = f"[ERROR] Could not connect to API at {endpoint}. Is the FastAPI server running?"
        print(error_msg)
        raise ConnectionError(error_msg)
    
    except requests.exceptions.Timeout:
        error_msg = f"[ERROR] API request timed out after 30 seconds for {endpoint}"
        print(error_msg)
        raise TimeoutError(error_msg)
    
    except requests.exceptions.HTTPError as e:
        error_msg = f"[ERROR] API returned HTTP error {e.response.status_code}: {e.response.text}"
        print(error_msg)
        raise requests.exceptions.HTTPError(error_msg)
    
    except json.JSONDecodeError as e:
        error_msg = f"[ERROR] Invalid JSON response from API: {str(e)}"
        print(error_msg)
        raise ValueError(error_msg)
    
    except Exception as e:
        error_msg = f"[ERROR] Unexpected error fetching data from API: {str(e)}"
        print(error_msg)
        raise Exception(error_msg)


def create_context_from_api_data(sensor_data: Dict[str, Any]) -> str:
    """
    Create comprehensive context from API sensor data
    Similar to create_comprehensive_context() but for API data
    
    Args:
        sensor_data: Dictionary containing sensor data from API
        
    Returns:
        Formatted context string for LLM
    """
    total_sensors = len(sensor_data)
    total_readings = sum(
        data.get('total_readings', 0) 
        for data in sensor_data.values() 
        if 'error' not in data
    )
    
    context_summary = f"""
    BIOSPHERE 2 ENVIRONMENTAL CONTROL SYSTEM ANALYSIS
    (Data sourced from FastAPI analyzed data endpoint)
    
    This analysis covers {total_sensors} different sensor systems across multiple areas:
    - Total sensors: {total_sensors}
    - Total readings: {total_readings:,}
    
    SENSOR SUMMARY:
    """
    
    # Group sensors by type/area for better organization
    sensor_list = []
    for sensor_id, data in sorted(sensor_data.items()):
        if 'error' in data:
            continue
        
        sensor_desc = data.get('sensor_description', sensor_id)
        readings = data.get('total_readings', 0)
        time_range = data.get('time_range', 'Unknown')
        
        sensor_info = f"  - {sensor_id}: {sensor_desc}\n"
        sensor_info += f"    Readings: {readings}, Time Range: {time_range}"
        
        if 'value_stats' in data and data['value_stats']:
            stats = data['value_stats']
            if stats.get('min') is not None:
                sensor_info += f"\n    Value Range: {stats['min']} to {stats['max']} (avg: {stats['mean']:.2f})"
        
        sensor_list.append(sensor_info)
    
    context_summary += "\n".join(sensor_list)
    context_summary += "\n\nSAMPLE DATA FROM KEY SENSORS:\n"
    
    # Add sample data from first 10 sensors (to avoid too much data)
    sample_count = 0
    for sensor_id, data in sorted(sensor_data.items()):
        if sample_count >= 10:
            break
        if 'error' not in data and 'sample_data' in data and data['sample_data']:
            context_summary += f"\n{sensor_id.upper()} SAMPLE:\n"
            for i, record in enumerate(data['sample_data'][:2]):  # First 2 records
                context_summary += f"  Record {i+1}: {record}\n"
            sample_count += 1
    
    return context_summary


def test_api_connection(api_url: Optional[str] = None) -> bool:
    """
    Test connection to FastAPI endpoint
    
    Args:
        api_url: Optional custom API URL
        
    Returns:
        True if connection successful, False otherwise
    """
    # Handle different URL formats
    if api_url:
        if api_url.endswith('/api/data'):
            endpoint = api_url
        elif api_url.endswith('/'):
            endpoint = f"{api_url}api/data"
        else:
            endpoint = f"{api_url}/api/data"
    else:
        endpoint = API_DATA_ENDPOINT
    
    try:
        verify_ssl = os.getenv("VERIFY_SSL", "true").lower() == "true"
        response = requests.get(endpoint, timeout=10, verify=verify_ssl)
        response.raise_for_status()
        print(f"[SUCCESS] API connection test passed: {endpoint}")
        return True
    except Exception as e:
        print(f"[ERROR] API connection test failed: {str(e)}")
        return False


if __name__ == "__main__":
    # Test the API data loader
    print("=== Testing API Data Loader ===")
    
    # Test connection
    if test_api_connection():
        print("\n[TEST] Fetching sensor data from API...")
        try:
            sensor_data = fetch_sensor_data_from_api()
            print(f"\n[SUCCESS] Retrieved {len(sensor_data)} sensors")
            
            # Show sample data
            for sensor_id, data in list(sensor_data.items())[:3]:
                print(f"\n{sensor_id}:")
                print(f"  Readings: {data.get('total_readings', 0)}")
                print(f"  Time Range: {data.get('time_range', 'Unknown')}")
            
            # Test context creation
            print("\n[TEST] Creating context from API data...")
            context = create_context_from_api_data(sensor_data)
            print(f"[SUCCESS] Context created ({len(context)} characters)")
            print("\nFirst 500 characters of context:")
            print(context[:500] + "...")
            
        except Exception as e:
            print(f"\n[ERROR] Failed to fetch data: {str(e)}")
    else:
        print("\n[ERROR] Cannot connect to API. Make sure FastAPI server is running.")
        print(f"Expected endpoint: {API_DATA_ENDPOINT}")
        print("\nTo set a custom API URL, set the FASTAPI_URL environment variable:")
        print("  export FASTAPI_URL=http://your-api-url:port")

