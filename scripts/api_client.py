"""
API Client for Biosphere Pipeline
=================================
Simple client to test the API endpoints
"""

import requests
import json
from datetime import datetime, timedelta

API_BASE_URL = "http://localhost:8000"

def test_api_endpoints():
    """Test all API endpoints"""
    
    print("Testing Biosphere Pipeline API...")
    print("=" * 50)
    
    # Test root endpoint
    print("\n1. Testing root endpoint...")
    try:
        response = requests.get(f"{API_BASE_URL}/")
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
    except Exception as e:
        print(f"Error: {e}")
    
    # Test health check
    print("\n2. Testing health check...")
    try:
        response = requests.get(f"{API_BASE_URL}/health")
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
    except Exception as e:
        print(f"Error: {e}")
    
    # Test tables endpoint
    print("\n3. Testing tables endpoint...")
    try:
        response = requests.get(f"{API_BASE_URL}/tables")
        print(f"Status: {response.status_code}")
        tables_data = response.json()
        print(f"Available tables: {len(tables_data['tables'])}")
        for table in tables_data['tables']:
            print(f"  - {table['category']}: {table['row_count']} rows, Available: {table['available']}")
    except Exception as e:
        print(f"Error: {e}")
    
    # Test data endpoints for each category
    categories = ['type1', 'type2', 'less50', 'between50and100', 'other']
    
    for category in categories:
        print(f"\n4. Testing data endpoint for {category}...")
        try:
            # Test basic data retrieval
            response = requests.get(f"{API_BASE_URL}/data/{category}?limit=5")
            print(f"Status: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"  Records returned: {len(data['data'])}")
                print(f"  Total records: {data['pagination']['total_count']}")
                if data['data']:
                    print(f"  Sample columns: {list(data['data'][0].keys())[:5]}...")
            else:
                print(f"  Error: {response.text}")
        except Exception as e:
            print(f"  Error: {e}")
        
        # Test stats endpoint
        print(f"\n5. Testing stats endpoint for {category}...")
        try:
            response = requests.get(f"{API_BASE_URL}/data/{category}/stats")
            print(f"Status: {response.status_code}")
            if response.status_code == 200:
                stats = response.json()
                print(f"  Total records: {stats['basic_stats']['total_records']}")
                print(f"  Time range: {stats['basic_stats']['earliest_timestamp']} to {stats['basic_stats']['latest_timestamp']}")
                print(f"  Unique IDs: {stats['basic_stats']['unique_ids_count']}")
            else:
                print(f"  Error: {response.text}")
        except Exception as e:
            print(f"  Error: {e}")
        
        # Test unique IDs endpoint
        print(f"\n6. Testing unique IDs endpoint for {category}...")
        try:
            response = requests.get(f"{API_BASE_URL}/data/{category}/unique_ids?limit=10")
            print(f"Status: {response.status_code}")
            if response.status_code == 200:
                ids_data = response.json()
                print(f"  Sample unique IDs: {ids_data['unique_ids'][:5]}...")
                print(f"  Total unique IDs returned: {ids_data['count']}")
            else:
                print(f"  Error: {response.text}")
        except Exception as e:
            print(f"  Error: {e}")
        
        # Test time range endpoint
        print(f"\n7. Testing time range endpoint for {category}...")
        try:
            response = requests.get(f"{API_BASE_URL}/data/{category}/time_range")
            print(f"Status: {response.status_code}")
            if response.status_code == 200:
                time_data = response.json()
                print(f"  Start time: {time_data['time_range']['start_time']}")
                print(f"  End time: {time_data['time_range']['end_time']}")
                print(f"  Unique days: {time_data['time_range']['unique_days']}")
            else:
                print(f"  Error: {response.text}")
        except Exception as e:
            print(f"  Error: {e}")
        
        # Only test first category in detail to avoid too much output
        break

def test_filtered_queries():
    """Test filtered queries"""
    print("\n" + "=" * 50)
    print("Testing filtered queries...")
    
    category = 'type1'
    
    # Test date filtering
    print(f"\n8. Testing date filtering for {category}...")
    try:
        # Get first few records to find a date range
        response = requests.get(f"{API_BASE_URL}/data/{category}?limit=1")
        if response.status_code == 200:
            data = response.json()
            if data['data']:
                sample_date = data['data'][0]['timestamp'][:10]  # Get YYYY-MM-DD part
                print(f"  Testing with date: {sample_date}")
                
                filtered_response = requests.get(f"{API_BASE_URL}/data/{category}?start_date={sample_date}&limit=5")
                print(f"  Status: {filtered_response.status_code}")
                if filtered_response.status_code == 200:
                    filtered_data = filtered_response.json()
                    print(f"  Records with date filter: {len(filtered_data['data'])}")
    except Exception as e:
        print(f"  Error: {e}")
    
    # Test unique_id filtering
    print(f"\n9. Testing unique_id filtering for {category}...")
    try:
        # Get a sample unique_id
        response = requests.get(f"{API_BASE_URL}/data/{category}/unique_ids?limit=1")
        if response.status_code == 200:
            ids_data = response.json()
            if ids_data['unique_ids']:
                sample_id = ids_data['unique_ids'][0]
                print(f"  Testing with unique_id: {sample_id}")
                
                filtered_response = requests.get(f"{API_BASE_URL}/data/{category}?unique_id={sample_id}&limit=5")
                print(f"  Status: {filtered_response.status_code}")
                if filtered_response.status_code == 200:
                    filtered_data = filtered_response.json()
                    print(f"  Records with unique_id filter: {len(filtered_data['data'])}")
    except Exception as e:
        print(f"  Error: {e}")

if __name__ == "__main__":
    print("Biosphere Pipeline API Client")
    print("Make sure the API server is running on http://localhost:8000")
    print("Start the server with: python scripts/api_server.py")
    print()
    
    test_api_endpoints()
    test_filtered_queries()
    
    print("\n" + "=" * 50)
    print("API testing completed!")
    print("Visit http://localhost:8000/docs for interactive API documentation")
