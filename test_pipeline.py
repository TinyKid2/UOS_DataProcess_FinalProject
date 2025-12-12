#!/usr/bin/env python3
"""
Test script for ETL pipeline
Simulates model submission and checks results
"""

import requests
import time
import json
import sys
from datetime import datetime

BASE_URL = "http://localhost:15000"

def test_health():
    """Test if web server is running"""
    try:
        response = requests.get(f"{BASE_URL}/health")
        if response.status_code == 200:
            print("✓ Web server is healthy")
            return True
        else:
            print("✗ Web server health check failed")
            return False
    except Exception as e:
        print(f"✗ Cannot connect to web server: {e}")
        return False

def create_dummy_model():
    """Create a dummy .pt file for testing"""
    import os
    dummy_file = "test_model.pt"
    with open(dummy_file, "wb") as f:
        # Write some dummy bytes to simulate a model file
        f.write(b"DUMMY_YOLO_MODEL_DATA" * 1000)
    print(f"✓ Created dummy model file: {dummy_file}")
    return dummy_file

def submit_model(model_file, name="Test User"):
    """Submit a model for evaluation"""
    try:
        with open(model_file, 'rb') as f:
            files = {'model': (model_file, f, 'application/octet-stream')}
            data = {'name': name}
            response = requests.post(f"{BASE_URL}/upload", files=files, data=data)
        
        if response.status_code == 200:
            result = response.json()
            print(f"✓ Model submitted successfully")
            print(f"  Submission ID: {result['submission_id']}")
            return result['submission_id']
        else:
            print(f"✗ Failed to submit model: {response.text}")
            return None
    except Exception as e:
        print(f"✗ Error submitting model: {e}")
        return None

def check_results(submission_id, max_attempts=10):
    """Check evaluation results"""
    print(f"\nChecking results for submission {submission_id}...")
    
    for attempt in range(max_attempts):
        try:
            response = requests.get(f"{BASE_URL}/results/{submission_id}")
            
            if response.status_code == 200:
                results = response.json()
                print(f"✓ Evaluation completed!")
                print(f"  F1 Score: {results.get('f1_score', 'N/A')}")
                print(f"  Precision: {results.get('precision', 'N/A')}")
                print(f"  Recall: {results.get('recall', 'N/A')}")
                return True
            elif response.status_code == 404:
                print(f"  Attempt {attempt + 1}/{max_attempts}: Results not yet available...")
                time.sleep(3)
            else:
                print(f"✗ Error checking results: {response.text}")
                return False
        except Exception as e:
            print(f"✗ Error: {e}")
            return False
    
    print("✗ Evaluation timed out")
    return False

def main():
    print("=" * 50)
    print("ETL Pipeline Test")
    print("=" * 50)
    
    # Test health
    if not test_health():
        print("\nMake sure the Docker containers are running:")
        print("  docker-compose up -d")
        sys.exit(1)
    
    # Create and submit dummy model
    model_file = create_dummy_model()
    submission_id = submit_model(model_file, "Pipeline Tester")
    
    if submission_id:
        # Check results
        check_results(submission_id)
    
    # Cleanup
    import os
    if os.path.exists(model_file):
        os.remove(model_file)
        print(f"\n✓ Cleaned up test file")
    
    print("\n" + "=" * 50)
    print("Test completed!")

if __name__ == "__main__":
    main()