#!/usr/bin/env python3
"""
Run the test suite for Better-DBT-Metrics
"""

import sys
import subprocess
import os

def run_tests():
    """Run the test suite"""
    
    # Add src to Python path
    src_path = os.path.join(os.path.dirname(__file__), 'src')
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    
    # Install pytest if not available
    try:
        import pytest
    except ImportError:
        print("Installing pytest...")
        subprocess.run([sys.executable, "-m", "pip", "install", "pytest", "pytest-cov"], check=True)
        import pytest
    
    # Run tests with coverage
    print("Running tests...")
    exit_code = pytest.main([
        "tests/",
        "-v",
        "--cov=src",
        "--cov-report=term-missing",
        "--cov-report=html",
        "-x",  # Stop on first failure
    ])
    
    if exit_code == 0:
        print("\n✅ All tests passed!")
        print("Coverage report generated in htmlcov/")
    else:
        print(f"\n❌ Tests failed with exit code: {exit_code}")
        
    return exit_code

if __name__ == "__main__":
    sys.exit(run_tests())