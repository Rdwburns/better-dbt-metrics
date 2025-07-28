"""
Better-DBT-Metrics: A DRY approach to dbt metrics
"""

__version__ = "2.0.0"

# Make the package work with both relative and absolute imports
import sys
from pathlib import Path

# Add src directory to path for absolute imports
src_path = Path(__file__).parent
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))