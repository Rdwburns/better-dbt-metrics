#!/usr/bin/env python3
"""
Test script to compile metrics and verify output
"""

import sys
import os
import yaml
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from core.compiler import BetterDBTCompiler, CompilerConfig

def test_compile():
    """Test compilation of example metrics"""
    
    # Test simple example
    print("Testing simple example...")
    config = CompilerConfig(
        input_dir="examples/simple/",
        output_dir="test_output/simple/",
        template_dirs=["examples/templates/"],
        split_files=False
    )
    
    compiler = BetterDBTCompiler(config)
    try:
        results = compiler.compile_directory()
        print(f"✅ Simple example compiled successfully!")
        print(f"   Metrics compiled: {results['metrics_compiled']}")
    except Exception as e:
        print(f"❌ Simple example failed: {e}")
        
    # Test advanced example with all metric types
    print("\nTesting advanced example with all metric types...")
    config = CompilerConfig(
        input_dir="examples/advanced/",
        output_dir="test_output/advanced/",
        split_files=True
    )
    
    compiler = BetterDBTCompiler(config)
    try:
        results = compiler.compile_directory()
        print(f"✅ Advanced example compiled successfully!")
        print(f"   Metrics compiled: {results['metrics_compiled']}")
        
        # Check output
        output_path = Path("test_output/advanced/_metrics.yml")
        if output_path.exists():
            with open(output_path, 'r') as f:
                output = yaml.safe_load(f)
                print(f"   Metric types found: {set(m['type'] for m in output['metrics'])}")
                
                # Verify each type
                types_found = {m['type'] for m in output['metrics']}
                expected_types = {'simple', 'ratio', 'derived', 'cumulative', 'conversion'}
                if expected_types.issubset(types_found):
                    print("   ✅ All metric types present!")
                else:
                    missing = expected_types - types_found
                    print(f"   ❌ Missing metric types: {missing}")
                    
    except Exception as e:
        print(f"❌ Advanced example failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_compile()