#!/usr/bin/env python3
"""
Direct test of compiler functionality
"""

import sys
import os
import yaml
from pathlib import Path

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Now we can import directly
from core.compiler import BetterDBTCompiler, CompilerConfig
from core.parser import BetterDBTParser
from features.templates import TemplateLibrary
from features.dimension_groups import DimensionGroupManager

def test_metric_types():
    """Test all metric types compile correctly"""
    
    print("=== Testing dbt Metric Types ===\n")
    
    # Create test output directory
    Path("test_output").mkdir(exist_ok=True)
    
    # Test configuration
    config = CompilerConfig(
        input_dir="examples/advanced/",
        output_dir="test_output/",
        split_files=False,
        validate=False
    )
    
    compiler = BetterDBTCompiler(config)
    
    try:
        # Compile the metrics
        print("Compiling metrics...")
        results = compiler.compile_directory()
        
        print(f"✅ Compilation successful!")
        print(f"   Files processed: {results['files_processed']}")
        print(f"   Metrics compiled: {results['metrics_compiled']}")
        print(f"   Models generated: {results['models_generated']}")
        
        # Check the output
        output_file = Path("test_output/compiled_semantic_models.yml")
        if output_file.exists():
            with open(output_file, 'r') as f:
                output = yaml.safe_load(f)
                
            print("\n📊 Metrics by Type:")
            metrics_by_type = {}
            for metric in output.get('metrics', []):
                metric_type = metric['type']
                if metric_type not in metrics_by_type:
                    metrics_by_type[metric_type] = []
                metrics_by_type[metric_type].append(metric['name'])
                
            for metric_type, names in sorted(metrics_by_type.items()):
                print(f"   {metric_type}: {', '.join(names)}")
                
            # Verify all expected types are present
            expected_types = {'simple', 'ratio', 'derived', 'cumulative', 'conversion'}
            found_types = set(metrics_by_type.keys())
            
            if expected_types.issubset(found_types):
                print(f"\n✅ All expected metric types present!")
            else:
                missing = expected_types - found_types
                print(f"\n❌ Missing metric types: {missing}")
                
            # Check entities in semantic models
            print("\n🔑 Entities in Semantic Models:")
            for model in output.get('semantic_models', []):
                entities = model.get('entities', [])
                print(f"   {model['name']}: {[e['name'] for e in entities]}")
                
            # Check advanced measure types
            print("\n📈 Advanced Measure Types:")
            measure_types = set()
            for model in output.get('semantic_models', []):
                for measure in model.get('measures', []):
                    measure_types.add(measure['agg'])
            print(f"   Found: {', '.join(sorted(measure_types))}")
            
        else:
            print(f"❌ Output file not found: {output_file}")
            
    except Exception as e:
        print(f"❌ Compilation failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    return True

if __name__ == "__main__":
    success = test_metric_types()
    sys.exit(0 if success else 1)