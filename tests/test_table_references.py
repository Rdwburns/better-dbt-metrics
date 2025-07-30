"""
Test table reference parsing and compilation
"""

import pytest
import yaml
from pathlib import Path
import tempfile
import shutil

from core.parser import BetterDBTParser
from core.compiler import BetterDBTCompiler, CompilerConfig


class TestTableReferences:
    """Test table reference functionality"""
    
    def setup_method(self):
        """Set up test environment"""
        self.test_dir = tempfile.mkdtemp()
        self.parser = BetterDBTParser(base_dir=self.test_dir)
        
    def teardown_method(self):
        """Clean up test environment"""
        shutil.rmtree(self.test_dir)
    
    def test_ref_function_parsing(self):
        """Test parsing of ref() function syntax"""
        test_content = """
metrics:
  - name: test_metric
    type: simple
    source: ref('fct_orders')
    measure:
      type: sum
      column: amount
"""
        test_file = Path(self.test_dir) / "test.yml"
        with open(test_file, 'w') as f:
            f.write(test_content)
            
        result = self.parser.parse_file(str(test_file))
        
        metric = result['metrics'][0]
        assert metric['source'] == 'fct_orders'
        assert 'source_ref' in metric
        assert metric['source_ref']['table'] == 'fct_orders'
        assert metric['source_ref']['type'] == 'ref'
    
    def test_table_function_parsing(self):
        """Test parsing of $table() function syntax"""
        test_content = """
metrics:
  - name: test_metric
    type: simple
    source: $table('fct_sales')
    measure:
      type: sum
      column: revenue
"""
        test_file = Path(self.test_dir) / "test.yml"
        with open(test_file, 'w') as f:
            f.write(test_content)
            
        result = self.parser.parse_file(str(test_file))
        
        metric = result['metrics'][0]
        assert metric['source'] == 'fct_sales'
        assert 'source_ref' in metric
        assert metric['source_ref']['table'] == 'fct_sales'
        assert metric['source_ref']['type'] == 'table'
    
    def test_dict_ref_parsing(self):
        """Test parsing of dictionary ref format"""
        test_content = """
metrics:
  - name: test_metric
    type: simple
    source:
      ref: fct_customers
    measure:
      type: count
      column: customer_id
"""
        test_file = Path(self.test_dir) / "test.yml"
        with open(test_file, 'w') as f:
            f.write(test_content)
            
        result = self.parser.parse_file(str(test_file))
        
        metric = result['metrics'][0]
        assert metric['source'] == 'fct_customers'
        assert 'source_ref' in metric
        assert metric['source_ref']['table'] == 'fct_customers'
        assert metric['source_ref']['type'] == 'ref'
    
    def test_dict_table_parsing(self):
        """Test parsing of dictionary $table format"""
        test_content = """
metrics:
  - name: test_metric
    type: simple
    source:
      $table: fct_products
    measure:
      type: sum
      column: price
"""
        test_file = Path(self.test_dir) / "test.yml"
        with open(test_file, 'w') as f:
            f.write(test_content)
            
        result = self.parser.parse_file(str(test_file))
        
        metric = result['metrics'][0]
        assert metric['source'] == 'fct_products'
        assert 'source_ref' in metric
        assert metric['source_ref']['table'] == 'fct_products'
        assert metric['source_ref']['type'] == 'table'
    
    def test_traditional_string_format(self):
        """Test that traditional string format still works"""
        test_content = """
metrics:
  - name: test_metric
    type: simple
    source: fct_traditional
    measure:
      type: sum
      column: value
"""
        test_file = Path(self.test_dir) / "test.yml"
        with open(test_file, 'w') as f:
            f.write(test_content)
            
        result = self.parser.parse_file(str(test_file))
        
        metric = result['metrics'][0]
        assert metric['source'] == 'fct_traditional'
        assert 'source_ref' not in metric  # No ref metadata for plain strings
    
    def test_ratio_metric_with_refs(self):
        """Test ratio metric with different table references"""
        test_content = """
metrics:
  - name: profit_margin
    type: ratio
    numerator:
      source: ref('fct_financials')
      measure:
        type: sum
        column: profit
    denominator:
      source: ref('fct_sales')
      measure:
        type: sum
        column: revenue
"""
        test_file = Path(self.test_dir) / "test.yml"
        with open(test_file, 'w') as f:
            f.write(test_content)
            
        result = self.parser.parse_file(str(test_file))
        
        metric = result['metrics'][0]
        assert metric['numerator']['source'] == 'fct_financials'
        assert 'source_ref' in metric['numerator']
        assert metric['numerator']['source_ref']['table'] == 'fct_financials'
        
        assert metric['denominator']['source'] == 'fct_sales'
        assert 'source_ref' in metric['denominator']
        assert metric['denominator']['source_ref']['table'] == 'fct_sales'
    
    def test_compiled_output_preserves_refs(self):
        """Test that compiled output preserves source_ref metadata"""
        test_content = """
version: 2
metrics:
  - name: test_metric
    description: "Test metric with ref"
    type: simple
    source: ref('fct_test')
    measure:
      type: sum
      column: amount
    dimensions:
      - name: date
        type: time
        grain: day
"""
        metrics_dir = Path(self.test_dir) / "metrics"
        metrics_dir.mkdir()
        
        test_file = metrics_dir / "test.yml"
        with open(test_file, 'w') as f:
            f.write(test_content)
        
        output_dir = Path(self.test_dir) / "output"
        
        # Compile the metrics
        config = CompilerConfig(
            input_dir=str(metrics_dir),
            output_dir=str(output_dir),
            validate=False,  # Skip validation for test
            split_files=False
        )
        compiler = BetterDBTCompiler(config)
        
        # Mock the model scanner to avoid validation errors
        compiler._model_scanner = None
        
        result = compiler.compile_directory()
        
        # Read the compiled output
        output_file = output_dir / "compiled_semantic_models.yml"
        assert output_file.exists()
        
        with open(output_file, 'r') as f:
            compiled = yaml.safe_load(f)
        
        # Check that source_ref is preserved in meta
        metric = compiled['metrics'][0]
        assert 'meta' in metric
        assert 'source_ref' in metric['meta']
        assert metric['meta']['source_ref']['table'] == 'fct_test'
        assert metric['meta']['source_ref']['type'] == 'ref'
    
    def test_nested_table_refs(self):
        """Test table references in nested structures"""
        test_content = """
metrics:
  - name: complex_metric
    type: ratio
    numerator:
      source: ref('fct_numerator')
      measure:
        type: sum
        column: num_value
      dimensions:
        - name: category
          source: ref('dim_category')
          column: category_name
    denominator:
      source: $table('fct_denominator')
      measure:
        type: sum
        column: den_value
"""
        test_file = Path(self.test_dir) / "test.yml"
        with open(test_file, 'w') as f:
            f.write(test_content)
            
        result = self.parser.parse_file(str(test_file))
        
        metric = result['metrics'][0]
        
        # Check numerator refs
        assert metric['numerator']['source'] == 'fct_numerator'
        assert metric['numerator']['source_ref']['type'] == 'ref'
        
        # Check dimension refs
        dim = metric['numerator']['dimensions'][0]
        assert dim['source'] == 'dim_category'
        assert dim['source_ref']['table'] == 'dim_category'
        assert dim['source_ref']['type'] == 'ref'
        
        # Check denominator refs
        assert metric['denominator']['source'] == 'fct_denominator'
        assert metric['denominator']['source_ref']['type'] == 'table'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])