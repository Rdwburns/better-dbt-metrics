"""Test metrics referencing semantic models"""

import pytest
import yaml
from pathlib import Path
import tempfile
import shutil

from core.compiler import BetterDBTCompiler, CompilerConfig


class TestSemanticModelReferences:
    """Test the new metric syntax that references semantic models"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test files"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def compiler(self, temp_dir):
        """Create a compiler instance"""
        config = CompilerConfig(
            input_dir=temp_dir,
            output_dir=str(Path(temp_dir) / "output"),
            debug=True,
            split_files=False,
            validate=False  # Disable validation for tests
        )
        return BetterDBTCompiler(config)
    
    def test_basic_semantic_model_reference(self, compiler, temp_dir):
        """Test basic metric referencing a semantic model"""
        # Create semantic model
        semantic_model_content = """
version: 2

semantic_models:
  - name: orders
    description: "Order fact table"
    source: fct_orders
    
    entities:
      - name: order_id
        type: primary
        expr: order_id
        
      - name: customer_id
        type: foreign
        expr: customer_id
    
    dimensions:
      - name: order_date
        type: time
        type_params:
          time_granularity: day
        expr: order_date
    
    measures:
      - name: order_count
        agg: count
        expr: order_id
        agg_time_dimension: order_date
        
      - name: total_revenue
        agg: sum
        expr: revenue
        agg_time_dimension: order_date
"""
        
        # Create metric using new syntax
        metric_content = """
version: 2

metrics:
  - name: revenue
    description: "Total revenue metric"
    type: simple
    # New syntax - reference semantic model
    semantic_model: orders
    measure: total_revenue
    
  - name: order_volume
    description: "Order count metric"
    type: simple
    semantic_model: orders
    measure: order_count
    dimensions:
      - name: order_date
        grain: week
"""
        
        # Write files
        with open(Path(temp_dir) / "semantic_models.yml", "w") as f:
            f.write(semantic_model_content)
            
        with open(Path(temp_dir) / "metrics.yml", "w") as f:
            f.write(metric_content)
        
        # Compile
        result = compiler.compile_directory()
        
        # Check output
        output_file = Path(temp_dir) / "output" / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            output = yaml.safe_load(f)
        
        # Verify metrics were compiled correctly
        assert 'metrics' in output
        metrics = output['metrics']
        
        # Find revenue metric
        revenue_metric = next(m for m in metrics if m['name'] == 'revenue')
        assert revenue_metric['type'] == 'simple'
        # For metrics referencing semantic models, the measure is in type_params
        assert 'type_params' in revenue_metric
        assert revenue_metric['type_params']['measure'] == 'total_revenue'
        
        # Find order_volume metric
        order_metric = next(m for m in metrics if m['name'] == 'order_volume')
        assert order_metric['type'] == 'simple'
        assert 'type_params' in order_metric
        assert order_metric['type_params']['measure'] == 'order_count'
    
    def test_cross_file_semantic_model_reference(self, compiler, temp_dir):
        """Test metric referencing a semantic model in another file"""
        # Create semantic model in one file
        semantic_model_content = """
version: 2

semantic_models:
  - name: customers
    source: dim_customers
    
    entities:
      - name: customer_id
        type: primary
        expr: customer_id
    
    dimensions:
      - name: customer_segment
        type: categorical
        expr: segment
        
      - name: signup_date
        type: time
        type_params:
          time_granularity: day
        expr: created_at
    
    measures:
      - name: customer_count
        agg: count_distinct
        expr: customer_id
        agg_time_dimension: signup_date
"""
        
        # Create metric in another file
        metric_content = """
version: 2

metrics:
  - name: active_customers
    description: "Count of active customers"
    type: simple
    semantic_model: customers
    measure: customer_count
    filter: "is_active = true"
"""
        
        # Write files
        Path(temp_dir).joinpath("semantic_models").mkdir(exist_ok=True)
        with open(Path(temp_dir) / "semantic_models" / "customers.yml", "w") as f:
            f.write(semantic_model_content)
            
        Path(temp_dir).joinpath("metrics").mkdir(exist_ok=True)
        with open(Path(temp_dir) / "metrics" / "customer_metrics.yml", "w") as f:
            f.write(metric_content)
        
        # Compile
        result = compiler.compile_directory()
        
        # Check that metric was compiled successfully
        assert result['metrics_compiled'] > 0
        assert len(result['errors']) == 0
    
    def test_semantic_model_not_found_error(self, compiler, temp_dir):
        """Test error when referencing non-existent semantic model"""
        metric_content = """
version: 2

metrics:
  - name: bad_metric
    type: simple
    semantic_model: non_existent_model
    measure: some_measure
"""
        
        with open(Path(temp_dir) / "metrics.yml", "w") as f:
            f.write(metric_content)
        
        # Should raise an error
        with pytest.raises(ValueError) as exc_info:
            compiler.compile_directory()
        
        assert "references semantic model 'non_existent_model' which doesn't exist" in str(exc_info.value)
    
    def test_measure_not_found_error(self, compiler, temp_dir):
        """Test error when referencing non-existent measure"""
        content = """
version: 2

semantic_models:
  - name: test_model
    source: test_table
    measures:
      - name: valid_measure
        agg: sum
        expr: amount

metrics:
  - name: bad_metric
    type: simple
    semantic_model: test_model
    measure: non_existent_measure
"""
        
        with open(Path(temp_dir) / "test.yml", "w") as f:
            f.write(content)
        
        # Should raise an error
        with pytest.raises(ValueError) as exc_info:
            compiler.compile_directory()
        
        assert "references measure 'non_existent_measure' which doesn't exist" in str(exc_info.value)
    
    def test_mixed_syntax_compatibility(self, compiler, temp_dir):
        """Test that old and new syntax can coexist"""
        content = """
version: 2

semantic_models:
  - name: sales
    source: fct_sales
    measures:
      - name: sales_amount
        agg: sum
        expr: amount

metrics:
  # Old syntax
  - name: total_sales_old
    description: "Using old syntax"
    type: simple
    source: fct_sales
    measure:
      type: sum
      column: amount
      
  # New syntax
  - name: total_sales_new
    description: "Using new syntax"
    type: simple
    semantic_model: sales
    measure: sales_amount
"""
        
        with open(Path(temp_dir) / "test.yml", "w") as f:
            f.write(content)
        
        # Compile
        result = compiler.compile_directory()
        
        # Both metrics should compile successfully
        assert result['metrics_compiled'] == 2
        assert len(result['errors']) == 0