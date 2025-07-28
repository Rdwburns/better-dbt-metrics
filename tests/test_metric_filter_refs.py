"""Tests for metric references in filters"""

import pytest
import yaml
from pathlib import Path
import tempfile
import shutil

from core.compiler import BetterDBTCompiler, CompilerConfig


class TestMetricFilterReferences:
    """Test metric references in filter expressions"""
    
    def setup_method(self):
        """Setup test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.metrics_dir = Path(self.temp_dir) / "metrics"
        self.output_dir = Path(self.temp_dir) / "output"
        
        self.metrics_dir.mkdir()
        self.output_dir.mkdir()
        
    def teardown_method(self):
        """Cleanup temp files"""
        shutil.rmtree(self.temp_dir)
        
    def test_simple_metric_reference_in_filter(self):
        """Test a simple metric reference in a filter"""
        
        metrics_content = """
version: 2

metrics:
  # Base metric for average
  - name: average_order_value
    type: simple
    source: fct_orders
    measure:
      type: average
      column: order_total
      
  # Metric with filter referencing another metric
  - name: high_value_orders
    type: simple
    source: fct_orders
    measure:
      type: count
      column: order_id
    filter: "order_total > metric('average_order_value')"
"""
        
        metrics_file = self.metrics_dir / "order_metrics.yml"
        with open(metrics_file, 'w') as f:
            f.write(metrics_content)
            
        # Compile
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir),
            split_files=False,
            auto_variants=False,
            validate=False
        )
        
        compiler = BetterDBTCompiler(config)
        results = compiler.compile_directory()
        
        assert results['files_processed'] == 1
        assert results['metrics_compiled'] == 2
        assert len(results['errors']) == 0
        
        # Check output
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            output = yaml.safe_load(f)
            
        # Find the high_value_orders metric
        metrics = {m['name']: m for m in output['metrics']}
        high_value_metric = metrics['high_value_orders']
        
        # Check that the filter is preserved
        assert 'filter' in high_value_metric
        assert "order_total > metric('average_order_value')" in high_value_metric['filter']
        
        # Check that metric references were extracted
        assert 'meta' in high_value_metric
        assert 'metric_refs_in_filter' in high_value_metric['meta']
        assert 'average_order_value' in high_value_metric['meta']['metric_refs_in_filter']
        
    def test_multiple_metric_references_in_filter(self):
        """Test multiple metric references in a single filter"""
        
        metrics_content = """
version: 2

metrics:
  - name: min_order_value
    type: simple
    source: fct_orders
    measure:
      type: min
      column: order_total
      
  - name: max_order_value
    type: simple
    source: fct_orders
    measure:
      type: max
      column: order_total
      
  - name: mid_range_orders
    type: simple
    source: fct_orders
    measure:
      type: count
      column: order_id
    filter: "order_total > metric('min_order_value') AND order_total < metric('max_order_value')"
"""
        
        metrics_file = self.metrics_dir / "range_metrics.yml"
        with open(metrics_file, 'w') as f:
            f.write(metrics_content)
            
        # Compile
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir),
            split_files=False,
            auto_variants=False,
            validate=False
        )
        
        compiler = BetterDBTCompiler(config)
        results = compiler.compile_directory()
        
        assert results['files_processed'] == 1
        assert results['metrics_compiled'] == 3
        assert len(results['errors']) == 0
        
    def test_complex_filter_expression(self):
        """Test complex filter expression with metric references"""
        
        metrics_content = """
version: 2

metrics:
  - name: avg_revenue
    type: simple
    source: fct_revenue
    measure:
      type: average
      column: revenue
      
  - name: revenue_stddev
    type: simple  
    source: fct_revenue
    measure:
      type: stddev
      column: revenue
      
  - name: outlier_revenue
    type: simple
    source: fct_revenue
    measure:
      type: sum
      column: revenue
    filter: |
      revenue > (metric('avg_revenue') + 2 * metric('revenue_stddev'))
      OR revenue < (metric('avg_revenue') - 2 * metric('revenue_stddev'))
"""
        
        metrics_file = self.metrics_dir / "outlier_metrics.yml"
        with open(metrics_file, 'w') as f:
            f.write(metrics_content)
            
        # Compile
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir),
            split_files=False,
            auto_variants=False,
            validate=False
        )
        
        compiler = BetterDBTCompiler(config)
        results = compiler.compile_directory()
        
        assert results['files_processed'] == 1
        assert results['metrics_compiled'] == 3
        assert len(results['errors']) == 0
        
    def test_measure_filter_with_metric_reference(self):
        """Test metric reference in measure-level filter"""
        
        metrics_content = """
version: 2

metrics:
  - name: threshold_value
    type: simple
    source: fct_config
    measure:
      type: max
      column: threshold
      
  - name: above_threshold_sum
    type: simple
    source: fct_transactions
    measure:
      type: sum
      column: amount
      filters:
        - "amount > metric('threshold_value')"
"""
        
        metrics_file = self.metrics_dir / "threshold_metrics.yml"
        with open(metrics_file, 'w') as f:
            f.write(metrics_content)
            
        # Compile
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir),
            split_files=False,
            auto_variants=False,
            validate=False
        )
        
        compiler = BetterDBTCompiler(config)
        results = compiler.compile_directory()
        
        assert results['files_processed'] == 1
        assert results['metrics_compiled'] == 2
        assert len(results['errors']) == 0
        
        # Check output
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            output = yaml.safe_load(f)
            
        # Check that the measure filter references were extracted
        semantic_models = {m['name']: m for m in output['semantic_models']}
        trans_model = semantic_models['sem_fct_transactions']
        
        # Find the measure with the filter
        above_threshold_measure = next(
            m for m in trans_model['measures'] 
            if m['name'] == 'above_threshold_sum_measure'
        )
        
        assert 'agg_params' in above_threshold_measure
        assert 'where' in above_threshold_measure['agg_params']
        assert 'metric_refs' in above_threshold_measure['agg_params']
        assert 'threshold_value' in above_threshold_measure['agg_params']['metric_refs']