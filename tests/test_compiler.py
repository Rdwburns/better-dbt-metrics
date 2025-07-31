"""Tests for the compiler module"""

import pytest
import yaml
from pathlib import Path
import tempfile
import shutil

from core.compiler import BetterDBTCompiler, CompilerConfig


class TestCompiler:
    """Test the BetterDBTCompiler functionality"""
    
    def setup_method(self):
        """Setup test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.output_dir = Path(self.temp_dir) / "output"
        self.output_dir.mkdir()
        
    def teardown_method(self):
        """Cleanup temp files"""
        shutil.rmtree(self.temp_dir)
        
    def create_test_file(self, filename, content):
        """Helper to create test files"""
        file_path = Path(self.temp_dir) / filename
        with open(file_path, 'w') as f:
            f.write(content)
        return file_path
        
    def test_simple_metric_compilation(self):
        """Test compiling a simple metric"""
        content = """
version: 2
metrics:
  - name: total_revenue
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: revenue
    dimensions:
      - name: order_date
        type: time
        grain: day
"""
        self.create_test_file("metrics.yml", content)
        
        config = CompilerConfig(
            input_dir=self.temp_dir,
            output_dir=str(self.output_dir),
            split_files=False,
            validate=False
        )
        compiler = BetterDBTCompiler(config)
        results = compiler.compile_directory()
        
        assert results['metrics_compiled'] == 1
        assert results['models_generated'] == 1
        
        # Check output file
        output_file = self.output_dir / "compiled_semantic_models.yml"
        assert output_file.exists()
        
        with open(output_file, 'r') as f:
            output = yaml.safe_load(f)
            
        assert 'metrics' in output
        assert len(output['metrics']) == 1
        assert output['metrics'][0]['name'] == 'total_revenue'
        assert output['metrics'][0]['type'] == 'simple'
        
    def test_ratio_metric_compilation(self):
        """Test compiling a ratio metric"""
        content = """
version: 2
metrics:
  - name: refund_rate
    type: ratio
    numerator:
      source: fct_refunds
      measure:
        type: sum
        column: refund_amount
    denominator:
      source: fct_orders
      measure:
        type: sum
        column: order_amount
    dimensions:
      - name: date_month
        type: time
        grain: month
"""
        self.create_test_file("metrics.yml", content)
        
        config = CompilerConfig(
            input_dir=self.temp_dir,
            output_dir=str(self.output_dir),
            split_files=False,
            validate=False
        )
        compiler = BetterDBTCompiler(config)
        results = compiler.compile_directory()
        
        # Check output
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            output = yaml.safe_load(f)
            
        # Verify ratio metric structure - find the ratio metric by name
        ratio_metric = None
        for metric in output['metrics']:
            if metric['name'] == 'refund_rate':
                ratio_metric = metric
                break
        
        assert ratio_metric is not None, "Ratio metric 'refund_rate' not found in output"
        assert ratio_metric['type'] == 'ratio'
        assert 'type_params' in ratio_metric
        assert 'numerator' in ratio_metric['type_params']
        assert 'denominator' in ratio_metric['type_params']
        
        # Check semantic models have both measures
        models = output['semantic_models']
        measures = []
        for model in models:
            measures.extend([m['name'] for m in model.get('measures', [])])
        
        assert 'refund_rate_numerator' in measures
        assert 'refund_rate_denominator' in measures
    
    def test_ratio_metric_missing_source_validation(self):
        """Test that ratio metrics without proper sources fail validation"""
        # Test case 1: Missing numerator source
        content = """
version: 2
metrics:
  - name: bad_ratio
    type: ratio
    numerator:
      measure:
        type: count
        column: visitors
    denominator:
      source: fct_visits  
      measure:
        type: count
        column: visitors
"""
        self.create_test_file("bad_ratio.yml", content)
        
        config = CompilerConfig(
            input_dir=self.temp_dir,
            output_dir=str(self.output_dir),
            debug=False,
            validate=False
        )
        compiler = BetterDBTCompiler(config)
        
        # Should capture error in results
        results = compiler.compile_directory()
        
        # Check that there was an error
        assert len(results['errors']) > 0
        error_msg = results['errors'][0]['error']
        assert "bad_ratio" in error_msg
        assert "numerator.source" in error_msg
        assert "denominator.source" in error_msg
        
    def test_ratio_metric_auto_source_detection(self):
        """Test that ratio metrics auto-detect source when both num/den have same source"""
        content = """
version: 2
metrics:
  - name: same_source_ratio
    type: ratio
    numerator:
      source: fct_orders
      measure:
        type: sum
        column: refunds
    denominator:
      source: fct_orders  
      measure:
        type: sum
        column: revenue
"""
        self.create_test_file("same_source.yml", content)
        
        config = CompilerConfig(
            input_dir=self.temp_dir,
            output_dir=str(self.output_dir),
            debug=False,
            validate=False
        )
        compiler = BetterDBTCompiler(config)
        
        # Should compile successfully and auto-detect source
        result = compiler.compile_directory()
        assert result['errors'] == []
        assert result['metrics_compiled'] > 0
        
        # Check that semantic model was created for fct_orders, not unknown
        output_files = list(self.output_dir.glob("*.yml"))
        sem_files = [f for f in output_files if f.name.startswith("sem_")]
        
        # Should not have sem_unknown.yml
        assert not any(f.name == "sem_unknown.yml" for f in sem_files)
        # Should have sem_fct_orders.yml
        assert any(f.name == "sem_fct_orders.yml" for f in sem_files)
        
    def test_derived_metric_compilation(self):
        """Test compiling a derived metric"""
        content = """
version: 2
metrics:
  - name: revenue_per_order
    type: derived
    expression: "metric('total_revenue') / metric('total_orders')"
    dimensions:
      - name: region
        type: categorical
"""
        self.create_test_file("metrics.yml", content)
        
        config = CompilerConfig(
            input_dir=self.temp_dir,
            output_dir=str(self.output_dir),
            split_files=False,
            validate=False
        )
        compiler = BetterDBTCompiler(config)
        results = compiler.compile_directory()
        
        # Check output
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            output = yaml.safe_load(f)
            
        metric = output['metrics'][0]
        assert metric['type'] == 'derived'
        assert 'type_params' in metric
        assert 'expr' in metric['type_params']
        assert 'metrics' in metric['type_params']
        assert 'total_revenue' in metric['type_params']['metrics']
        assert 'total_orders' in metric['type_params']['metrics']
        
    def test_cumulative_metric_compilation(self):
        """Test compiling a cumulative metric"""
        content = """
version: 2
metrics:
  - name: running_total
    type: cumulative
    source: fct_sales
    measure:
      type: sum
      column: sales_amount
    window: unbounded
    grain_to_date: month
    dimensions:
      - name: sales_date
        type: time
        grain: day
"""
        self.create_test_file("metrics.yml", content)
        
        config = CompilerConfig(
            input_dir=self.temp_dir,
            output_dir=str(self.output_dir),
            split_files=False,
            validate=False
        )
        compiler = BetterDBTCompiler(config)
        results = compiler.compile_directory()
        
        # Check output
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            output = yaml.safe_load(f)
            
        metric = output['metrics'][0]
        assert metric['type'] == 'cumulative'
        assert 'type_params' in metric
        assert 'cumulative_type_params' in metric['type_params']
        assert metric['type_params']['cumulative_type_params']['window'] == 'unbounded'
        assert metric['type_params']['cumulative_type_params']['grain_to_date'] == 'month'
        
    def test_conversion_metric_compilation(self):
        """Test compiling a conversion metric"""
        content = """
version: 2
metrics:
  - name: signup_conversion
    type: conversion
    base_measure:
      source: fct_events
      measure:
        type: count_distinct
        column: user_id
        filters:
          - "event_type = 'visit'"
    conversion_measure:
      source: fct_events
      measure:
        type: count_distinct
        column: user_id
        filters:
          - "event_type = 'signup'"
    entity: user_id
    window: 7 days
"""
        self.create_test_file("metrics.yml", content)
        
        config = CompilerConfig(
            input_dir=self.temp_dir,
            output_dir=str(self.output_dir),
            split_files=False,
            validate=False
        )
        compiler = BetterDBTCompiler(config)
        results = compiler.compile_directory()
        
        # Check output
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            output = yaml.safe_load(f)
            
        metric = output['metrics'][0]
        assert metric['type'] == 'conversion'
        assert 'type_params' in metric
        assert metric['type_params']['entity'] == 'user_id'
        assert metric['type_params']['window'] == '7 days'
        
    def test_advanced_measure_types(self):
        """Test advanced measure type compilation"""
        content = """
version: 2
metrics:
  - name: median_value
    type: simple
    source: fct_transactions
    measure:
      type: median
      column: transaction_amount
      
  - name: p95_latency
    type: simple
    source: fct_api_calls
    measure:
      type: percentile
      column: response_time
      percentile: 0.95
      
  - name: revenue_stddev
    type: simple
    source: fct_revenue
    measure:
      type: stddev
      column: daily_revenue
"""
        self.create_test_file("metrics.yml", content)
        
        config = CompilerConfig(
            input_dir=self.temp_dir,
            output_dir=str(self.output_dir),
            split_files=False,
            validate=False
        )
        compiler = BetterDBTCompiler(config)
        results = compiler.compile_directory()
        
        # Check output
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            output = yaml.safe_load(f)
            
        # Check measures in semantic models
        all_measures = []
        for model in output['semantic_models']:
            all_measures.extend(model.get('measures', []))
            
        # Verify measure types
        measure_types = {m['agg'] for m in all_measures}
        assert 'median' in measure_types
        assert 'percentile' in measure_types
        assert 'stddev' in measure_types
        
        # Check percentile parameters
        p95_measure = next(m for m in all_measures if 'p95' in m['name'])
        assert 'agg_params' in p95_measure
        assert p95_measure['agg_params']['percentile'] == 0.95
        
    def test_entity_extraction(self):
        """Test entity extraction in semantic models"""
        content = """
version: 2
metrics:
  - name: unique_users
    type: simple
    source: fct_events
    entity: user_id
    measure:
      type: count_distinct
      column: user_id
      
  - name: orders_per_customer
    type: simple
    source: fct_orders
    measure:
      type: count
      column: order_id
    dimensions:
      - name: customer_id
        type: categorical
"""
        self.create_test_file("metrics.yml", content)
        
        config = CompilerConfig(
            input_dir=self.temp_dir,
            output_dir=str(self.output_dir),
            split_files=False,
            validate=False
        )
        compiler = BetterDBTCompiler(config)
        results = compiler.compile_directory()
        
        # Check output
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            output = yaml.safe_load(f)
            
        # Check entities in semantic models
        for model in output['semantic_models']:
            assert 'entities' in model
            assert len(model['entities']) > 0
            
            if model['name'] == 'sem_fct_events':
                # Should have extracted user_id entity
                entity_names = [e['name'] for e in model['entities']]
                assert 'user_id' in entity_names
                
            elif model['name'] == 'sem_fct_orders':
                # Should have detected customer_id from dimension
                entity_names = [e['name'] for e in model['entities']]
                assert 'customer_id' in entity_names or 'id' in entity_names
                
    def test_dimension_groups(self):
        """Test dimension group expansion"""
        content = """
version: 2

dimension_groups:
  temporal:
    dimensions:
      - name: date_day
        type: time
        grain: day
      - name: date_month
        type: time
        grain: month
        
metrics:
  - name: revenue
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: amount
    dimension_groups: [temporal]
"""
        self.create_test_file("metrics.yml", content)
        
        config = CompilerConfig(
            input_dir=self.temp_dir,
            output_dir=str(self.output_dir),
            split_files=False,
            validate=False
        )
        compiler = BetterDBTCompiler(config)
        results = compiler.compile_directory()
        
        # Check output
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            output = yaml.safe_load(f)
            
        # Check dimensions in semantic model
        model = output['semantic_models'][0]
        dimension_names = [d['name'] for d in model['dimensions']]
        assert 'date_day' in dimension_names
        assert 'date_month' in dimension_names
        
    def test_auto_variants(self):
        """Test auto-variant generation"""
        content = """
version: 2
metrics:
  - name: revenue
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: amount
    auto_variants:
      time_comparison: [wow, mom]
      by_dimension: [region]
"""
        self.create_test_file("metrics.yml", content)
        
        config = CompilerConfig(
            input_dir=self.temp_dir,
            output_dir=str(self.output_dir),
            split_files=False,
            auto_variants=True,
            validate=False
        )
        compiler = BetterDBTCompiler(config)
        results = compiler.compile_directory()
        
        # Should have created additional metrics
        assert results['metrics_compiled'] > 1
        
        # Check output
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            output = yaml.safe_load(f)
            
        metric_names = [m['name'] for m in output['metrics']]
        assert 'revenue' in metric_names
        assert 'revenue_wow' in metric_names
        assert 'revenue_mom' in metric_names
        assert 'revenue_by_region' in metric_names