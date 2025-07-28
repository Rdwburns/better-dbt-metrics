"""
Tests for metric_time dimension support
"""

import pytest
import yaml
from pathlib import Path
import tempfile
import shutil

from src.core.compiler import BetterDBTCompiler, CompilerConfig


class TestMetricTime:
    """Test metric_time dimension functionality"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp()
        self.metrics_dir = Path(self.test_dir) / "metrics"
        self.metrics_dir.mkdir(parents=True)
        self.output_dir = Path(self.test_dir) / "output"
        
    def teardown_method(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.test_dir)
        
    def test_basic_metric_time(self):
        """Test basic metric_time dimension"""
        metrics_file = self.metrics_dir / "test_metric_time.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: daily_orders
    type: simple
    source: fct_orders
    measure:
      type: count
      column: order_id
    dimensions:
      - name: metric_time
        type: time
        grain: day
        expr: order_date
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check metric was compiled with metric_time
        metric = compiler.compiled_metrics[0]
        metric_time_dims = [d for d in metric['dimensions'] if d.get('name') == 'metric_time']
        assert len(metric_time_dims) == 1
        assert metric_time_dims[0]['is_metric_time'] == True
        
        # Check semantic model has metric_time dimension
        semantic_model = compiler.semantic_models[0]
        dimensions = semantic_model['dimensions']
        metric_time_dim = next((d for d in dimensions if d['name'] == 'metric_time'), None)
        assert metric_time_dim is not None
        assert metric_time_dim['type'] == 'time'
        assert metric_time_dim['label'] == 'Metric Time'
        assert metric_time_dim.get('is_primary_time') == True
        
    def test_metric_time_different_grains(self):
        """Test metric_time with different grains"""
        metrics_file = self.metrics_dir / "test_metric_time_grains.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: monthly_revenue
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: revenue
    dimensions:
      - name: metric_time
        type: time
        grain: month
        expr: order_date
        
  - name: weekly_orders
    type: simple
    source: fct_orders
    measure:
      type: count
      column: order_id
    dimensions:
      - name: metric_time
        type: time
        grain: week
        expr: order_date
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check first metric has month grain
        monthly_metric = next(m for m in compiler.compiled_metrics if m['name'] == 'monthly_revenue')
        mt_dim = next(d for d in monthly_metric['dimensions'] if d['name'] == 'metric_time')
        assert mt_dim['grain'] == 'month'
        
        # Check second metric has week grain
        weekly_metric = next(m for m in compiler.compiled_metrics if m['name'] == 'weekly_orders')
        mt_dim = next(d for d in weekly_metric['dimensions'] if d['name'] == 'metric_time')
        assert mt_dim['grain'] == 'week'
        
    def test_metric_time_in_ratio_metrics(self):
        """Test metric_time in ratio metrics"""
        metrics_file = self.metrics_dir / "test_metric_time_ratio.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: conversion_rate
    type: ratio
    numerator:
      source: fct_conversions
      measure:
        type: count
        column: conversion_id
      dimensions:
        - name: metric_time
          type: time
          grain: day
          expr: conversion_date
    denominator:
      source: fct_visits
      measure:
        type: count
        column: visit_id
      dimensions:
        - name: metric_time
          type: time
          grain: day
          expr: visit_date
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check metric was compiled
        metric = next(m for m in compiler.compiled_metrics if m['name'] == 'conversion_rate')
        assert metric['type'] == 'ratio'
        
        # Check numerator has metric_time
        num_dims = metric['numerator']['dimensions']
        assert any(d['name'] == 'metric_time' and d.get('is_metric_time') for d in num_dims)
        
        # Check denominator has metric_time
        den_dims = metric['denominator']['dimensions']
        assert any(d['name'] == 'metric_time' and d.get('is_metric_time') for d in den_dims)
        
    def test_multiple_time_dimensions_with_metric_time(self):
        """Test metrics with both metric_time and other time dimensions"""
        metrics_file = self.metrics_dir / "test_multiple_time_dims.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: order_lifecycle
    type: simple
    source: fct_orders
    measure:
      type: count
      column: order_id
    dimensions:
      - name: metric_time
        type: time
        grain: day
        expr: order_date
      - name: ship_date
        type: time
        grain: day
      - name: delivery_date
        type: time
        grain: day
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check all dimensions are present
        metric = compiler.compiled_metrics[0]
        dim_names = [d['name'] for d in metric['dimensions']]
        assert 'metric_time' in dim_names
        assert 'ship_date' in dim_names
        assert 'delivery_date' in dim_names
        
        # Check only metric_time is marked as primary
        semantic_model = compiler.semantic_models[0]
        time_dims = [d for d in semantic_model['dimensions'] if d['type'] == 'time']
        primary_dims = [d for d in time_dims if d.get('is_primary_time')]
        assert len(primary_dims) == 1
        assert primary_dims[0]['name'] == 'metric_time'
        
    def test_semantic_model_with_primary_time_dimension(self):
        """Test explicit semantic model with primary_time_dimension"""
        metrics_file = self.metrics_dir / "test_primary_time.yml"
        metrics_file.write_text("""
version: 2

semantic_models:
  - name: unified_metrics
    model: ref('fct_unified')
    primary_time_dimension: metric_time
    dimensions:
      - name: metric_time
        type: time
        type_params:
          time_granularity: day
        expr: COALESCE(order_date, event_date)
      - name: metric_time_month
        type: time
        type_params:
          time_granularity: month
        expr: DATE_TRUNC('month', COALESCE(order_date, event_date))

metrics:
  - name: unified_events
    type: simple
    source: fct_unified
    measure:
      type: count
      column: event_id
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check semantic model has primary_time_dimension
        semantic_model = next(sm for sm in compiler.semantic_models if sm['name'] == 'unified_metrics')
        assert semantic_model.get('primary_time_dimension') == 'metric_time'
        
        # Check dimensions are properly defined
        dim_names = [d['name'] for d in semantic_model['dimensions']]
        assert 'metric_time' in dim_names
        assert 'metric_time_month' in dim_names
        
    def test_cumulative_metric_with_metric_time(self):
        """Test cumulative metrics with metric_time"""
        metrics_file = self.metrics_dir / "test_cumulative_metric_time.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: cumulative_revenue
    type: cumulative
    measure:
      source: fct_orders
      type: sum
      column: revenue
    dimensions:
      - name: metric_time
        type: time
        grain: month
        expr: order_date
    grain_to_date: month
    window: unbounded
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check cumulative metric has metric_time
        metric = compiler.compiled_metrics[0]
        assert metric['type'] == 'cumulative'
        mt_dims = [d for d in metric['dimensions'] if d['name'] == 'metric_time']
        assert len(mt_dims) == 1
        assert mt_dims[0]['grain'] == 'month'
        
    def test_metric_time_without_expr(self):
        """Test metric_time without explicit expression"""
        metrics_file = self.metrics_dir / "test_metric_time_no_expr.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: simple_count
    type: simple
    source: fct_events
    measure:
      type: count
      column: event_id
    dimensions:
      - name: metric_time
        type: time
        grain: day
        # No expr specified - should use default
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check metric_time dimension exists
        metric = compiler.compiled_metrics[0]
        mt_dim = next(d for d in metric['dimensions'] if d['name'] == 'metric_time')
        assert mt_dim['type'] == 'time'
        assert mt_dim['grain'] == 'day'
        # When no expr is provided, it should be handled by the semantic layer