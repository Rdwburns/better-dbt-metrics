"""Tests for fill nulls feature"""

import pytest
import yaml
from pathlib import Path
import tempfile
import shutil

from core.compiler import BetterDBTCompiler, CompilerConfig


class TestFillNulls:
    """Test fill nulls functionality for time series metrics"""
    
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
        
    def test_fill_nulls_with_zero(self):
        """Test filling nulls with zero"""
        
        metrics_content = """
version: 2

metrics:
  - name: daily_revenue
    description: "Daily revenue with gaps filled with zeros"
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: revenue
    dimensions:
      - name: date_day
        type: time
        grain: day
    fill_nulls_with: 0
"""
        
        metrics_file = self.metrics_dir / "revenue_metrics.yml"
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
        assert results['metrics_compiled'] == 1
        assert len(results['errors']) == 0
        
        # Check output
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            output = yaml.safe_load(f)
            
        # Find the metric
        metric = output['metrics'][0]
        assert metric['name'] == 'daily_revenue'
        
        # Check that fill_nulls_with is preserved
        assert 'config' in metric or 'meta' in metric
        if 'config' in metric:
            assert metric['config'].get('fill_nulls_with') == 0
        else:
            assert metric['meta'].get('fill_nulls_with') == 0
            
    def test_fill_nulls_with_previous(self):
        """Test filling nulls with previous value (forward fill)"""
        
        metrics_content = """
version: 2

metrics:
  - name: daily_active_users
    description: "Daily active users with forward fill"
    type: simple
    source: fct_user_activity
    measure:
      type: count_distinct
      column: user_id
    dimensions:
      - name: date_day
        type: time
        grain: day
    fill_nulls_with: previous
"""
        
        metrics_file = self.metrics_dir / "user_metrics.yml"
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
        assert results['metrics_compiled'] == 1
        assert len(results['errors']) == 0
        
        # Check output
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            output = yaml.safe_load(f)
            
        # Find the metric
        metric = output['metrics'][0]
        assert metric['name'] == 'daily_active_users'
        
        # Check that fill_nulls_with is preserved
        assert 'config' in metric or 'meta' in metric
        if 'config' in metric:
            assert metric['config'].get('fill_nulls_with') == 'previous'
        else:
            assert metric['meta'].get('fill_nulls_with') == 'previous'
            
    def test_fill_nulls_with_interpolate(self):
        """Test filling nulls with linear interpolation"""
        
        metrics_content = """
version: 2

metrics:
  - name: hourly_temperature
    description: "Hourly temperature with linear interpolation"
    type: simple
    source: fct_sensor_data
    measure:
      type: average
      column: temperature
    dimensions:
      - name: date_hour
        type: time
        grain: hour
    fill_nulls_with: interpolate
    config:
      interpolation_method: linear
"""
        
        metrics_file = self.metrics_dir / "sensor_metrics.yml"
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
        assert results['metrics_compiled'] == 1
        assert len(results['errors']) == 0
        
    def test_fill_nulls_with_custom_value(self):
        """Test filling nulls with a custom value"""
        
        metrics_content = """
version: 2

metrics:
  - name: inventory_level
    description: "Inventory level with custom fill value"
    type: simple
    source: fct_inventory
    measure:
      type: last_value
      column: quantity
    dimensions:
      - name: date_day
        type: time
        grain: day
      - name: warehouse_id
        type: categorical
    fill_nulls_with: -1  # -1 indicates data not available
"""
        
        metrics_file = self.metrics_dir / "inventory_metrics.yml"
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
        assert results['metrics_compiled'] == 1
        assert len(results['errors']) == 0
        
        # Check output
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            output = yaml.safe_load(f)
            
        # Find the metric
        metric = output['metrics'][0]
        assert metric['name'] == 'inventory_level'
        
        # Check that fill_nulls_with is preserved
        assert 'config' in metric or 'meta' in metric
        if 'config' in metric:
            assert metric['config'].get('fill_nulls_with') == -1
        else:
            assert metric['meta'].get('fill_nulls_with') == -1
            
    def test_fill_nulls_conditional(self):
        """Test conditional fill nulls based on dimension"""
        
        metrics_content = """
version: 2

metrics:
  - name: regional_sales
    description: "Regional sales with different fill strategies"
    type: simple
    source: fct_sales
    measure:
      type: sum
      column: amount
    dimensions:
      - name: date_day
        type: time
        grain: day
      - name: region
        type: categorical
    fill_nulls_with: 0
    config:
      fill_nulls_rules:
        - dimension: region
          value: "APAC"
          fill_with: previous  # APAC reports might be delayed
        - dimension: region
          value: "EU"
          fill_with: 0  # EU has complete reporting
"""
        
        metrics_file = self.metrics_dir / "regional_metrics.yml"
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
        assert results['metrics_compiled'] == 1
        assert len(results['errors']) == 0