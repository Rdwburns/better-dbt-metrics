"""
Tests for time spine configuration
"""

import pytest
import yaml
from pathlib import Path
import tempfile
import shutil

from src.core.compiler import BetterDBTCompiler, CompilerConfig


class TestTimeSpine:
    """Test time spine configuration for metrics"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp()
        self.metrics_dir = Path(self.test_dir) / "metrics"
        self.metrics_dir.mkdir(parents=True)
        self.output_dir = Path(self.test_dir) / "output"
        
    def teardown_method(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.test_dir)
        
    def test_default_time_spine(self):
        """Test default time spine configuration"""
        metrics_file = self.metrics_dir / "test_time_spine.yml"
        metrics_file.write_text("""
version: 2

time_spine:
  default:
    model: ref('dim_date')
    columns:
      date_day: date_day
      date_week: date_week
      date_month: date_month
      date_year: date_year

metrics:
  - name: daily_revenue
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: revenue
    dimensions:
      - name: order_date
        type: time
        grain: day
    time_spine: default
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check time spine was registered
        assert 'default' in compiler.time_spines
        assert compiler.time_spines['default']['model'] == "ref('dim_date')"
        
        # Check semantic model has time spine configuration
        semantic_model = compiler.semantic_models[0]
        assert 'time_spine_table_configurations' in semantic_model
        time_configs = semantic_model['time_spine_table_configurations']
        
        # Should have configurations for each grain
        grains = [tc['grain'] for tc in time_configs]
        assert 'day' in grains
        assert 'week' in grains
        assert 'month' in grains
        assert 'year' in grains
        
        # Check location
        assert all(tc['location'] == "ref('dim_date')" for tc in time_configs)
        
    def test_custom_time_spine(self):
        """Test custom time spine for hourly metrics"""
        metrics_file = self.metrics_dir / "test_hourly_spine.yml"
        metrics_file.write_text("""
version: 2

time_spine:
  hourly:
    model: ref('dim_datetime')
    columns:
      datetime_hour: datetime_hour
      date_day: date_day
    meta:
      timezone: UTC

metrics:
  - name: hourly_traffic
    type: simple
    source: fct_page_views
    measure:
      type: count
      column: page_view_id
    dimensions:
      - name: view_datetime
        type: time
        grain: hour
    time_spine: hourly
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check custom spine was registered
        assert 'hourly' in compiler.time_spines
        assert compiler.time_spines['hourly']['meta']['timezone'] == 'UTC'
        
        # Check metric configuration
        metric = next(m for m in compiler.compiled_metrics if m['name'] == 'hourly_traffic')
        assert metric['time_spine'] == 'hourly'
        
        # Check compiled metric has time spine in config
        output_metrics = yaml.safe_load((self.output_dir / "_metrics.yml").read_text())
        compiled_metric = next(m for m in output_metrics['metrics'] if m['name'] == 'hourly_traffic')
        assert compiled_metric['config']['time_spine'] == 'hourly'
        
    def test_inline_time_spine(self):
        """Test inline time spine definition in metric"""
        metrics_file = self.metrics_dir / "test_inline_spine.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: manufacturing_output
    type: simple
    source: fct_production
    measure:
      type: sum
      column: units_produced
    dimensions:
      - name: production_date
        type: time
        grain: day
    time_spine:
      model: ref('dim_manufacturing_calendar')
      columns:
        manufacturing_date: mfg_date
        manufacturing_week: mfg_week
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check semantic model has inline time spine
        semantic_model = compiler.semantic_models[0]
        assert 'time_spine_table_configurations' in semantic_model
        time_configs = semantic_model['time_spine_table_configurations']
        
        # Should have configuration from inline definition
        assert len(time_configs) > 0
        assert time_configs[0]['location'] == "ref('dim_manufacturing_calendar')"
        
    def test_fiscal_calendar_spine(self):
        """Test fiscal calendar time spine"""
        metrics_file = self.metrics_dir / "test_fiscal_spine.yml"
        metrics_file.write_text("""
version: 2

time_spine:
  fiscal:
    model: ref('dim_fiscal_calendar')
    columns:
      fiscal_date: fiscal_date
      fiscal_month: fiscal_month
      fiscal_quarter: fiscal_quarter
      fiscal_year: fiscal_year
    meta:
      fiscal_year_start_month: 4
      calendar_type: fiscal

metrics:
  - name: fiscal_revenue
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: revenue
    dimensions:
      - name: order_date
        type: time
        grain: month
    time_spine: fiscal
    config:
      fiscal_alignment: true
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check fiscal spine metadata
        assert 'fiscal' in compiler.time_spines
        fiscal_spine = compiler.time_spines['fiscal']
        assert fiscal_spine['meta']['fiscal_year_start_month'] == 4
        assert fiscal_spine['meta']['calendar_type'] == 'fiscal'
        
        # Check semantic model has fiscal metadata
        semantic_model = compiler.semantic_models[0]
        time_configs = semantic_model['time_spine_table_configurations']
        fiscal_configs = [tc for tc in time_configs if 'meta' in tc]
        assert len(fiscal_configs) > 0
        assert fiscal_configs[0]['meta']['fiscal_year_start_month'] == 4
        
    def test_implicit_time_spine(self):
        """Test that metrics with time dimensions get default spine if defined"""
        metrics_file = self.metrics_dir / "test_implicit_spine.yml"
        metrics_file.write_text("""
version: 2

time_spine:
  default:
    model: ref('dim_date')
    columns:
      date_day: date_day
      date_month: date_month

metrics:
  - name: daily_sales
    type: simple
    source: fct_sales
    measure:
      type: sum
      column: sales_amount
    dimensions:
      - name: sale_date
        type: time
        grain: day
    # No explicit time_spine, should use default
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check semantic model gets default time spine
        semantic_model = compiler.semantic_models[0]
        assert 'time_spine_table_configurations' in semantic_model
        time_configs = semantic_model['time_spine_table_configurations']
        
        # Should have default spine configurations
        assert len(time_configs) > 0
        assert all(tc['location'] == "ref('dim_date')" for tc in time_configs)
        
    def test_explicit_semantic_model_time_spine(self):
        """Test explicit time spine in semantic model definition"""
        metrics_file = self.metrics_dir / "test_explicit_sm_spine.yml"
        metrics_file.write_text("""
version: 2

semantic_models:
  - name: orders_timeseries
    source: fct_orders
    description: "Orders with complete time spine"
    time_spine_table_configurations:
      - location: ref('dim_date')
        column_name: date_day
        grain: day
      - location: ref('dim_fiscal_calendar')
        column_name: fiscal_date
        grain: day
        meta:
          calendar_type: fiscal

metrics:
  - name: order_count
    type: simple
    source: fct_orders
    measure:
      type: count
      column: order_id
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check semantic model has explicit time spine configurations
        semantic_model = next(sm for sm in compiler.semantic_models if sm['name'] == 'orders_timeseries')
        assert 'time_spine_table_configurations' in semantic_model
        time_configs = semantic_model['time_spine_table_configurations']
        
        # Should have both regular and fiscal calendar
        assert len(time_configs) == 2
        assert any(tc['location'] == "ref('dim_date')" for tc in time_configs)
        assert any(tc['location'] == "ref('dim_fiscal_calendar')" for tc in time_configs)
        
        # Check fiscal metadata
        fiscal_config = next(tc for tc in time_configs if tc['location'] == "ref('dim_fiscal_calendar')")
        assert fiscal_config['meta']['calendar_type'] == 'fiscal'
        
    def test_cumulative_metric_with_time_spine(self):
        """Test cumulative metrics automatically use time spine"""
        metrics_file = self.metrics_dir / "test_cumulative_spine.yml"
        metrics_file.write_text("""
version: 2

time_spine:
  default:
    model: ref('dim_date')
    columns:
      date_day: date_day
      date_month: date_month

metrics:
  - name: cumulative_revenue
    type: cumulative
    measure:
      source: fct_orders
      type: sum
      column: revenue
    grain_to_date: month
    window: unbounded
    time_spine: default
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check cumulative metric has time spine
        metric = next(m for m in compiler.compiled_metrics if m['name'] == 'cumulative_revenue')
        assert metric['time_spine'] == 'default'
        
        # Check output
        output_metrics = yaml.safe_load((self.output_dir / "_metrics.yml").read_text())
        compiled_metric = next(m for m in output_metrics['metrics'] if m['name'] == 'cumulative_revenue')
        assert compiled_metric['config']['time_spine'] == 'default'