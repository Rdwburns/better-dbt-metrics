"""
Tests for offset windows in cumulative metrics
"""

import pytest
import yaml
from pathlib import Path
import tempfile
import shutil

from src.core.compiler import BetterDBTCompiler, CompilerConfig


class TestOffsetWindows:
    """Test offset window support in cumulative metrics"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp()
        self.metrics_dir = Path(self.test_dir) / "metrics"
        self.metrics_dir.mkdir(parents=True)
        self.output_dir = Path(self.test_dir) / "output"
        
    def teardown_method(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.test_dir)
        
    def test_basic_offset_window(self):
        """Test basic offset window in cumulative metric"""
        metrics_file = self.metrics_dir / "test_offset.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: revenue_mtd_with_offset
    type: cumulative
    source: fct_orders
    measure:
      type: sum
      column: order_total
    grain_to_date: day
    window: month
    offsets:
      - period: month
        offset: -1
        alias: last_month_mtd
    dimensions:
      - name: order_date
        type: time
        grain: day
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir),
            validate=False,
            split_files=False
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check offset was compiled
        metric = next(m for m in compiler.compiled_metrics if m['name'] == 'revenue_mtd_with_offset')
        assert metric['type'] == 'cumulative'
        
        # Check dbt metric has offset configuration
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            import yaml
            output = yaml.safe_load(f)
            
        dbt_metric = next(m for m in output['metrics'] if m['name'] == 'revenue_mtd_with_offset')
        cumulative_params = dbt_metric['type_params']['cumulative_type_params']
        assert 'offset_windows' in cumulative_params
        assert len(cumulative_params['offset_windows']) == 1
        
        offset = cumulative_params['offset_windows'][0]
        assert offset['period'] == 'month'
        assert offset['offset'] == -1
        assert offset['alias'] == 'last_month_mtd'
        
    def test_multiple_offsets(self):
        """Test multiple offset windows"""
        metrics_file = self.metrics_dir / "test_multiple_offsets.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: cumulative_users_comparisons
    type: cumulative
    source: fct_user_activity
    measure:
      type: count_distinct
      column: user_id
    grain_to_date: day
    window: unbounded
    offsets:
      - period: day
        offset: -7
        alias: week_ago
      - period: day
        offset: -30
        alias: month_ago
      - period: year
        offset: -1
        alias: year_ago
    dimensions:
      - name: activity_date
        type: time
        grain: day
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir),
            validate=False,
            split_files=False
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check all offsets were compiled
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            import yaml
            output = yaml.safe_load(f)
            
        dbt_metric = next(m for m in output['metrics'] if m['name'] == 'cumulative_users_comparisons')
        offsets = dbt_metric['type_params']['cumulative_type_params']['offset_windows']
        assert len(offsets) == 3
        
        # Check each offset
        assert offsets[0]['alias'] == 'week_ago'
        assert offsets[1]['alias'] == 'month_ago'
        assert offsets[2]['alias'] == 'year_ago'
        
    def test_offset_with_calculations(self):
        """Test offset window with calculations"""
        metrics_file = self.metrics_dir / "test_calculations.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: weekly_active_users_growth
    type: cumulative
    source: fct_user_activity
    measure:
      type: count_distinct
      column: user_id
    grain_to_date: day
    window: week
    offsets:
      - period: week
        offset: -1
        alias: last_week
        calculations:
          - type: difference
            alias: wow_change
          - type: percent_change
            alias: wow_growth_rate
    dimensions:
      - name: activity_date
        type: time
        grain: day
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir),
            validate=False,
            split_files=False
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check calculations were compiled
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            import yaml
            output = yaml.safe_load(f)
            
        dbt_metric = next(m for m in output['metrics'] if m['name'] == 'weekly_active_users_growth')
        offset = dbt_metric['type_params']['cumulative_type_params']['offset_windows'][0]
        assert 'calculations' in offset
        assert len(offset['calculations']) == 2
        assert offset['calculations'][0]['type'] == 'difference'
        assert offset['calculations'][1]['type'] == 'percent_change'
        
    def test_offset_with_filter_inheritance(self):
        """Test offset window with filter inheritance"""
        metrics_file = self.metrics_dir / "test_filter_inherit.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: premium_revenue_offset
    type: cumulative
    source: fct_orders
    measure:
      type: sum
      column: revenue
      filters:
        - "customer_tier = 'premium'"
    grain_to_date: day
    window: month
    offsets:
      - period: month
        offset: -1
        alias: last_month_premium
        inherit_filters: true
      - period: month
        offset: -1
        alias: last_month_all
        inherit_filters: false
    dimensions:
      - name: order_date
        type: time
        grain: day
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir),
            validate=False,
            split_files=False
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check filter inheritance settings
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            import yaml
            output = yaml.safe_load(f)
            
        dbt_metric = next(m for m in output['metrics'] if m['name'] == 'premium_revenue_offset')
        offsets = dbt_metric['type_params']['cumulative_type_params']['offset_windows']
        assert offsets[0]['inherit_filters'] == True
        assert offsets[1]['inherit_filters'] == False
        
    def test_offset_pattern(self):
        """Test using offset patterns"""
        metrics_file = self.metrics_dir / "test_pattern.yml"
        metrics_file.write_text("""
version: 2

offset_window_config:
  offset_patterns:
    standard_comparisons:
      - period: week
        offset: -1
        alias: last_week
      - period: month
        offset: -1
        alias: last_month
      - period: year
        offset: -1
        alias: last_year

metrics:
  - name: revenue_with_pattern
    type: cumulative
    source: fct_orders
    measure:
      type: sum
      column: revenue
    grain_to_date: day
    window: month
    offset_pattern: standard_comparisons
    dimensions:
      - name: order_date
        type: time
        grain: day
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir),
            validate=False,
            split_files=False
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check pattern was expanded
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            import yaml
            output = yaml.safe_load(f)
            
        dbt_metric = next(m for m in output['metrics'] if m['name'] == 'revenue_with_pattern')
        offsets = dbt_metric['type_params']['cumulative_type_params']['offset_windows']
        assert len(offsets) == 3
        assert offsets[0]['alias'] == 'last_week'
        assert offsets[1]['alias'] == 'last_month'
        assert offsets[2]['alias'] == 'last_year'
        
    def test_window_type_trailing(self):
        """Test trailing window type"""
        metrics_file = self.metrics_dir / "test_trailing.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: trailing_30d_revenue
    type: cumulative
    source: fct_orders
    measure:
      type: sum
      column: revenue
    grain_to_date: day
    window: 30
    window_type: trailing
    offsets:
      - period: day
        offset: -30
        alias: previous_30d_period
    dimensions:
      - name: order_date
        type: time
        grain: day
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir),
            validate=False,
            split_files=False
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check window type was set
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            import yaml
            output = yaml.safe_load(f)
            
        dbt_metric = next(m for m in output['metrics'] if m['name'] == 'trailing_30d_revenue')
        cumulative_params = dbt_metric['type_params']['cumulative_type_params']
        assert cumulative_params['window_type'] == 'trailing'
        assert cumulative_params['window'] == 30
        
    def test_fiscal_period_offset(self):
        """Test fiscal period offset"""
        metrics_file = self.metrics_dir / "test_fiscal.yml"
        metrics_file.write_text("""
version: 2

time_spine:
  fiscal:
    model: ref('dim_fiscal_calendar')
    columns:
      day: fiscal_date
      month: fiscal_month
      quarter: fiscal_quarter
      year: fiscal_year

metrics:
  - name: fiscal_ytd_offset
    type: cumulative
    source: fct_fiscal_orders
    measure:
      type: sum
      column: revenue
    grain_to_date: day
    window: fiscal_year
    time_spine: fiscal
    offsets:
      - period: fiscal_year
        offset: -1
        alias: prior_fiscal_ytd
    dimensions:
      - name: fiscal_date
        type: time
        grain: day
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir),
            validate=False,
            split_files=False
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check fiscal offset
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            import yaml
            output = yaml.safe_load(f)
            
        dbt_metric = next(m for m in output['metrics'] if m['name'] == 'fiscal_ytd_offset')
        offset = dbt_metric['type_params']['cumulative_type_params']['offset_windows'][0]
        assert offset['period'] == 'fiscal_year'
        
    def test_validation_error_non_cumulative(self):
        """Test validation error for offsets on non-cumulative metric"""
        metrics_file = self.metrics_dir / "test_invalid.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: simple_with_offset
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: revenue
    offsets:
      - period: month
        offset: -1
        alias: invalid
""")
        
        from src.validation.validator import MetricsValidator
        validator = MetricsValidator(str(self.test_dir))
        result = validator.validate_file(metrics_file)
        
        assert not result.is_valid
        assert any("Non-cumulative metric" in e.message for e in result.errors)
        
    def test_complex_calculation_expression(self):
        """Test offset with complex calculation expression"""
        metrics_file = self.metrics_dir / "test_complex_calc.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: qtd_revenue_yoy
    type: cumulative
    source: fct_orders
    measure:
      type: sum
      column: revenue
    grain_to_date: day
    window: quarter
    offsets:
      - period: year
        offset: -1
        alias: qtd_last_year
        calculation: |
          (current_value - offset_value) / NULLIF(offset_value, 0) * 100
        calculation_alias: yoy_growth_percent
    dimensions:
      - name: order_date
        type: time
        grain: day
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir),
            validate=False,
            split_files=False
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check calculation expression
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            import yaml
            output = yaml.safe_load(f)
            
        dbt_metric = next(m for m in output['metrics'] if m['name'] == 'qtd_revenue_yoy')
        offset = dbt_metric['type_params']['cumulative_type_params']['offset_windows'][0]
        assert 'calculation' in offset
        assert 'calculation_alias' in offset
        assert offset['calculation_alias'] == 'yoy_growth_percent'