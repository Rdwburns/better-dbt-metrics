"""
Tests for window functions in measures
"""

import pytest
import yaml
from pathlib import Path
import tempfile
import shutil

from src.core.compiler import BetterDBTCompiler, CompilerConfig


class TestWindowFunctions:
    """Test window function support in measures"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp()
        self.metrics_dir = Path(self.test_dir) / "metrics"
        self.metrics_dir.mkdir(parents=True)
        self.output_dir = Path(self.test_dir) / "output"
        
    def teardown_method(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.test_dir)
        
    def test_basic_window_function(self):
        """Test basic window function measure"""
        metrics_file = self.metrics_dir / "test_window.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: revenue_moving_avg
    type: simple
    source: fct_orders
    measure:
      type: window
      column: order_total
      window_function: "AVG({{ column }}) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)"
    dimensions:
      - name: order_date
        type: time
        grain: day
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check measure was compiled with window function
        semantic_model = compiler.semantic_models[0]
        measures = semantic_model['measures']
        window_measure = next(m for m in measures if m['name'] == 'revenue_moving_avg_measure')
        
        # Check window function expression
        assert 'AVG(order_total) OVER' in window_measure['expr']
        assert 'ROWS BETWEEN 6 PRECEDING AND CURRENT ROW' in window_measure['expr']
        assert window_measure['agg_params']['is_window_function'] == True
        
    def test_window_with_partition(self):
        """Test window function with partition"""
        metrics_file = self.metrics_dir / "test_partition.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: customer_rank
    type: simple
    source: fct_customers
    measure:
      type: window
      column: total_revenue
      window_function: "RANK() OVER (PARTITION BY customer_segment ORDER BY {{ column }} DESC)"
    dimensions:
      - name: customer_segment
        type: categorical
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check partition is preserved
        semantic_model = compiler.semantic_models[0]
        measure = next(m for m in semantic_model['measures'] if 'customer_rank' in m['name'])
        assert 'PARTITION BY customer_segment' in measure['expr']
        assert 'ORDER BY total_revenue DESC' in measure['expr']
        
    def test_window_with_post_aggregation(self):
        """Test window function with post-aggregation"""
        metrics_file = self.metrics_dir / "test_post_agg.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: latest_order_count
    type: simple
    source: fct_orders
    measure:
      type: window
      column: order_id
      window_function: |
        CASE 
          WHEN ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) = 1 
          THEN {{ column }}
          ELSE NULL 
        END
      aggregation: count_distinct
    dimensions:
      - name: customer_segment
        type: categorical
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check post-aggregation is applied
        semantic_model = compiler.semantic_models[0]
        measure = next(m for m in semantic_model['measures'] if 'latest_order' in m['name'])
        assert measure['agg'] == 'count_distinct'
        assert 'ROW_NUMBER() OVER' in measure['expr']
        
    def test_lead_lag_functions(self):
        """Test lead/lag window functions"""
        metrics_file = self.metrics_dir / "test_lead_lag.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: revenue_change
    type: simple
    source: fct_daily_summary
    measure:
      type: window
      column: daily_revenue
      window_function: "{{ column }} - LAG({{ column }}, 1, 0) OVER (ORDER BY date_day)"
    dimensions:
      - name: date_day
        type: time
        grain: day
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check LAG function is preserved
        semantic_model = compiler.semantic_models[0]
        measure = next(m for m in semantic_model['measures'] if 'revenue_change' in m['name'])
        assert 'LAG(daily_revenue, 1, 0)' in measure['expr']
        
    def test_cumulative_window(self):
        """Test cumulative sum with window function"""
        metrics_file = self.metrics_dir / "test_cumulative.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: cumulative_by_month
    type: simple
    source: fct_orders
    measure:
      type: window
      column: order_total
      window_function: |
        SUM({{ column }}) OVER (
          PARTITION BY DATE_TRUNC('month', order_date) 
          ORDER BY order_date 
          ROWS UNBOUNDED PRECEDING
        )
    dimensions:
      - name: order_date
        type: time
        grain: day
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check cumulative window is correct
        semantic_model = compiler.semantic_models[0]
        measure = next(m for m in semantic_model['measures'] if 'cumulative' in m['name'])
        assert 'SUM(order_total) OVER' in measure['expr']
        assert 'ROWS UNBOUNDED PRECEDING' in measure['expr']
        assert "DATE_TRUNC('month', order_date)" in measure['expr']
        
    def test_window_in_ratio_metric(self):
        """Test window functions in ratio metrics"""
        metrics_file = self.metrics_dir / "test_ratio_window.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: revenue_share
    type: ratio
    numerator:
      source: fct_customers
      measure:
        type: sum
        column: customer_revenue
    denominator:
      source: fct_customers
      measure:
        type: window
        column: customer_revenue
        window_function: "SUM({{ column }}) OVER (PARTITION BY customer_segment)"
        aggregation: max
    dimensions:
      - name: customer_segment
        type: categorical
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check ratio metric with window function
        metric = next(m for m in compiler.compiled_metrics if m['name'] == 'revenue_share')
        assert metric['type'] == 'ratio'
        
        # Check denominator has window function
        semantic_model = compiler.semantic_models[0]
        den_measure = next(m for m in semantic_model['measures'] if 'denominator' in m['name'])
        assert 'SUM(customer_revenue) OVER' in den_measure['expr']
        assert den_measure['agg'] == 'max'  # Post-aggregation
        
    def test_window_validation_error(self):
        """Test validation error for window function without expression"""
        metrics_file = self.metrics_dir / "test_invalid_window.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: invalid_window
    type: simple
    source: fct_orders
    measure:
      type: window
      column: order_total
      # Missing window_function
""")
        
        from src.validation.validator import MetricsValidator
        validator = MetricsValidator(str(self.test_dir))
        result = validator.validate_file(metrics_file)
        
        assert not result.is_valid
        assert any("must specify window_function" in e.message for e in result.errors)