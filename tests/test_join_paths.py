"""
Tests for join path configuration
"""

import pytest
import yaml
from pathlib import Path
import tempfile
import shutil

from src.core.compiler import BetterDBTCompiler, CompilerConfig


class TestJoinPaths:
    """Test join path configuration functionality"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp()
        self.metrics_dir = Path(self.test_dir) / "metrics"
        self.metrics_dir.mkdir(parents=True)
        self.output_dir = Path(self.test_dir) / "output"
        
    def teardown_method(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.test_dir)
        
    def test_basic_join_path(self):
        """Test basic join path definition"""
        metrics_file = self.metrics_dir / "test_joins.yml"
        metrics_file.write_text("""
version: 2

join_paths:
  - from: fct_orders
    to: dim_customers
    join_type: inner
    join_keys:
      - from_column: customer_id
        to_column: customer_id

metrics:
  - name: revenue_by_segment
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: order_total
    dimensions:
      - name: customer_segment
        source: dim_customers
        column: segment
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check join paths were registered
        assert len(compiler.join_paths) == 1
        assert compiler.join_paths[0]['from'] == 'fct_orders'
        assert compiler.join_paths[0]['to'] == 'dim_customers'
        
        # Check semantic model has joins
        semantic_model = compiler.semantic_models[0]
        assert 'joins' in semantic_model
        assert len(semantic_model['joins']) == 1
        
        join = semantic_model['joins'][0]
        assert join['name'] == 'dim_customers'
        assert join['type'] == 'inner'
        assert 'customer_id' in join['sql_on']
        
    def test_multi_hop_join_path(self):
        """Test multi-hop join path through intermediate table"""
        metrics_file = self.metrics_dir / "test_multi_hop.yml"
        metrics_file.write_text("""
version: 2

join_paths:
  - from: fct_order_items
    to: dim_customers
    through: fct_orders
    join_path:
      - from: fct_order_items
        to: fct_orders
        join_keys:
          - from_column: order_id
            to_column: order_id
      - from: fct_orders
        to: dim_customers
        join_keys:
          - from_column: customer_id
            to_column: customer_id

metrics:
  - name: items_by_customer_region
    type: simple
    source: fct_order_items
    measure:
      type: count
      column: item_id
    dimensions:
      - name: customer_region
        source: dim_customers
        column: region
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check multi-hop join was processed
        assert len(compiler.join_paths) == 1
        assert 'through' in compiler.join_paths[0]
        
        # Check semantic model has multiple joins
        semantic_model = compiler.semantic_models[0]
        assert 'joins' in semantic_model
        # Should have joins for both hops
        join_names = [j['name'] for j in semantic_model['joins']]
        assert 'fct_orders' in join_names or 'dim_customers' in join_names
        
    def test_join_with_conditions(self):
        """Test join path with additional conditions"""
        metrics_file = self.metrics_dir / "test_join_conditions.yml"
        metrics_file.write_text("""
version: 2

join_paths:
  - from: fct_transactions
    to: dim_accounts
    join_type: inner
    join_keys:
      - from_column: account_id
        to_column: account_id
    join_conditions:
      - "dim_accounts.is_active = true"
      - "dim_accounts.account_type IN ('checking', 'savings')"

metrics:
  - name: active_account_balance
    type: simple
    source: fct_transactions
    measure:
      type: sum
      column: amount
    dimensions:
      - name: account_type
        source: dim_accounts
        column: account_type
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check join conditions are included
        semantic_model = compiler.semantic_models[0]
        join = semantic_model['joins'][0]
        assert 'is_active = true' in join['sql_on']
        assert 'account_type IN' in join['sql_on']
        
    def test_join_path_aliases(self):
        """Test join path aliases for reuse"""
        metrics_file = self.metrics_dir / "test_join_aliases.yml"
        metrics_file.write_text("""
version: 2

join_path_aliases:
  customer_full:
    description: "Full customer information"
    paths:
      - from: fct_orders
        to: dim_customers
        join_type: left
        join_keys:
          - from_column: customer_id
            to_column: customer_id
      - from: dim_customers
        to: dim_segments
        join_type: left
        join_keys:
          - from_column: segment_id
            to_column: segment_id

metrics:
  - name: revenue_analysis
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: revenue
    join_paths: [customer_full]
    dimensions:
      - name: segment_name
        source: dim_segments
        column: name
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check join path alias was registered
        assert 'customer_full' in compiler.join_path_aliases
        
        # Check metric references join path alias
        metric = compiler.compiled_metrics[0]
        assert 'join_paths' in metric
        assert 'customer_full' in metric['join_paths']
        
    def test_explicit_semantic_model_joins(self):
        """Test explicit join configuration in semantic models"""
        metrics_file = self.metrics_dir / "test_explicit_joins.yml"
        metrics_file.write_text("""
version: 2

semantic_models:
  - name: sales_analysis
    model: ref('fct_sales')
    joins:
      - name: customer
        sql_on: "${fct_sales}.customer_id = ${customer}.customer_id"
        type: left
      - name: product
        sql_on: "${fct_sales}.product_id = ${product}.product_id"
        type: left
    dimensions:
      - name: sale_date
        type: time
        grain: day
      - name: customer_segment
        type: categorical
        expr: ${customer}.segment
      - name: product_category
        type: categorical
        expr: ${product}.category

metrics:
  - name: sales_total
    type: simple
    source: fct_sales
    measure:
      type: sum
      column: sale_amount
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check semantic model has explicit joins
        semantic_model = next(sm for sm in compiler.semantic_models if sm['name'] == 'sales_analysis')
        assert 'joins' in semantic_model
        assert len(semantic_model['joins']) == 2
        
        # Check join configuration
        customer_join = next(j for j in semantic_model['joins'] if j['name'] == 'customer')
        assert customer_join['type'] == 'left'
        assert '${fct_sales}.customer_id = ${customer}.customer_id' in customer_join['sql_on']
        
    def test_no_joins_when_not_needed(self):
        """Test that joins are not added when dimensions don't reference other sources"""
        metrics_file = self.metrics_dir / "test_no_joins.yml"
        metrics_file.write_text("""
version: 2

join_paths:
  - from: fct_orders
    to: dim_customers
    join_type: inner
    join_keys:
      - from_column: customer_id
        to_column: customer_id

metrics:
  - name: order_count
    type: simple
    source: fct_orders
    measure:
      type: count
      column: order_id
    dimensions:
      - name: order_date
        type: time
        grain: day
      - name: order_status
        type: categorical
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir)
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check semantic model doesn't have unnecessary joins
        semantic_model = compiler.semantic_models[0]
        # Should not have joins since no dimensions reference other sources
        assert 'joins' not in semantic_model or len(semantic_model.get('joins', [])) == 0