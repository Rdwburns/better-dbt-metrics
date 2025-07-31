"""Test semantic model template functionality"""

import pytest
import yaml
from pathlib import Path
import tempfile
import shutil

from core.compiler import BetterDBTCompiler, CompilerConfig


class TestSemanticModelTemplates:
    """Test semantic model template expansion and compilation"""
    
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
            debug=True
        )
        return BetterDBTCompiler(config)
    
    def test_basic_semantic_model_template(self, compiler, temp_dir):
        """Test basic semantic model template expansion"""
        # Create template file
        template_content = """
version: 2

semantic_model_templates:
  fact_table:
    parameters:
      - name: table_name
        type: string
        required: true
      - name: date_column
        type: string
        required: true
    template:
      source: "{{ table_name }}"
      entities:
        - name: id
          type: primary
          expr: id
      dimensions:
        - name: date
          type: time
          type_params:
            time_granularity: day
          expr: "{{ date_column }}"
      measures:
        - name: record_count
          agg: count
          expr: id
          agg_time_dimension: date
"""
        
        # Create semantic model using template
        model_content = """
version: 2

imports:
  - templates.yml

semantic_models:
  - name: transactions
    description: "Transaction fact table"
    template: fact_table
    parameters:
      table_name: fct_transactions
      date_column: transaction_date
"""
        
        # Write files
        with open(Path(temp_dir) / "templates.yml", "w") as f:
            f.write(template_content)
        
        with open(Path(temp_dir) / "models.yml", "w") as f:
            f.write(model_content)
        
        # Compile
        result = compiler.compile_directory()
        
        # Verify semantic model was created
        assert len(compiler.semantic_models) > 0
        
        # Find the transactions semantic model
        trans_model = None
        for sm in compiler.semantic_models:
            if 'transactions' in sm['name']:
                trans_model = sm
                break
        
        assert trans_model is not None
        assert trans_model['model'] == "ref('fct_transactions')"
        
        # Verify dimensions were expanded
        assert 'dimensions' in trans_model
        date_dim = next((d for d in trans_model['dimensions'] if d['name'] == 'date'), None)
        assert date_dim is not None
        assert date_dim['expr'] == 'transaction_date'
        
        # Verify measures were expanded
        assert 'measures' in trans_model
        count_measure = next((m for m in trans_model['measures'] if m['name'] == 'record_count'), None)
        assert count_measure is not None
    
    def test_template_with_optional_fields(self, compiler, temp_dir):
        """Test semantic model template with optional fields"""
        template_content = """
version: 2

semantic_model_templates:
  flexible_fact:
    parameters:
      - name: table_name
        type: string
        required: true
      - name: revenue_column
        type: string
        default: "revenue"
      - name: date_expr
        type: string
        default: "date"
    template:
      source: "{{ table_name }}"
      entities:
        - name: id
          type: primary
      dimensions:
        - name: date
          type: time
          type_params:
            time_granularity: day
          expr: "{{ date_expr }}"
      measures:
        - name: count
          agg: count
          expr: id
          agg_time_dimension: date
        - name: total_revenue
          agg: sum
          expr: "{{ revenue_column }}"
          agg_time_dimension: date
"""
        
        model_content = """
version: 2

imports:
  - templates.yml

semantic_models:
  - name: sales
    template: flexible_fact
    parameters:
      table_name: fct_sales
      revenue_column: sale_amount
      date_expr: sale_date
      
  - name: events
    template: flexible_fact
    parameters:
      table_name: fct_events
      # Uses default revenue_column and date_expr
"""
        
        # Write files
        with open(Path(temp_dir) / "templates.yml", "w") as f:
            f.write(template_content)
        
        with open(Path(temp_dir) / "models.yml", "w") as f:
            f.write(model_content)
        
        # Compile
        result = compiler.compile_directory()
        
        # Find the models
        sales_model = None
        events_model = None
        for sm in compiler.semantic_models:
            if 'sales' in sm['name']:
                sales_model = sm
            elif 'events' in sm['name']:
                events_model = sm
        
        # Sales should have revenue measure with custom column
        assert sales_model is not None
        revenue_measure = next((m for m in sales_model['measures'] if m['name'] == 'total_revenue'), None)
        assert revenue_measure is not None
        assert revenue_measure['expr'] == 'sale_amount'
        
        # Check date dimension has custom expression
        date_dim = next((d for d in sales_model['dimensions'] if d['name'] == 'date'), None)
        assert date_dim is not None
        assert date_dim['expr'] == 'sale_date'
        
        # Events should have revenue measure with default column
        assert events_model is not None
        revenue_measure = next((m for m in events_model['measures'] if m['name'] == 'total_revenue'), None)
        assert revenue_measure is not None
        assert revenue_measure['expr'] == 'revenue'  # Default value
    
    def test_template_inheritance(self, compiler, temp_dir):
        """Test semantic model templates with additional fields"""
        template_content = """
version: 2

semantic_model_templates:
  base_fact:
    parameters:
      - name: table_name
        type: string
        required: true
    template:
      source: "{{ table_name }}"
      entities:
        - name: id
          type: primary
      dimensions:
        - name: date
          type: time
          type_params:
            time_granularity: day
"""
        
        model_content = """
version: 2

imports:
  - templates.yml

semantic_models:
  - name: orders
    template: base_fact
    parameters:
      table_name: fct_orders
    # Additional fields beyond template
    dimensions:
      - name: region
        type: categorical
        expr: ship_region
    measures:
      - name: order_count
        agg: count
        expr: order_id
        agg_time_dimension: date
"""
        
        # Write files
        with open(Path(temp_dir) / "templates.yml", "w") as f:
            f.write(template_content)
        
        with open(Path(temp_dir) / "models.yml", "w") as f:
            f.write(model_content)
        
        # Compile
        result = compiler.compile_directory()
        
        # Find the orders model
        orders_model = None
        for sm in compiler.semantic_models:
            if 'orders' in sm['name']:
                orders_model = sm
                break
        
        assert orders_model is not None
        
        # Should have both template dimensions and additional dimensions
        dim_names = [d['name'] for d in orders_model['dimensions']]
        assert 'date' in dim_names  # From template
        assert 'region' in dim_names  # Additional
        
        # Should have the manually added measure
        assert len(orders_model['measures']) == 1
        assert orders_model['measures'][0]['name'] == 'order_count'
    
    def test_nested_template_references(self, compiler, temp_dir):
        """Test templates referencing dimension groups"""
        template_content = """
version: 2

dimension_groups:
  time_dimensions:
    dimensions:
      - name: date
        type: time
        type_params:
          time_granularity: day
      - name: week
        type: time
        type_params:
          time_granularity: week
      - name: month
        type: time
        type_params:
          time_granularity: month

semantic_model_templates:
  time_series_fact:
    parameters:
      - name: table_name
        type: string
        required: true
      - name: date_expr
        type: string
        default: "date"
    template:
      source: "{{ table_name }}"
      dimensions:
        - name: date
          type: time
          type_params:
            time_granularity: day
          expr: "{{ date_expr }}"
        - name: week
          type: time
          type_params:
            time_granularity: week
          expr: "date_trunc('week', {{ date_expr }})"
        - name: month
          type: time
          type_params:
            time_granularity: month
          expr: "date_trunc('month', {{ date_expr }})"
"""
        
        model_content = """
version: 2

imports:
  - templates.yml

semantic_models:
  - name: daily_stats
    template: time_series_fact
    parameters:
      table_name: fct_daily_stats
      date_expr: stat_date
    entities:
      - name: stat_id
        type: primary
    measures:
      - name: total_value
        agg: sum
        expr: value
        agg_time_dimension: date
"""
        
        # Write files
        with open(Path(temp_dir) / "templates.yml", "w") as f:
            f.write(template_content)
        
        with open(Path(temp_dir) / "models.yml", "w") as f:
            f.write(model_content)
        
        # Compile
        result = compiler.compile_directory()
        
        # Find the model
        stats_model = None
        for sm in compiler.semantic_models:
            if 'daily_stats' in sm['name']:
                stats_model = sm
                break
        
        assert stats_model is not None
        
        # Verify date expressions were properly templated
        date_dim = next((d for d in stats_model['dimensions'] if d['name'] == 'date'), None)
        assert date_dim['expr'] == 'stat_date'
        
        week_dim = next((d for d in stats_model['dimensions'] if d['name'] == 'week'), None)
        assert 'stat_date' in week_dim['expr']
    
    def test_template_parameter_validation(self, compiler, temp_dir):
        """Test that required parameters are validated"""
        template_content = """
version: 2

semantic_model_templates:
  strict_template:
    parameters:
      - name: table_name
        type: string
        required: true
      - name: primary_key
        type: string
        required: true
      - name: optional_field
        type: string
        default: "default_value"
    template:
      source: "{{ table_name }}"
      entities:
        - name: "{{ primary_key }}"
          type: primary
"""
        
        # Missing required parameter
        model_content = """
version: 2

imports:
  - templates.yml

semantic_models:
  - name: bad_model
    template: strict_template
    parameters:
      table_name: fct_test
      # Missing primary_key!
"""
        
        # Write files
        with open(Path(temp_dir) / "templates.yml", "w") as f:
            f.write(template_content)
        
        with open(Path(temp_dir) / "models.yml", "w") as f:
            f.write(model_content)
        
        # This should raise an error during compilation
        with pytest.raises(ValueError) as exc_info:
            compiler.compile_directory()
        
        assert "Required parameter 'primary_key' not provided" in str(exc_info.value)