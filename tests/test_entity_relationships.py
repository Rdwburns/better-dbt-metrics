"""
Tests for entity relationships in semantic models
"""

import pytest
import yaml
from pathlib import Path
import tempfile
import shutil

from src.core.compiler import BetterDBTCompiler, CompilerConfig


class TestEntityRelationships:
    """Test primary/foreign key relationships in semantic models"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp()
        self.metrics_dir = Path(self.test_dir) / "metrics"
        self.metrics_dir.mkdir(parents=True)
        self.output_dir = Path(self.test_dir) / "output"
        
    def teardown_method(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.test_dir)
        
    def test_basic_entity_definition(self):
        """Test basic entity definition with relationships"""
        metrics_file = self.metrics_dir / "test_entities.yml"
        metrics_file.write_text("""
version: 2

entities:
  - name: customer
    type: primary
    column: customer_id
    
  - name: order
    type: primary
    column: order_id
    relationships:
      - type: many_to_one
        to_entity: customer
        foreign_key: customer_id

metrics:
  - name: customer_revenue
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: order_total
    entity: customer
    dimensions:
      - name: customer_segment
        type: categorical
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir),
            validate=False
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check that entities were registered
        assert 'customer' in compiler.entities
        assert 'order' in compiler.entities
        assert compiler.entities['order']['relationships'][0]['to_entity'] == 'customer'
        
        # Check semantic model has correct entities
        semantic_model = compiler.semantic_models[0]
        entities = semantic_model['entities']
        entity_names = [e['name'] for e in entities]
        
        # Should have customer as primary and customer_id as foreign key
        assert 'customer' in entity_names
        assert any(e['type'] == 'foreign' and e['expr'] == 'customer_id' for e in entities)
        
    def test_entity_sets(self):
        """Test entity sets for complex relationships"""
        metrics_file = self.metrics_dir / "test_entity_sets.yml"
        metrics_file.write_text("""
version: 2

entities:
  - name: customer
    type: primary
    column: customer_id
    
  - name: order
    type: primary
    column: order_id
    relationships:
      - type: many_to_one
        to_entity: customer
        foreign_key: customer_id
        
  - name: order_item
    type: primary
    column: order_item_id
    relationships:
      - type: many_to_one
        to_entity: order
        foreign_key: order_id

entity_sets:
  - name: customer_orders
    description: "Customer and their orders"
    primary_entity: customer
    includes:
      - entity: order
        join_type: left
      - entity: order_item
        join_type: left
        through: order

semantic_models:
  - name: customer_analysis
    source: fct_orders
    entity_set: customer_orders
    description: "Customer order analysis"
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir),
            validate=False
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check entity set was registered
        assert 'customer_orders' in compiler.entity_sets
        
        # Check semantic model was created from entity set
        assert len(compiler.semantic_models) == 1
        semantic_model = compiler.semantic_models[0]
        assert semantic_model['name'] == 'sem_customer_analysis'
        
        # Check entities were properly extracted from entity set
        entities = semantic_model['entities']
        entity_names = [e['name'] for e in entities]
        assert 'customer' in entity_names
        
    def test_explicit_semantic_model_entities(self):
        """Test explicit entity definitions in semantic models"""
        metrics_file = self.metrics_dir / "test_explicit_entities.yml"
        metrics_file.write_text("""
version: 2

semantic_models:
  - name: product_performance
    source: fct_order_items
    description: "Product performance metrics"
    entities:
      - name: order_item
        type: primary
        expr: order_item_id
      - name: order_id
        type: foreign
        expr: order_id
        relationship:
          to_entity: order
          type: many_to_one
      - name: product_id
        type: foreign
        expr: product_id
        relationship:
          to_entity: product
          type: many_to_one

metrics:
  - name: product_revenue
    type: simple
    source: fct_order_items
    measure:
      type: sum
      column: item_revenue
    entity: order_item
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir),
            validate=False
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check semantic model has explicit entities
        assert len(compiler.semantic_models) >= 1
        semantic_model = next(sm for sm in compiler.semantic_models if sm['name'] == 'sem_product_performance')
        
        entities = semantic_model['entities']
        assert len(entities) == 3
        
        # Check primary entity
        primary = next(e for e in entities if e['name'] == 'order_item')
        assert primary['type'] == 'primary'
        assert primary['expr'] == 'order_item_id'
        
        # Check foreign keys with relationships
        order_fk = next(e for e in entities if e['name'] == 'order_id')
        assert order_fk['type'] == 'foreign'
        assert 'meta' in order_fk
        assert order_fk['meta']['relationship']['to_entity'] == 'order'
        
    def test_entity_relationships_in_metrics(self):
        """Test that metrics can reference entities and inherit relationships"""
        metrics_file = self.metrics_dir / "test_metric_entities.yml"
        metrics_file.write_text("""
version: 2

entities:
  - name: customer
    type: primary
    column: customer_id
    
  - name: product
    type: primary
    column: product_id
    
  - name: order_item
    type: primary
    column: order_item_id
    relationships:
      - type: many_to_one
        to_entity: customer
        foreign_key: customer_id
      - type: many_to_one
        to_entity: product
        foreign_key: product_id

metrics:
  - name: customer_product_revenue
    description: "Revenue by customer and product"
    type: simple
    source: fct_order_items
    measure:
      type: sum
      column: item_revenue
    entity: order_item
    dimensions:
      - name: customer_name
        type: categorical
        source: dim_customers
      - name: product_name
        type: categorical
        source: dim_products
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir),
            validate=False
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check semantic model includes all related entities
        semantic_model = compiler.semantic_models[0]
        entities = semantic_model['entities']
        entity_exprs = [e['expr'] for e in entities]
        
        # Should have foreign keys for related entities
        assert 'customer_id' in entity_exprs
        assert 'product_id' in entity_exprs
        
    def test_entity_inference_fallback(self):
        """Test that entity inference still works when no explicit entities are defined"""
        metrics_file = self.metrics_dir / "test_inference.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: order_count
    type: simple
    source: fct_orders
    measure:
      type: count
      column: order_id
    dimensions:
      - name: customer_id
        type: categorical
      - name: order_date
        type: time
        grain: day
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir),
            validate=False
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check that entities were inferred
        semantic_model = compiler.semantic_models[0]
        entities = semantic_model['entities']
        
        # Should have inferred customer_id as an entity
        assert any(e['name'] == 'customer_id' and e['type'] == 'primary' for e in entities)
        
    def test_conversion_metric_with_entities(self):
        """Test conversion metrics work with entity relationships"""
        metrics_file = self.metrics_dir / "test_conversion_entities.yml"
        metrics_file.write_text("""
version: 2

entities:
  - name: user
    type: primary
    column: user_id

metrics:
  - name: signup_to_purchase
    type: conversion
    base_measure:
      source: fct_signups
      measure:
        type: count
        column: signup_id
    conversion_measure:
      source: fct_purchases
      measure:
        type: count
        column: purchase_id
    entity: user
    window: 30 days
""")
        
        config = CompilerConfig(
            input_dir=str(self.metrics_dir),
            output_dir=str(self.output_dir),
            validate=False
        )
        compiler = BetterDBTCompiler(config)
        result = compiler.compile_directory()
        
        # Check metric was compiled correctly
        metric = next(m for m in compiler.compiled_metrics if m['name'] == 'signup_to_purchase')
        assert metric['type'] == 'conversion'
        assert metric.get('entity') == 'user'
        
        # For conversion metrics with multiple sources, check both sources have been processed
        # But they don't create a combined semantic model - this is expected behavior
        # The entity is stored in the metric itself
        assert 'entity' in metric
        assert metric['entity'] == 'user'