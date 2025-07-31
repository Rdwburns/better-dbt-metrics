# Entity Sets Guide

Entity sets in Better-DBT-Metrics allow you to define reusable groups of entities that can be applied to multiple semantic models. This promotes consistency and reduces duplication when modeling complex data relationships.

## Table of Contents
- [Overview](#overview)
- [Basic Entity Set Definition](#basic-entity-set-definition)
- [Global Entity Definitions](#global-entity-definitions)
- [Entity Set Types](#entity-set-types)
- [Using Entity Sets in Semantic Models](#using-entity-sets-in-semantic-models)
- [Advanced Features](#advanced-features)
- [Best Practices](#best-practices)
- [Complete Example](#complete-example)

## Overview

Entity sets solve common problems in data modeling:

- **Consistency**: Ensure the same entities are used across related semantic models
- **Reusability**: Define entity groups once and use them across multiple models
- **Maintainability**: Update entity definitions in one place
- **Clarity**: Make relationships between entities explicit

### Before Entity Sets
```yaml
# Repetitive entity definitions across models
semantic_models:
  - name: orders
    entities:
      - name: order_id
        type: primary
      - name: customer_id
        type: foreign
      - name: product_id
        type: foreign
        
  - name: transactions
    entities:
      - name: transaction_id
        type: primary
      - name: customer_id  # Duplicate definition
        type: foreign
      - name: product_id   # Duplicate definition
        type: foreign
```

### With Entity Sets
```yaml
# Define once, use everywhere
entity_sets:
  - name: ecommerce_base
    primary_entity:
      name: record_id
      type: primary
    foreign_entities:
      - customer_id
      - product_id

semantic_models:
  - name: orders
    entity_set: ecommerce_base  # Automatic entity application
    # ... rest of model definition
    
  - name: transactions
    entity_set: ecommerce_base  # Same entities, no duplication
    # ... rest of model definition
```

## Basic Entity Set Definition

Entity sets are defined at the top level of your YAML files:

```yaml
entity_sets:
  - name: ecommerce_fact
    description: "Standard entities for ecommerce fact tables"
    primary_entity:
      name: transaction_id
      type: primary
      expr: transaction_id
    foreign_entities:
      - name: customer_id
        type: foreign
        expr: customer_id
      - name: product_id
        type: foreign
        expr: product_id
```

### Entity Set Structure

- **name**: Unique identifier for the entity set
- **description**: Optional documentation
- **primary_entity**: The main entity (usually the grain of your fact table)
- **foreign_entities**: List of related entities for joins and relationships

## Global Entity Definitions

You can define entities globally and reference them in entity sets:

```yaml
# Global entity definitions
entities:
  - name: customer_id
    type: foreign
    expr: customer_id
    column: customer_id
    description: "Customer identifier"
    
  - name: product_id
    type: foreign
    expr: product_id
    column: product_id
    description: "Product identifier"

# Entity set referencing global entities
entity_sets:
  - name: sales_fact
    primary_entity:
      name: sale_id
      type: primary
      expr: sale_id
    foreign_entities:
      - customer_id  # References global definition
      - product_id   # References global definition
```

### Benefits of Global Entities

1. **Consistency**: Same entity definition across all uses
2. **Documentation**: Single place to document entity meaning
3. **Relationships**: Define relationships once, inherit everywhere
4. **Maintenance**: Update entity logic in one place

## Entity Set Types

### 1. Simple Entity Sets

Basic grouping of entities with inline definitions:

```yaml
entity_sets:
  - name: simple_transaction
    primary_entity:
      name: txn_id
      type: primary
      expr: transaction_id
    foreign_entities:
      - name: user_id
        type: foreign
        expr: user_id
```

### 2. Reference-Based Entity Sets

Using global entity references:

```yaml
entity_sets:
  - name: reference_based
    primary_entity: order_id  # References global entity
    foreign_entities:
      - customer_id
      - product_id
      - store_id
```

### 3. Entity Sets with Includes

Complex relationships with join specifications:

```yaml
entity_sets:
  - name: customer_journey
    primary_entity: customer_id
    includes:
      - entity: order_id
        join_type: left
        description: "Customer orders"
      - entity: order_item_id
        through: order_id  # Multi-hop relationship
        join_type: left
        description: "Order items through orders"
```

## Using Entity Sets in Semantic Models

### Basic Usage

Apply an entity set to a semantic model:

```yaml
semantic_models:
  - name: sales_data
    description: "Sales fact table"
    source: fct_sales
    entity_set: ecommerce_fact  # Applies all entities from the set
    
    dimensions:
      - name: sale_date
        type: time
        grain: day
    
    measures:
      - name: sales_count
        agg: count
        expr: sale_id
```

### Combining with Additional Entities

Entity sets don't limit you - add more entities as needed:

```yaml
semantic_models:
  - name: enhanced_sales
    source: fct_sales
    entity_set: ecommerce_fact  # Base entities
    
    # Additional entities beyond the set
    entities:
      - name: promotion_id
        type: foreign
        expr: promotion_id
      - name: store_id
        type: foreign
        expr: store_id
```

### Entity Set Inheritance

Entities from sets are merged with existing entities:

```yaml
semantic_models:
  - name: sales_with_context
    source: fct_sales
    entity_set: ecommerce_fact
    
    # These entities are added to those from the entity set
    entities:
      - name: campaign_id
        type: foreign
        expr: marketing_campaign_id
        
    # Result: sale_id (primary), customer_id, product_id, campaign_id
```

## Advanced Features

### Entity Relationships in Sets

Define relationships within entity sets:

```yaml
entities:
  - name: customer
    type: primary
    column: customer_id
    relationships:
      - type: one_to_many
        to_entity: order
        foreign_key: customer_id
        
  - name: order
    type: primary
    column: order_id
    relationships:
      - type: many_to_one
        to_entity: customer
        foreign_key: customer_id

entity_sets:
  - name: customer_orders
    primary_entity: customer
    includes:
      - entity: order
        join_type: inner  # Specify join behavior
```

### Join Path Metadata

Entity sets can include join path information:

```yaml
entity_sets:
  - name: complex_joins
    primary_entity: customer_id
    includes:
      - entity: order_id
        join_type: left
        join_condition: "customers.customer_id = orders.customer_id"
      - entity: order_item_id
        through: order_id
        join_type: left
        join_condition: "orders.order_id = order_items.order_id"
```

### Conditional Entity Inclusion

Use entity sets dynamically:

```yaml
entity_sets:
  - name: flexible_fact
    primary_entity: record_id
    foreign_entities:
      - customer_id
      - product_id
    includes:
      - entity: store_id
        condition: "{{ include_stores | default(false) }}"
      - entity: promotion_id
        condition: "{{ include_promotions | default(false) }}"
```

## Best Practices

### 1. Naming Conventions

- Entity sets: Use descriptive names indicating the domain (`ecommerce_fact`, `customer_journey`)
- Entities: Use consistent naming patterns (`customer_id`, `product_id`, `order_id`)

### 2. Organization

Structure your files for clarity:

```
metrics/
├── _base/
│   ├── entities.yml        # Global entity definitions
│   └── entity_sets.yml     # Reusable entity sets
├── ecommerce/
│   ├── sales.yml          # Semantic models using entity sets
│   └── orders.yml
└── customer/
    └── analytics.yml
```

### 3. Documentation

Document entity sets and their intended use:

```yaml
entity_sets:
  - name: ecommerce_transaction
    description: |
      Standard entity set for ecommerce transaction tables.
      Includes customer and product relationships.
      Use for: sales, orders, returns, refunds
    primary_entity:
      name: transaction_id
      type: primary
      description: "Unique transaction identifier"
```

### 4. Gradual Adoption

Start with simple entity sets and expand:

1. **Phase 1**: Define basic entity sets for your most common patterns
2. **Phase 2**: Add global entities and references
3. **Phase 3**: Include complex relationships and join paths
4. **Phase 4**: Add conditional logic and advanced features

### 5. Entity Set Composition

Design entity sets to be composable:

```yaml
entity_sets:
  # Base commerce entities
  - name: commerce_base
    foreign_entities:
      - customer_id
      - product_id
  
  # Time-based entities
  - name: time_series_base
    primary_entity:
      name: event_id
      type: primary
    foreign_entities:
      - date_id
      - time_id
  
  # Combined entity set
  - name: commerce_time_series
    extends: [commerce_base, time_series_base]  # Future feature
```

## Complete Example

Here's a comprehensive example showing entity sets in action:

```yaml
version: 2

# Global entities with relationships
entities:
  - name: customer_id
    type: foreign
    expr: customer_id
    description: "Customer identifier"
    
  - name: product_id
    type: foreign
    expr: product_id
    description: "Product identifier"
    
  - name: order_id
    type: primary
    expr: order_id
    description: "Order identifier"
    relationships:
      - type: many_to_one
        to_entity: customer_id
        foreign_key: customer_id

# Entity sets for different analysis patterns
entity_sets:
  # Simple transaction analysis
  - name: transaction_base
    description: "Basic transaction entities"
    primary_entity:
      name: transaction_id
      type: primary
      expr: transaction_id
    foreign_entities:
      - customer_id
      - product_id
  
  # Customer journey analysis
  - name: customer_journey
    description: "Complete customer journey entities"
    primary_entity: customer_id
    includes:
      - entity: order_id
        join_type: left
      - entity: product_id
        through: order_id
        join_type: left

# Semantic models using entity sets
semantic_models:
  - name: transactions
    description: "Transaction fact table"
    source: fct_transactions
    entity_set: transaction_base
    
    dimensions:
      - name: transaction_date
        type: time
        grain: day
        expr: transaction_date
    
    measures:
      - name: transaction_count
        agg: count
        expr: transaction_id
        agg_time_dimension: transaction_date
      - name: revenue
        agg: sum
        expr: transaction_amount
        agg_time_dimension: transaction_date
  
  - name: customer_analysis
    description: "Customer analytics with journey data"
    source: dim_customers
    entity_set: customer_journey
    
    dimensions:
      - name: customer_segment
        type: categorical
        expr: customer_segment
    
    measures:
      - name: customer_count
        agg: count_distinct
        expr: customer_id

# Metrics using the semantic models
metrics:
  - name: daily_revenue
    type: simple
    semantic_model: transactions
    measure: revenue
    dimensions:
      - transaction_date
  
  - name: customers_by_segment
    type: simple
    semantic_model: customer_analysis
    measure: customer_count
    dimensions:
      - customer_segment
```

## Troubleshooting

### Common Issues

1. **Entity set not found**
   ```
   Warning: Entity set 'my_set' not found for model 'my_model'
   ```
   - Check entity set name spelling
   - Ensure entity set is defined before use
   - Verify entity set is in the same file or properly imported

2. **Duplicate entities**
   - Entity sets merge with existing entities
   - Duplicates are automatically handled (no conflict)
   - Use unique names to avoid confusion

3. **Missing entity references**
   ```
   Warning: Global entity 'customer_id' not found
   ```
   - Define global entities before referencing in entity sets
   - Check entity name spelling
   - Ensure entities are in the same file or imported

### Debug Tips

Enable debug mode to see entity set application:

```bash
better-dbt-metrics compile --debug
```

This shows:
- Entity set resolution
- Entity merging process
- Final entity list for each semantic model

## Migration Guide

### From Manual Entities to Entity Sets

1. **Identify Patterns**: Find repeated entity definitions across models
2. **Extract Common Sets**: Create entity sets for common patterns
3. **Update Models**: Replace manual entities with entity set references
4. **Test**: Verify compiled output matches expectations

### Example Migration

**Before:**
```yaml
semantic_models:
  - name: orders
    entities:
      - name: order_id
        type: primary
      - name: customer_id
        type: foreign
      - name: product_id
        type: foreign
        
  - name: returns
    entities:
      - name: return_id
        type: primary
      - name: customer_id  # Duplicate
        type: foreign
      - name: product_id   # Duplicate
        type: foreign
```

**After:**
```yaml
entity_sets:
  - name: commerce_base
    foreign_entities:
      - customer_id
      - product_id

semantic_models:
  - name: orders
    entity_set: commerce_base
    entities:
      - name: order_id
        type: primary
        
  - name: returns
    entity_set: commerce_base
    entities:
      - name: return_id
        type: primary
```

Entity sets make your semantic models more maintainable, consistent, and easier to understand. Start with simple patterns and gradually adopt more advanced features as your data modeling needs grow.