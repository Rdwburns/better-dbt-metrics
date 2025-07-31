# Semantic Models Guide

Better-DBT-Metrics now supports explicit semantic model definitions, making it a complete semantic layer toolkit for dbt. This guide covers how to define and use semantic models.

## Table of Contents
- [Overview](#overview)
- [Basic Semantic Model Definition](#basic-semantic-model-definition)
- [Semantic Model Templates](#semantic-model-templates)
- [Entity Management](#entity-management)
- [Metrics Referencing Semantic Models](#metrics-referencing-semantic-models)
- [Migration from Source-Based Metrics](#migration-from-source-based-metrics)
- [Best Practices](#best-practices)

## Overview

Semantic models define the structure of your data sources, including:
- **Entities**: Primary and foreign keys for joining
- **Dimensions**: Attributes for grouping and filtering
- **Measures**: Pre-aggregated calculations available for metrics

By defining semantic models explicitly, you get:
- Better control over dimension and measure definitions
- Reusability across multiple metrics
- Clearer separation between data modeling and metric definition
- Full compatibility with dbt's semantic layer

## Basic Semantic Model Definition

Here's a simple semantic model definition:

```yaml
version: 2

semantic_models:
  - name: orders
    description: "Order fact table"
    source: fct_orders  # This becomes ref('fct_orders')
    
    # Define entities (keys)
    entities:
      - name: order_id
        type: primary
        expr: order_id
        
      - name: customer_id
        type: foreign
        expr: customer_id
    
    # Define dimensions
    dimensions:
      - name: order_date
        type: time
        type_params:
          time_granularity: day
        expr: order_date
        
      - name: status
        type: categorical
        expr: order_status
    
    # Define measures
    measures:
      - name: order_count
        agg: count
        expr: order_id
        agg_time_dimension: order_date
        
      - name: total_revenue
        agg: sum
        expr: order_amount
        agg_time_dimension: order_date
```

## Semantic Model Templates

Just like metrics, semantic models support templates for reusability. Templates use Jinja2 syntax for powerful parameterization:

### Basic Template Example

```yaml
version: 2

# Define a template
semantic_model_templates:
  standard_fact_table:
    description: "Template for standard fact tables"
    parameters:
      - name: table_name
        type: string
        required: true
        description: "Source table name"
      - name: date_column
        type: string
        required: true
        description: "Primary date column"
      - name: primary_key
        type: string
        default: "id"
        description: "Primary key column"
    template:
      source: "{{ table_name }}"
      entities:
        - name: "{{ primary_key }}"
          type: primary
          expr: "{{ primary_key }}"
      dimensions:
        - name: date
          type: time
          type_params:
            time_granularity: day
          expr: "{{ date_column }}"
        - name: week
          type: time
          type_params:
            time_granularity: week
          expr: "date_trunc('week', {{ date_column }})"
        - name: month
          type: time
          type_params:
            time_granularity: month
          expr: "date_trunc('month', {{ date_column }})"

# Use the template
semantic_models:
  - name: transactions
    template: standard_fact_table
    parameters:
      table_name: fct_transactions
      date_column: transaction_date
      primary_key: transaction_id
    # Add additional fields beyond the template
    measures:
      - name: transaction_count
        agg: count
        expr: transaction_id
        agg_time_dimension: date
```

### Advanced Template Features

#### Conditional Logic

Templates support Jinja2 conditionals for flexible schemas:

```yaml
semantic_model_templates:
  flexible_fact:
    parameters:
      - name: table_name
        required: true
      - name: include_financial
        type: boolean
        default: false
      - name: amount_column
        default: "amount"
    template:
      source: "{{ table_name }}"
      measures:
        - name: record_count
          agg: count
          expr: "*"
        {% if include_financial %}
        - name: total_amount
          agg: sum
          expr: "{{ amount_column }}"
        - name: average_amount
          agg: avg
          expr: "{{ amount_column }}"
        {% endif %}
```

#### Loops and Lists

Generate multiple similar elements:

```yaml
semantic_model_templates:
  multi_currency_fact:
    parameters:
      - name: table_name
        required: true
      - name: currencies
        type: list
        default: ["USD", "EUR", "GBP"]
    template:
      source: "{{ table_name }}"
      measures:
        {% for currency in currencies %}
        - name: total_{{ currency|lower }}
          agg: sum
          expr: "amount_{{ currency }}"
          agg_time_dimension: date
        {% endfor %}
```

#### Template Composition

Combine templates with additional fields:

```yaml
semantic_models:
  - name: orders
    template: standard_fact_table
    parameters:
      table_name: fct_orders
      date_column: order_date
    # Template provides base structure
    # Add domain-specific elements
    entities:
      - name: customer_id
        type: foreign
        expr: customer_id
    dimensions:
      - name: order_status
        type: categorical
        expr: status
    measures:
      - name: revenue
        agg: sum
        expr: order_amount
        agg_time_dimension: date
```

## Entity Management

### Global Entity Definitions

Define reusable entities that can be referenced across semantic models:

```yaml
# Define reusable entities
entities:
  - name: customer_id
    type: foreign
    expr: customer_id
    
  - name: product_id
    type: foreign
    expr: product_id
```

### Entity Sets

Group related entities for common patterns:

```yaml
# Define entity sets
entity_sets:
  - name: ecommerce_fact
    primary_entity:
      name: id
      type: primary
    foreign_entities:
      - name: customer_id
        type: foreign
        expr: customer_id
      - name: product_id
        type: foreign
        expr: product_id
      - name: order_id
        type: foreign
        expr: order_id

# Use in semantic model
semantic_models:
  - name: order_items
    source: fct_order_items
    entity_set: ecommerce_fact  # Applies all entities from the set
    dimensions:
      - name: order_date
        type: time
        grain: day
```

## Metrics Referencing Semantic Models

Once you've defined semantic models, metrics can reference them instead of defining sources and measures inline:

```yaml
# Traditional approach (still supported)
metrics:
  - name: revenue_old_style
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: order_amount
    dimensions:
      - order_date
      - region

# New approach with semantic models (Recommended)
metrics:
  - name: revenue
    type: simple
    semantic_model: orders  # References the semantic model by name
    measure: total_revenue  # References a measure by name (not a dict)
    dimensions:
      - order_date
      - region
```

### Key Differences in the New Syntax:

1. **`semantic_model` instead of `source`**: Reference a semantic model definition
2. **`measure` as a string**: Reference a measure by name from the semantic model
3. **Automatic inheritance**: Dimensions and entities are available from the semantic model

### Cross-File References

Metrics can reference semantic models defined in different files:

```yaml
# semantic_models/customers.yml
semantic_models:
  - name: customers
    source: dim_customers
    measures:
      - name: customer_count
        agg: count_distinct
        expr: customer_id

# metrics/customer_metrics.yml
metrics:
  - name: active_customers
    type: simple
    semantic_model: customers  # Cross-file reference
    measure: customer_count
    filter: "is_active = true"
```

Benefits of the new approach:
- Measures are defined once and reused
- Consistent aggregation time dimensions
- Cleaner metric definitions
- Better separation of concerns
- Validation ensures measures exist

### Parameter Validation

Templates validate parameters at compile time:

```yaml
semantic_model_templates:
  validated_template:
    parameters:
      - name: table_name
        type: string
        required: true
      - name: metric_type
        type: string
        enum: ["revenue", "volume", "efficiency"]
        required: true
      - name: decimal_places
        type: number
        default: 2

# This will error - invalid metric_type
semantic_models:
  - name: bad_model
    template: validated_template
    parameters:
      table_name: fct_test
      metric_type: "invalid"  # Error: not in enum
```

### Template Best Practices

1. **Use descriptive parameter names** - Make it clear what each parameter controls
2. **Provide defaults where sensible** - Reduce boilerplate for common cases
3. **Document parameters** - Include description field for each parameter
4. **Validate inputs** - Use type and enum constraints
5. **Keep templates focused** - One template per pattern, compose as needed

## Migration from Source-Based Metrics

You can migrate incrementally from source-based metrics to semantic models:

### Step 1: Keep existing metrics as-is
Your existing metrics will continue to work. Better-DBT-Metrics will auto-generate semantic models for them.

### Step 2: Define semantic models for key sources
Start with your most-used sources:

```yaml
semantic_models:
  - name: orders
    source: fct_orders
    dimensions:
      # Copy dimensions from your metrics
    measures:
      # Define common measures
```

### Step 3: Update metrics to reference semantic models
Replace source/measure with semantic_model/measure references:

```yaml
# Before
metrics:
  - name: daily_revenue
    source: fct_orders
    measure:
      type: sum
      column: order_amount

# After  
metrics:
  - name: daily_revenue
    semantic_model: orders
    measure: total_revenue
```

## Best Practices

### 1. Organization

Structure your files logically:
```
metrics/
├── _semantic_models/       # Semantic model definitions
│   ├── facts/
│   │   ├── orders.yml
│   │   └── transactions.yml
│   └── dimensions/
│       ├── customers.yml
│       └── products.yml
├── finance/               # Domain-specific metrics
│   └── revenue.yml
└── product/
    └── engagement.yml
```

### 2. Naming Conventions

- Semantic models: Match the source table name (e.g., `orders` for `fct_orders`)
- Entities: Use `_id` suffix for keys (e.g., `customer_id`)
- Measures: Be descriptive (e.g., `total_revenue` not just `revenue`)
- Dimensions: Use business-friendly names

### 3. Measure Design

Define measures at the right granularity:
```yaml
measures:
  # Good: Specific, reusable measures
  - name: gross_revenue
    agg: sum
    expr: order_amount
    
  - name: net_revenue
    agg: sum
    expr: order_amount - discount_amount
    
  # Avoid: Overly specific measures
  - name: revenue_usa_2023  # Too specific
```

### 4. Time Dimensions

Always specify aggregation time dimensions:
```yaml
measures:
  - name: order_count
    agg: count
    expr: order_id
    agg_time_dimension: order_date  # Required for time-based aggregations
```

### 5. Use Templates for Consistency

Create templates for common patterns:
```yaml
semantic_model_templates:
  # Template for fact tables with standard dimensions
  fact_table:
    parameters: [table_name, date_column]
    template:
      source: "{{ table_name }}"
      dimensions:
        - name: date
          type: time
          grain: day
          expr: "{{ date_column }}"
        # Standard dimensions all fact tables should have
        
  # Template for slowly changing dimensions
  dimension_table:
    parameters: [table_name, primary_key]
    template:
      source: "{{ table_name }}"
      entities:
        - name: "{{ primary_key }}"
          type: primary
```

## Advanced Features

### Auto-Inference

Better-DBT-Metrics can automatically detect dimensions, entities, and measures based on column names and types:

```yaml
semantic_models:
  - name: orders
    source: fct_orders
    auto_infer:
      dimensions: true      # Auto-detect time and categorical dimensions
      entities: true        # Auto-detect primary/foreign keys
      measures: true        # Auto-detect numeric measures
      exclude_columns:      # Exclude specific columns
        - _fivetran_synced
        - internal_id
```

#### How Auto-Inference Works:

1. **Time Dimensions**: Detected by suffixes like `_date`, `_at`, `_timestamp` or date/timestamp data types
2. **Categorical Dimensions**: Detected by suffixes like `_type`, `_status`, `_category` or low cardinality
3. **Entities**: Detected by `_id` suffix and key constraints
4. **Measures**: Detected by numeric types and suffixes like `_amount`, `_value`, `_count`

#### Combining Manual and Auto-Inferred:

```yaml
semantic_models:
  - name: orders
    source: fct_orders
    
    # Manually define critical dimensions
    dimensions:
      - name: order_date
        type: time
        type_params:
          time_granularity: hour  # More specific than auto-inferred 'day'
    
    # Auto-infer additional dimensions
    auto_infer:
      dimensions: true
      exclude_columns: [order_date]  # Don't auto-infer what we defined manually
```

#### Customizing Inference Patterns:

Configure patterns in `bdm_config.yml`:

```yaml
auto_inference:
  time_dimension_patterns:
    suffix:
      - _created_at
      - _updated_at
      - _processed_date
  categorical_patterns:
    prefix:
      - product_
      - customer_
    max_cardinality: 50  # Lower threshold
```

### Join Paths

Define how semantic models relate:
```yaml
semantic_models:
  - name: orders
    source: fct_orders
    joins:
      - to: customers
        type: inner
        on: customer_id
```

### Primary Time Dimensions

Designate the main time dimension for metric_time:
```yaml
semantic_models:
  - name: orders
    primary_time_dimension: order_date
    dimensions:
      - name: order_date
        type: time
        is_primary: true
```

## Complete Example

Here's a complete example showing semantic models and metrics together:

```yaml
version: 2

# Import common components
imports:
  - ../_base/entities.yml as entities
  - ../_base/dimensions.yml as dims

# Define semantic models
semantic_models:
  - name: orders
    description: "Order transactions"
    source: fct_orders
    
    entities:
      - name: order_id
        type: primary
      - $ref: entities.customer
      - $ref: entities.product
    
    dimensions:
      - name: order_date
        type: time
        grain: day
      - $ref: dims.geography
      - name: order_status
        type: categorical
    
    measures:
      - name: order_count
        agg: count
        expr: order_id
        agg_time_dimension: order_date
        
      - name: gross_revenue
        agg: sum
        expr: order_amount
        agg_time_dimension: order_date
        
      - name: net_revenue
        agg: sum
        expr: order_amount - coalesce(discount_amount, 0)
        agg_time_dimension: order_date

# Define metrics using the semantic model
metrics:
  - name: daily_revenue
    type: simple
    description: "Daily gross revenue"
    semantic_model: orders
    measure: gross_revenue
    dimensions:
      - order_date
      - $ref: dims.geography
    
  - name: revenue_by_status
    type: simple
    description: "Revenue breakdown by order status"
    semantic_model: orders
    measure: net_revenue
    dimensions:
      - order_status
      - order_date
```

## Troubleshooting

### Common Issues

1. **"Missing semantic model" errors**
   - Ensure the semantic model is defined before metrics that reference it
   - Check that the semantic model name matches exactly

2. **"Unknown measure" errors**
   - Verify the measure is defined in the semantic model
   - Check for typos in the measure name

3. **Empty dimensions in output**
   - Make sure dimensions are properly defined with name and type
   - Check that dimension references are resolving correctly

### Debug Tips

Use the `--debug` flag to see how semantic models are processed:
```bash
better-dbt-metrics compile --debug
```

This will show:
- Template expansion details
- Entity resolution
- Dimension processing
- Measure compilation

## Next Steps

- Review the [examples/semantic_models](../examples/semantic_models) directory for more examples
- Read about [entity relationships](./entity-relationships.md) for complex joins
- Learn about [semantic model templates](./templates.md#semantic-model-templates) for advanced reuse