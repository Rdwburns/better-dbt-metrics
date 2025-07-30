# Table References Guide

This guide explains how to use table references in Better-DBT-Metrics to make navigation between metrics and their source models easier.

## Overview

Better-DBT-Metrics now supports multiple ways to reference tables in your metrics definitions. These references are preserved in the compiled output, making it easier to navigate from metrics to their source models in your IDE or editor.

## Supported Formats

### 1. Traditional String Format
The simplest format - just specify the table name as a string:

```yaml
metrics:
  - name: total_revenue
    source: fct_orders
    measure:
      type: sum
      column: order_total
```

### 2. ref() Function Format
Use the familiar dbt `ref()` function syntax:

```yaml
metrics:
  - name: total_revenue
    source: ref('fct_orders')
    measure:
      type: sum
      column: order_total
```

This format is parsed and the table name is extracted, but the reference metadata is preserved in the compiled output.

### 3. $table() Function Format
An alternative function syntax specific to Better-DBT-Metrics:

```yaml
metrics:
  - name: total_revenue
    source: $table('fct_orders')
    measure:
      type: sum
      column: order_total
```

### 4. Dictionary Format with ref
Use a dictionary with a `ref` key:

```yaml
metrics:
  - name: total_revenue
    source:
      ref: fct_orders
    measure:
      type: sum
      column: order_total
```

### 5. Dictionary Format with $table
Use a dictionary with a `$table` key:

```yaml
metrics:
  - name: total_revenue
    source:
      $table: fct_orders
    measure:
      type: sum
      column: order_total
```

## Ratio Metrics with Different Sources

For ratio metrics where numerator and denominator come from different tables, specify sources at the component level:

```yaml
metrics:
  - name: profit_margin
    type: ratio
    numerator:
      source: ref('fct_financials')
      measure:
        type: sum
        column: gross_profit
    denominator:
      source: ref('fct_orders')
      measure:
        type: sum
        column: revenue
```

For ratio metrics where both components use the same table, you can specify the source at the metric level:

```yaml
metrics:
  - name: average_order_value
    type: ratio
    source: ref('fct_orders')  # Single source for both
    numerator:
      measure:
        type: sum
        column: order_total
    denominator:
      measure:
        type: count
        column: order_id
```

## Template Usage

Table references work seamlessly with templates:

```yaml
metrics:
  - name: product_revenue
    template: revenue_template
    parameters:
      SOURCE_TABLE: ref('fct_order_items')
      AMOUNT_COLUMN: item_revenue

metric_templates:
  revenue_template:
    parameters:
      - SOURCE_TABLE
      - AMOUNT_COLUMN
    template:
      type: simple
      source: "{{ SOURCE_TABLE }}"
      measure:
        type: sum
        column: "{{ AMOUNT_COLUMN }}"
```

## Compiled Output

When metrics are compiled, table reference metadata is preserved in the output:

```yaml
metrics:
  - name: total_revenue
    # ... other fields ...
    meta:
      source_ref:
        table: fct_orders
        type: ref
```

This metadata can be used by IDEs, documentation generators, or other tools to create navigation links between metrics and their source models.

## Benefits

1. **Better Navigation**: IDEs can use the reference metadata to provide "Go to Definition" functionality
2. **Documentation**: Tools can automatically generate links between metrics and models
3. **Validation**: The compiler can validate that referenced tables exist in your dbt project
4. **Consistency**: Use the same `ref()` syntax as dbt for familiarity

## Best Practices

1. **Use ref() for dbt models**: When referencing dbt models, use the `ref()` syntax to maintain consistency with dbt
2. **Be consistent**: Choose one format and stick with it across your project
3. **Document sources**: For external tables not managed by dbt, consider adding comments explaining the source

## Migration

Existing metrics with string sources will continue to work. To add navigation support, simply wrap the source in a `ref()` call:

```yaml
# Before
source: fct_orders

# After
source: ref('fct_orders')
```

The compiled output will be identical except for the added metadata, so this change is backward compatible.