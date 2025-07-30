# Metric Reuse and Deduplication Guide

This guide explains how Better-DBT-Metrics handles metric reuse and deduplication to create more efficient and maintainable metric definitions.

## Overview

Better-DBT-Metrics automatically detects duplicate metrics and reuses them where possible. This helps:
- Reduce compilation time
- Minimize the number of metrics in your semantic layer
- Ensure consistency across related metrics
- Simplify maintenance

## How Deduplication Works

### Automatic Detection

The compiler generates a unique signature for each metric based on:
- Type (simple, ratio, derived, etc.)
- Source table
- Measure configuration
- Filters
- Dimensions

When multiple metrics have the same signature, only one is created and others reference it.

### Example

```yaml
metrics:
  # This metric will be created
  - name: total_revenue
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: revenue

  # This ratio metric's denominator will reuse 'total_revenue'
  - name: profit_margin
    type: ratio
    numerator:
      source: fct_orders
      measure:
        type: sum
        column: profit
    denominator:
      source: fct_orders
      measure:
        type: sum
        column: revenue  # Same as total_revenue
```

In this example, instead of creating `profit_margin_denominator`, the compiler will reuse `total_revenue`.

## Metric References

### In Derived Metrics

Use the `metric()` function to reference other metrics:

```yaml
metrics:
  - name: revenue_per_order
    type: derived
    expression: |
      metric('total_revenue') / metric('order_count')
```

### In Templates

Templates can accept metric names as parameters:

```yaml
metric_templates:
  growth_rate:
    parameters:
      - name: base_metric
        type: string
    template:
      type: derived
      expression: |
        (metric('{{ base_metric }}') - metric('{{ base_metric }}', offset_window=mom)) 
        / metric('{{ base_metric }}', offset_window=mom)

metrics:
  - name: revenue_growth
    template: growth_rate
    parameters:
      base_metric: total_revenue
```

## Best Practices

### 1. Define Base Metrics First

Create simple metrics for commonly used measures:

```yaml
metrics:
  # Base metrics
  - name: total_revenue
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: revenue

  - name: order_count
    type: simple
    source: fct_orders
    measure:
      type: count
      column: order_id
```

### 2. Build Complex Metrics on Top

Use base metrics in ratios and derived metrics:

```yaml
metrics:
  - name: average_order_value
    type: derived
    expression: metric('total_revenue') / metric('order_count')

  - name: revenue_share
    type: ratio
    numerator:
      # This will reuse total_revenue
      source: fct_orders
      measure:
        type: sum
        column: revenue
    denominator:
      # Reference to company-wide revenue
      source: fct_company_metrics
      measure:
        type: sum
        column: total_revenue
```

### 3. Use Templates for Patterns

Create reusable templates for common metric patterns:

```yaml
metric_templates:
  percentage_of_total:
    parameters:
      - name: part_metric
      - name: total_metric
    template:
      type: derived
      expression: |
        metric('{{ part_metric }}') / metric('{{ total_metric }}') * 100
      format:
        type: percentage
        decimal_places: 1
```

## Advanced Features

### Conditional Deduplication

You can force unique metrics by adding distinctive elements:

```yaml
metrics:
  # These won't be deduplicated due to different filters
  - name: revenue_product_a
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: revenue
    filter: product_type = 'A'

  - name: revenue_product_b
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: revenue
    filter: product_type = 'B'
```

### Debugging Deduplication

Enable debug mode to see deduplication in action:

```bash
better-dbt-metrics compile --debug
```

Output will show:
```
[DEBUG] Metric 'profit_margin_denominator' is a duplicate of 'total_revenue'
[DEBUG] Reusing existing metric 'total_revenue' for denominator of 'profit_margin'
```

## Benefits

1. **Performance**: Fewer metrics mean faster query execution
2. **Consistency**: Shared metrics ensure consistent calculations
3. **Maintenance**: Update once, apply everywhere
4. **Clarity**: Clearer relationships between metrics

## Limitations

- Deduplication only works within a single compilation run
- Metrics must have identical configurations to be deduplicated
- Manual references (using `metric()`) are not deduplicated

## Future Enhancements

- Cross-file metric references
- Metric versioning
- Dependency visualization
- Smart metric suggestions based on existing metrics