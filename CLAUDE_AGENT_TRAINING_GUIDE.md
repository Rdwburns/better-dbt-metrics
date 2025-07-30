# Better-DBT-Metrics Training Guide for Claude Agents

This guide provides comprehensive examples and patterns for writing Better-DBT-Metrics YAML correctly.

## Core Syntax Rules

### 1. Always Use `version: 1` or `version: 2`
```yaml
version: 1  # Always start with this
```

### 2. Dimension Groups Structure
**❌ WRONG - Direct list:**
```yaml
dimension_groups:
  temporal_dimensions:
    - name: date
    - name: week
```

**✅ CORRECT - Dictionary with dimensions key:**
```yaml
dimension_groups:
  temporal_dimensions:
    dimensions:  # This key is REQUIRED
      - name: date
        type: time
        grain: day
      - name: week
        type: time
        grain: week
```

### 3. Metric Structure
**Basic metric structure:**
```yaml
metrics:
  - name: metric_name       # Required
    description: "..."      # Recommended  
    type: simple           # simple, ratio, derived, cumulative, conversion
    source: table_name     # Required for most types
    measure:               # Required for simple/cumulative
      type: sum           # sum, count, average, min, max
      column: amount_column
    dimensions:            # Optional
      - name: date
        type: time
        grain: day
```

## Complete Working Examples

### 1. Dimension Groups File
```yaml
# File: metrics/_base/dimension_groups.yml
version: 1

dimension_groups:
  # Temporal dimensions
  time_dimensions:
    description: "Standard time-based analysis dimensions"
    dimensions:
      - name: date_day
        type: time
        grain: day
        sql: "{{ dimension_table }}.date"
      - name: date_week
        type: time
        grain: week
        sql: "DATE_TRUNC({{ dimension_table }}.date, WEEK)"
      - name: date_month
        type: time
        grain: month
        sql: "DATE_TRUNC({{ dimension_table }}.date, MONTH)"
      - name: date_quarter
        type: time
        grain: quarter
        sql: "DATE_TRUNC({{ dimension_table }}.date, QUARTER)"
      - name: date_year
        type: time
        grain: year
        sql: "DATE_TRUNC({{ dimension_table }}.date, YEAR)"

  # Geographic dimensions
  geography:
    description: "Geographic analysis dimensions"
    dimensions:
      - name: country
        type: categorical
        sql: "{{ dimension_table }}.country"
      - name: region
        type: categorical
        sql: "{{ dimension_table }}.region"
      - name: city
        type: categorical
        sql: "{{ dimension_table }}.city"

  # Customer dimensions
  customer_analysis:
    description: "Customer segmentation dimensions"
    dimensions:
      - name: customer_tier
        type: categorical
        sql: "{{ dimension_table }}.customer_tier"
      - name: acquisition_channel
        type: categorical
        sql: "{{ dimension_table }}.acquisition_channel"
      - name: customer_lifetime_value_bucket
        type: categorical
        sql: |
          CASE 
            WHEN {{ dimension_table }}.lifetime_value < 100 THEN 'Low Value'
            WHEN {{ dimension_table }}.lifetime_value < 500 THEN 'Medium Value'
            ELSE 'High Value'
          END

  # Product dimensions
  product_analysis:
    description: "Product categorization dimensions"
    dimensions:
      - name: product_category
        type: categorical
        sql: "{{ dimension_table }}.category"
      - name: product_brand
        type: categorical
        sql: "{{ dimension_table }}.brand"
      - name: price_tier
        type: categorical
        sql: |
          CASE 
            WHEN {{ dimension_table }}.unit_price < 20 THEN 'Budget'
            WHEN {{ dimension_table }}.unit_price < 50 THEN 'Standard'
            ELSE 'Premium'
          END
```

### 2. Metric Templates File
```yaml
# File: metrics/_base/templates.yml
version: 1

metric_templates:
  # Revenue metrics template
  revenue_base:
    description: "Standard revenue calculation template"
    parameters:
      - name: SOURCE_TABLE
        type: string
        required: true
        description: "The fact table containing revenue data"
      - name: AMOUNT_COLUMN
        type: string
        default: "amount"
        description: "Column containing revenue amounts"
      - name: STATUS_FILTER
        type: string
        default: "status = 'completed'"
        description: "Filter for completed transactions"
    template:
      type: simple
      source: "{{ SOURCE_TABLE }}"
      measure:
        type: sum
        column: "{{ AMOUNT_COLUMN }}"
        filters:
          - "{{ STATUS_FILTER }}"

  # Count metrics template
  count_base:
    description: "Standard count calculation template"
    parameters:
      - name: SOURCE_TABLE
        type: string
        required: true
      - name: COUNT_COLUMN
        type: string
        default: "id"
      - name: FILTER_CONDITION
        type: string
        default: "1=1"
    template:
      type: simple
      source: "{{ SOURCE_TABLE }}"
      measure:
        type: count
        column: "{{ COUNT_COLUMN }}"
        filters:
          - "{{ FILTER_CONDITION }}"

  # Ratio metrics template
  conversion_rate_base:
    description: "Standard conversion rate calculation"
    parameters:
      - name: SOURCE_TABLE
        type: string
        required: true
      - name: CONVERSION_COLUMN
        type: string
        required: true
      - name: TOTAL_COLUMN
        type: string
        required: true
    template:
      type: ratio
      numerator:
        source: "{{ SOURCE_TABLE }}"
        measure:
          type: count
          column: "{{ CONVERSION_COLUMN }}"
          filters:
            - "{{ CONVERSION_COLUMN }} IS NOT NULL"
      denominator:
        source: "{{ SOURCE_TABLE }}"
        measure:
          type: count
          column: "{{ TOTAL_COLUMN }}"
```

### 3. Complete Metrics File with Imports
```yaml
# File: metrics/revenue_metrics.yml
version: 1

# Import reusable components
imports:
  - ../_base/dimension_groups.yml as dims
  - ../_base/templates.yml as templates

# Define metrics using imports
metrics:
  # Simple revenue metric using template
  - name: total_revenue
    description: "Total revenue from completed orders"
    label: "Total Revenue"
    template: templates.revenue_base
    parameters:
      SOURCE_TABLE: fct_orders
      AMOUNT_COLUMN: order_total
      STATUS_FILTER: "order_status = 'completed'"
    dimension_groups:
      - $use: dims.time_dimensions
      - $use: dims.geography
    meta:
      domain: finance
      owner: finance_team

  # Revenue by product category
  - name: revenue_by_category
    description: "Revenue broken down by product category" 
    label: "Revenue by Category"
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: order_total
      filters:
        - "order_status = 'completed'"
    dimensions:
      - $ref: dims.time_dimensions.date_month
      - $ref: dims.product_analysis.product_category
      - name: channel
        type: categorical
        sql: "channel"

  # Average order value
  - name: average_order_value
    description: "Average value per completed order"
    label: "Average Order Value"
    type: simple
    source: fct_orders
    measure:
      type: average
      column: order_total
      filters:
        - "order_status = 'completed'"
    dimensions:
      - $ref: dims.time_dimensions.date_day
      - $ref: dims.customer_analysis.customer_tier

  # Conversion rate metric
  - name: order_conversion_rate
    description: "Percentage of sessions that result in orders"
    label: "Order Conversion Rate"
    type: ratio
    numerator:
      source: fct_sessions
      measure:
        type: count_distinct
        column: session_id
        filters:
          - "converted = true"
    denominator:
      source: fct_sessions
      measure:
        type: count_distinct
        column: session_id
    dimensions:
      - $ref: dims.time_dimensions.date_week
      - $ref: dims.geography.country

  # Derived metric using other metrics
  - name: revenue_per_session
    description: "Average revenue generated per website session"
    label: "Revenue per Session"
    type: derived
    expression: "metric('total_revenue') / metric('total_sessions')"
    dimensions:
      - $ref: dims.time_dimensions.date_month

  # Cumulative revenue
  - name: cumulative_monthly_revenue
    description: "Running total of revenue month-to-date"
    label: "Cumulative Monthly Revenue"
    type: cumulative
    source: fct_orders
    measure:
      type: sum
      column: order_total
      filters:
        - "order_status = 'completed'"
    grain_to_date: day
    window: month
    dimensions:
      - name: metric_time
        type: time
        grain: day
        expr: order_date
```

## Key Patterns to Remember

### 1. Import Syntax
```yaml
imports:
  - path/to/file.yml as alias_name
  
# Then use:
dimensions:
  - $ref: alias_name.dimension_group.dimension_name
  - $use: alias_name.dimension_group  # Gets all dimensions in group
```

### 2. Dimension Reference Types
```yaml
dimensions:
  # Direct dimension definition
  - name: order_date
    type: time
    grain: day
    
  # Reference to imported dimension
  - $ref: dims.time_dimensions.date_day
  
  # String shorthand (for simple cases)
  - customer_id
```

### 3. Template Usage
```yaml
# Using a template
- name: my_metric
  template: templates.revenue_base
  parameters:
    SOURCE_TABLE: my_table
    AMOUNT_COLUMN: my_amount_col
```

### 4. Measure Types
```yaml
measure:
  type: sum          # sum, count, average, min, max, count_distinct
  column: amount_col
  filters:           # Optional
    - "status = 'active'"
    - "amount > 0"
```

### 5. Metric Types
- **simple**: Basic aggregation of a column
- **ratio**: Numerator divided by denominator  
- **derived**: Mathematical expression using other metrics
- **cumulative**: Running totals with time windows
- **conversion**: Specialized for conversion rate calculations

## Common Mistakes to Avoid

### ❌ Wrong dimension group structure:
```yaml
dimension_groups:
  time_dims:
    - name: date
```

### ✅ Correct:
```yaml
dimension_groups:
  time_dims:
    dimensions:
      - name: date
```

### ❌ Wrong import reference:
```yaml
- $ref: dims.date_day  # Missing intermediate group name
```

### ✅ Correct:
```yaml
- $ref: dims.time_dimensions.date_day
```

### ❌ Missing required fields:
```yaml
metrics:
  - name: revenue  # Missing type, source, measure
```

### ✅ Correct:
```yaml
metrics:
  - name: revenue
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: amount
```

## Validation Checklist

Before submitting Better-DBT-Metrics YAML:

1. ✅ Starts with `version: 1` or `version: 2`
2. ✅ Dimension groups use `dimensions:` key with list underneath
3. ✅ All metrics have `name`, `type`, `source` (if applicable), and `measure`
4. ✅ Import paths are correct and use `as alias_name`
5. ✅ References use correct syntax: `$ref: alias.group.dimension`
6. ✅ Template parameters match template definition
7. ✅ Measure types are valid: sum, count, average, min, max, count_distinct
8. ✅ Metric types are valid: simple, ratio, derived, cumulative, conversion
9. ✅ All required template parameters are provided
10. ✅ SQL expressions are properly quoted and formatted

This guide should help your Claude agent write syntactically correct Better-DBT-Metrics YAML consistently!