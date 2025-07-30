# Fiscal Calendar Guide

## Overview

This guide shows how to implement custom fiscal years in Better-DBT-Metrics when your financial year runs from February 1 to January 31.

## Quick Start

### 1. Import the Fiscal Calendar

```yaml
imports:
  - _base.fiscal_calendar as fiscal
```

### 2. Use Fiscal Dimensions

```yaml
metrics:
  - name: revenue_by_fiscal_quarter
    type: simple
    source: fct_revenue
    measure:
      type: sum
      column: amount
    dimensions:
      - $ref: fiscal.fiscal_time_dimensions.fiscal_year
      - $ref: fiscal.fiscal_time_dimensions.fiscal_quarter
```

## Available Fiscal Dimensions

### fiscal_year
- Returns the fiscal year number
- Example: Date 2024-01-15 → FY 2023 (because Jan belongs to previous FY)
- Example: Date 2024-02-15 → FY 2024

### fiscal_quarter
- Q1: February - April
- Q2: May - July
- Q3: August - October
- Q4: November - January

### fiscal_year_quarter
- Combined format: "FY2024-Q1"
- Useful for charts and reporting

### fiscal_month_num
- Month 1 = February
- Month 12 = January
- Maintains sequential ordering within fiscal year

### fiscal_month_name
- Shows calendar month with fiscal position
- Example: "Feb (M1)", "Jan (M12)"

### fiscal_week_num
- Week 1 starts February 1
- Weeks 1-52/53 within fiscal year

## Pre-built Filters

### Current Fiscal Year
```yaml
filter: $ref(fiscal.filters.current_fiscal_year)
```

### Current Fiscal Quarter
```yaml
filter: $ref(fiscal.filters.current_fiscal_quarter)
```

### Last Fiscal Year
```yaml
filter: $ref(fiscal.filters.last_fiscal_year)
```

### Fiscal Year-to-Date
```yaml
filter: $ref(fiscal.filters.fiscal_year_to_date)
```

## Templates

### fiscal_metric Template
Basic template with fiscal dimensions pre-configured:

```yaml
- name: my_fiscal_metric
  $use: fiscal.fiscal_metric
  parameters:
    source_table: fct_sales
    measure_column: revenue
    measure_type: sum
    date_column: sale_date
```

### fiscal_comparison Template
Year-over-year comparisons on fiscal year basis:

```yaml
- name: revenue_growth_fiscal
  $use: fiscal.fiscal_comparison
  parameters:
    source_table: fct_revenue
    measure_column: amount
    measure_type: sum
```

## SQL Patterns

### Calculating Fiscal Year
```sql
CASE 
  WHEN EXTRACT(MONTH FROM date) >= 2 
  THEN EXTRACT(YEAR FROM date)
  ELSE EXTRACT(YEAR FROM date) - 1
END
```

### Calculating Fiscal Quarter
```sql
CASE
  WHEN EXTRACT(MONTH FROM date) IN (2, 3, 4) THEN 'Q1'
  WHEN EXTRACT(MONTH FROM date) IN (5, 6, 7) THEN 'Q2'
  WHEN EXTRACT(MONTH FROM date) IN (8, 9, 10) THEN 'Q3'
  WHEN EXTRACT(MONTH FROM date) IN (11, 12, 1) THEN 'Q4'
END
```

## Advanced Examples

### Mixed Calendar and Fiscal Reporting
```yaml
- name: revenue_comparison
  type: simple
  source: fct_revenue
  measure:
    type: sum
    column: amount
  dimensions:
    # Both fiscal and calendar views
    - $ref: fiscal.fiscal_time_dimensions.fiscal_quarter
    - $ref: fiscal.fiscal_time_dimensions.calendar_date
```

### Custom Fiscal Filters
```yaml
- name: q4_holiday_sales
  type: simple
  source: fct_sales
  measure:
    type: sum
    column: amount
  # Q4 includes holiday season (Nov-Jan)
  filter: |
    EXTRACT(MONTH FROM date) IN (11, 12, 1)
    AND product_category = 'gifts'
```

### Fiscal Period Comparisons
```yaml
- name: qtd_vs_target
  type: ratio
  numerator:
    source: fct_revenue
    measure:
      type: sum
      column: actual_amount
    filter: $ref(fiscal.filters.current_fiscal_quarter)
  denominator:
    source: dim_targets
    measure:
      type: sum
      column: target_amount
    filter: "target_period = 'current_quarter'"
```

## Best Practices

1. **Consistency**: Use fiscal dimensions consistently across metrics
2. **Documentation**: Always note that FY runs Feb-Jan in descriptions
3. **Naming**: Prefix fiscal metrics clearly (e.g., `fiscal_ytd_revenue`)
4. **Testing**: Verify edge cases around year boundaries (January dates)

## Customization

To adjust for different fiscal year starts, modify the month checks:
- April 1 start: Change `>= 2` to `>= 4`
- July 1 start: Change `>= 2` to `>= 7`
- October 1 start: Change `>= 2` to `>= 10`

## Database-Specific Notes

### PostgreSQL
The examples use PostgreSQL syntax. Adjust for other databases:

### Snowflake
```sql
-- Replace EXTRACT with
YEAR(date_column)
MONTH(date_column)
```

### BigQuery
```sql
-- Replace EXTRACT with
EXTRACT(YEAR FROM date_column)
EXTRACT(MONTH FROM date_column)
```

### SQL Server
```sql
-- Replace EXTRACT with
YEAR(date_column)
MONTH(date_column)
```