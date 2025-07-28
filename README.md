# Better-DBT-Metrics

A **GitHub Actions-first** approach to defining dbt semantic models with powerful DRY features through imports, dimension groups, and templates.

## 🎯 Why This Approach?

After analyzing multiple implementations and approaches, we've learned:

### Key Learnings:
1. **dbt's limitations** - Python models can't do file I/O during parse, forcing manual compilation
2. **DRY is critical** - Teams copy-paste dimensions across hundreds of metrics
3. **GitHub Actions avoid limitations** - Compile during CI/CD, not during dbt parse
4. **Progressive complexity** - Simple for basic use, powerful when needed

### Our Solution:
- ✅ **GitHub Actions primary** - Automated compilation in CI/CD pipeline
- ✅ **Import & reuse** - Define dimensions, templates, and rules once
- ✅ **dbt compatible** - Outputs standard dbt semantic models

## 🚀 Quick Start with GitHub Actions

Add to `.github/workflows/metrics.yml`:

```yaml
name: Compile Metrics
on:
  push:
    paths: ['metrics/**', 'templates/**']
  pull_request:
    paths: ['metrics/**', 'templates/**']

jobs:
  compile:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: rdwburns/better-dbt-metrics@v2
        with:
          metrics-dir: 'metrics/'
          output-dir: 'models/semantic/'
```

## 🚀 Quick Start

### 1. Install

```bash
pip install better-dbt-metrics
```

### 2. Define Your Metrics

```yaml
# metrics/revenue.yml
version: 2

# Import reusable components
imports:
  - ../templates/dimensions/temporal.yml as time
  - ../templates/dimensions/customer.yml as customer

metrics:
  - name: total_revenue
    description: "Total revenue from completed orders"
    source: fct_orders
    measure:
      type: sum
      column: amount
      filters:
        - "status = 'completed'"
    dimensions:
      - $ref: time.daily
      - $ref: customer.segment
```

### 3. Compile to dbt Format

```bash
better-dbt-metrics compile --input-dir metrics/ --output-dir models/semantic/
```

## 📚 Core Features (Built)

### 1. Import System

Import and reuse components across files:

```yaml
# Import with alias
imports:
  - ../shared/dimensions.yml as dim
  - ../shared/metrics.yml as base

# Use imported content
metrics:
  - name: revenue
    dimensions:
      - $ref: dim.temporal_daily
      - $ref: dim.customer_segment
```

### 2. Metric References in Filters

Use other metrics as dynamic thresholds in your filters:

```yaml
metrics:
  - name: average_order_value
    type: simple
    source: fct_orders
    measure:
      type: average
      column: order_total
      
  - name: high_value_orders
    type: simple
    source: fct_orders
    measure:
      type: count
      column: order_id
    filter: "order_total > metric('average_order_value')"
```

Supports complex expressions:
```yaml
filter: |
  order_total > (metric('avg_value') + 2 * metric('stddev_value'))
  OR order_total < (metric('avg_value') - 2 * metric('stddev_value'))
```

### 3. Fill Nulls for Time Series

Handle gaps in time series data with intelligent null filling strategies:

```yaml
metrics:
  - name: daily_revenue
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: revenue
    dimensions:
      - name: date_day
        type: time
        grain: day
    fill_nulls_with: 0  # Options: 0, previous, interpolate, or custom value
```

Advanced fill strategies:
```yaml
fill_nulls_with: 0  # Default strategy
config:
  fill_nulls_rules:
    - dimension: region
      value: "APAC"
      fill_with: previous  # Region-specific strategy
```

### 4. Entity Relationships

Define primary/foreign key relationships for semantic models:

```yaml
# Define entities with their relationships
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

# Use entities in metrics
metrics:
  - name: customer_lifetime_value
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: order_total
    entity: customer  # Primary entity for this metric
```

Entity sets for complex relationships:
```yaml
entity_sets:
  - name: customer_orders
    primary_entity: customer
    includes:
      - entity: order
        join_type: left
      - entity: order_item
        through: order  # Join path

# Use in semantic models
semantic_models:
  - name: customer_analysis
    source: fct_orders
    entity_set: customer_orders
```

### 5. Time Spine Configuration

Ensure complete time series data with time spine configurations:

```yaml
# Define time spines for different granularities
time_spine:
  default:
    model: ref('dim_date')
    columns:
      date_day: date_day
      date_week: date_week
      date_month: date_month
      date_year: date_year
    
  hourly:
    model: ref('dim_datetime')
    columns:
      datetime_hour: datetime_hour
      date_day: date_day

# Use in metrics
metrics:
  - name: daily_revenue
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: revenue
    dimensions:
      - name: order_date
        type: time
        grain: day
    time_spine: default  # Ensures no gaps in time series
```

Fiscal calendar support:
```yaml
time_spine:
  fiscal:
    model: ref('dim_fiscal_calendar')
    columns:
      fiscal_date: fiscal_date
      fiscal_quarter: fiscal_quarter
      fiscal_year: fiscal_year
    meta:
      fiscal_year_start_month: 4
```

### 6. Metric Time Dimension

Standardize time dimensions across metrics with `metric_time`:

```yaml
metrics:
  # Each metric can map metric_time to its own date column
  - name: daily_orders
    type: simple
    source: fct_orders
    measure:
      type: count
      column: order_id
    dimensions:
      - name: metric_time  # Special dimension
        type: time
        grain: day
        expr: order_date  # Maps to this column
        
  - name: customer_signups
    type: simple
    source: fct_customers
    measure:
      type: count
      column: customer_id
    dimensions:
      - name: metric_time
        type: time
        grain: day
        expr: signup_date  # Different column, same dimension
```

Use in semantic models:
```yaml
semantic_models:
  - name: unified_metrics
    model: ref('fct_unified')
    primary_time_dimension: metric_time  # Designate as primary
    dimensions:
      - name: metric_time
        type: time
        expr: COALESCE(order_date, signup_date, event_date)
```

### 7. Dimension Groups (DRY)

Define dimensions once, use everywhere:

```yaml
# templates/dimensions/temporal.yml
dimension_groups:
  daily:
    dimensions:
      - name: date_day
        type: time
        grain: day
      - name: date_week
        type: time
        grain: week
      - name: date_month
        type: time
        grain: month

# Use in any metric
metrics:
  - name: orders
    dimension_groups: [daily]
```

### 8. Metric Templates

Standardize metric patterns:

```yaml
# templates/metrics/revenue.yml
metric_templates:
  revenue_base:
    parameters:
      - name: SOURCE_TABLE
        required: true
      - name: AMOUNT_COLUMN
        default: "amount"
    template:
      source: "{{ SOURCE_TABLE }}"
      measure:
        type: sum
        column: "{{ AMOUNT_COLUMN }}"

# Use the template
metrics:
  - name: product_revenue
    template: revenue_base
    parameters:
      SOURCE_TABLE: fct_product_sales
      AMOUNT_COLUMN: product_amount
```

### 9. References ($ref and $use)

Reference imported content:

```yaml
# $ref - brings in specific item
dimensions:
  - $ref: time.daily

# $use - brings in a collection
dimension_groups:
  - $use: standard_dimensions
```

### 10. Join Path Configuration

Define explicit join paths for complex data models:

```yaml
# Define join paths
join_paths:
  - from: fct_orders
    to: dim_customers
    join_type: inner
    join_keys:
      - from_column: customer_id
        to_column: customer_id
        
  # Multi-hop join through intermediate table
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

# Use in metrics
metrics:
  - name: revenue_by_customer_segment
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: order_total
    dimensions:
      - name: customer_segment
        source: dim_customers  # Automatically uses defined join path
        column: segment
```

Join path aliases for reuse:
```yaml
join_path_aliases:
  customer_full:
    paths:
      - from: fct_orders
        to: dim_customers
        join_type: left
        join_keys:
          - from_column: customer_id
            to_column: customer_id

metrics:
  - name: customer_analysis
    source: fct_orders
    join_paths: [customer_full]  # Use predefined join paths
```

### 11. Window Functions in Measures

Use SQL window functions for advanced analytics:

```yaml
metrics:
  # Moving average
  - name: revenue_7d_moving_avg
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
        
  # Rank within partition
  - name: customer_revenue_rank
    type: simple
    source: fct_customers
    measure:
      type: window
      column: total_revenue
      window_function: "RANK() OVER (PARTITION BY customer_segment ORDER BY {{ column }} DESC)"
      
  # Lead/Lag comparison
  - name: revenue_vs_previous
    type: simple
    source: fct_daily_summary
    measure:
      type: window
      column: daily_revenue
      window_function: "{{ column }} - LAG({{ column }}, 1, 0) OVER (ORDER BY date_day)"
```

### 12. Offset Windows for Cumulative Metrics

Compare cumulative metrics across different time periods:

```yaml
metrics:
  # Month-to-date with comparisons
  - name: revenue_mtd_comparison
    type: cumulative
    measure:
      source: fct_orders
      type: sum
      column: order_total
    grain_to_date: day
    window: month
    offsets:
      - period: month
        offset: -1
        alias: last_month_mtd
      - period: year
        offset: -1
        alias: same_month_last_year
        
  # With growth calculations
  - name: weekly_active_users
    type: cumulative
    measure:
      source: fct_user_activity
      type: count_distinct
      column: user_id
    grain_to_date: day
    window: week
    offsets:
      - period: week
        offset: -1
        alias: last_week
        calculations:
          - type: difference
            alias: wow_change
          - type: percent_change
            alias: wow_growth_rate
            
  # Trailing window with offset
  - name: trailing_30d_revenue
    type: cumulative
    measure:
      source: fct_orders
      type: sum
      column: revenue
    grain_to_date: day
    window: 30
    window_type: trailing
    offsets:
      - period: day
        offset: -30
        alias: previous_30d_period
```

You can also define reusable offset patterns:

```yaml
# Define patterns
offset_window_config:
  offset_patterns:
    standard_comparisons:
      - period: week
        offset: -1
        alias: last_week
      - period: month
        offset: -1
        alias: last_month
      - period: year
        offset: -1
        alias: last_year

# Use in metrics
metrics:
  - name: revenue_with_comparisons
    type: cumulative
    # ... measure config ...
    offset_pattern: standard_comparisons
```

### 13. Validation Framework

Catch errors before compilation with comprehensive validation:

```bash
# Run validation
better-dbt-metrics validate --input-dir metrics/

# Validation checks include:
# ✓ Required fields (name, type, source, measures)
# ✓ Valid metric types (simple, ratio, derived, cumulative, conversion)
# ✓ Valid dimension and measure types
# ✓ Circular dependency detection
# ✓ Reference resolution
# ✓ Entity relationship validation
# ✓ Template parameter validation
# ✓ Duplicate name detection
# ✓ Window function validation
# ✓ Offset window validation
# ✓ YAML syntax validation
```

Example validation output:
```
❌ Found 3 error(s):
  metrics/revenue.yml:15 - error: Invalid metric type 'complex'
    Suggestion: Valid types are: simple, ratio, derived, cumulative, conversion
  metrics/revenue.yml:25 - error: Circular dependency detected involving metric 'metric_a'
    Suggestion: Review metric dependencies and remove circular references
  metrics/product.yml:10 - error: Metric 'revenue' defined in multiple files
    Suggestion: Use unique metric names across all files

⚠️  Found 2 warning(s):
  metrics/orders.yml:30 - warning: Time dimension 'order_date' should specify a grain
    Suggestion: Add grain: day, week, month, quarter, or year
```

## 🤖 GitHub Actions Integration

Better-DBT-Metrics is designed to work seamlessly with GitHub Actions:

### Basic Usage

```yaml
- uses: rdwburns/better-dbt-metrics@v2
  with:
    metrics-dir: 'metrics/'
    output-dir: 'models/semantic/'
```

### Advanced Options

```yaml
- uses: rdwburns/better-dbt-metrics@v2
  with:
    metrics-dir: 'metrics/'
    output-dir: 'models/semantic/'
    template-dirs: 'templates/,shared/templates/'  # Multiple dirs
    validate-only: false                           # Set true for PR validation
    python-version: '3.11'
    fail-on-warning: true                          # Strict validation
    upload-artifacts: true                         # Upload compiled models
    comment-on-pr: true                           # Add PR comments
```

### Example Workflows

**Validation on PRs:**
```yaml
name: Validate Metrics
on:
  pull_request:
    paths: ['metrics/**']

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: rdwburns/better-dbt-metrics@v2
        with:
          validate-only: true
          comment-on-pr: true
```

**Full Pipeline with dbt:**
```yaml
name: Metrics Pipeline
on:
  push:
    branches: [main]
    paths: ['metrics/**']

jobs:
  compile:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - uses: rdwburns/better-dbt-metrics@v2
        with:
          metrics-dir: 'metrics/'
          output-dir: 'models/semantic/'
          
      - name: Run dbt
        run: |
          pip install dbt-core dbt-snowflake
          dbt run --models +semantic
```

See [examples/github-workflows/](examples/github-workflows/) for more workflow examples.

## 🔧 CLI Commands

```bash
# Compile metrics to dbt format
better-dbt-metrics compile --input-dir metrics/ --output-dir models/

# Compile with custom template directory
better-dbt-metrics compile -i metrics/ -o models/ -t templates/

# Validate metrics (catch errors before compilation)
better-dbt-metrics validate --input-dir metrics/

# Validate with detailed output
better-dbt-metrics validate -v --fail-on-warning

# List available templates
better-dbt-metrics list-templates

# Initialize a new project
better-dbt-metrics init
```

## 📁 Project Structure

```
your-analytics-repo/
├── metrics/                    # Your metric definitions
│   ├── finance/
│   │   ├── revenue.yml
│   │   └── costs.yml
│   └── product/
│       └── engagement.yml
├── templates/                  # Reusable components
│   ├── dimensions/
│   │   ├── temporal.yml       # Time dimensions
│   │   └── customer.yml       # Customer attributes
│   └── metrics/
│       └── financial.yml      # Metric templates
└── models/                     # dbt models (git-ignored)
    └── semantic/              # Compiled output goes here
```

## 🎯 Complete Example

```yaml
# metrics/revenue_analytics.yml
version: 2

imports:
  - ../templates/dimensions/temporal.yml as time
  - ../templates/dimensions/geographic.yml as geo
  - ../templates/metrics/financial.yml as fin

# Define dimension group
dimension_groups:
  revenue_analysis:
    dimensions:
      - $ref: time.daily
      - $ref: geo.country
      - name: product_category
        type: categorical

# Use template with dimension group
metrics:
  - name: total_revenue
    template: fin.revenue_base
    parameters:
      SOURCE_TABLE: fct_orders
      AMOUNT_COLUMN: order_total
    dimension_groups: [revenue_analysis]
    
  - name: product_revenue
    template: fin.revenue_base
    parameters:
      SOURCE_TABLE: fct_orders
      AMOUNT_COLUMN: product_amount
    dimension_groups: [revenue_analysis]
```

## 🚧 Roadmap

See [FEATURE_STATUS.md](FEATURE_STATUS.md) for detailed feature tracking.

### Recently Completed ✅:
- Join path configuration for complex data models
- Window functions in measures  
- Offset windows for cumulative metrics
- Validation and testing framework
- GitHub Action package

### Coming Soon:
- Auto-generated metric variants (WoW, MoM, YoY)
- Performance profiling and optimization
- Direct BI tool integration (Tableau, Looker, PowerBI)
- Metric catalog with auto-generated documentation
- Change detection for incremental compilation

## 🤝 Contributing

We welcome contributions! The codebase is modular and well-documented.

### Development Setup:

```bash
# Clone the repo
git clone https://github.com/rdwburns/better-dbt-metrics.git
cd better-dbt-metrics

# Install in development mode
pip install -e .

# Run tests
pytest tests/
```

## 📄 License

Apache 2.0 - See [LICENSE](LICENSE) for details.

---

**Built with learnings from multiple iterations to create a practical metrics layer solution.**