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

### ✨ Recent Additions
- **🤖 Smart Suggestions** - AI-powered metric suggestions from database schema
- **📊 Metric Catalog** - Auto-generated searchable documentation with lineage
- **🔍 Enhanced Error Handling** - Clear, actionable error messages with suggestions

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

### 13. Enhanced Error Handling & Validation

Comprehensive pre-compilation validation with clear, actionable error messages:

```bash
# Standard compilation with enhanced errors
better-dbt-metrics compile

# Verbose mode for detailed progress and debugging
better-dbt-metrics compile --verbose

# Different output formats for CI/CD
better-dbt-metrics compile --report-format json
better-dbt-metrics compile --report-format junit > results.xml

# Pre-validation checks include:
# ✓ YAML syntax with line numbers
# ✓ Required fields (name, type, source, measures)
# ✓ Valid metric types (simple, ratio, derived, cumulative, conversion)
# ✓ Reference resolution (imports, dimensions, templates)
# ✓ Circular dependency detection
# ✓ Best practice recommendations
# ✓ Naming convention validation
```

Example enhanced error output:
```
============================================================
📊 Better-DBT-Metrics Compilation Report
============================================================

📋 Issues: ❌ 2 error(s) | ⚠️ 1 warning(s)

❌ Errors (must fix):
----------------------------------------

1. ❌ ERROR: Cannot resolve reference: $ref: time.weekly
  📍 Location: metrics/sales.yml
  📊 Metric: weekly_revenue
  💡 Suggestion: Ensure the dimension is imported and the reference path is correct.
     Use '$ref:' for dimension references and '$use:' for template references.

2. ❌ ERROR: Invalid metric type: 'percentage'
  📍 Location: metrics/kpis.yml:15
  📊 Metric: growth_rate
  💡 Suggestion: Valid metric types are: simple, ratio, derived, cumulative, conversion

⚠️ Warnings (should review):
----------------------------------------

1. ⚠️ WARNING: Missing description
  📍 Location: metrics/finance.yml
  📊 Metric: total_revenue
  💡 Suggestion: Add a 'description' field to document the metric's purpose

============================================================
❌ Compilation failed with errors
Please fix the errors above and try again
============================================================
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

### Compilation
```bash
# Basic compilation
better-dbt-metrics compile --input-dir metrics/ --output-dir models/

# With detailed progress and error information
better-dbt-metrics compile --verbose

# Skip pre-validation for faster compilation
better-dbt-metrics compile --no-pre-validate

# Output structured errors for CI/CD
better-dbt-metrics compile --report-format json
better-dbt-metrics compile --report-format junit > test-results.xml
```

### Smart Suggestions (New!)
```bash
# Analyze database schema and suggest metrics
better-dbt-metrics suggest --schema-file schema.yml

# Try with example e-commerce schema
better-dbt-metrics suggest

# Get only high-confidence suggestions
better-dbt-metrics suggest --confidence high --output-file metrics.yml
```

### Metric Catalog (New!)
```bash
# Generate searchable metric documentation
better-dbt-metrics catalog --input-dir metrics/ --output-dir docs/catalog/

# Compact format for quick reference
better-dbt-metrics catalog --format compact

# Include search functionality (default)
better-dbt-metrics catalog --include-search
```

### Validation & Utilities
```bash
# Validate metrics (catch errors before compilation)
better-dbt-metrics validate --input-dir metrics/

# List available templates and dimension groups
better-dbt-metrics list-templates
better-dbt-metrics list-dimensions
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

## ⚙️ Configuration

Better-DBT-Metrics can be configured using a `bdm_config.yml` file to customize compilation behavior and set organization-specific defaults.

### Quick Configuration Example

```yaml
# bdm_config.yml
version: 1

# Custom paths
paths:
  metrics_dir: analytics/metrics/
  output_dir: models/marts/metrics/
  template_dir: shared/templates/

# Import shortcuts
imports:
  mappings:
    "_base.templates": "_base/templates.yml"
    "_base.dimensions": "_base/dimension_groups.yml"

# Auto-variant settings
auto_variants:
  time_comparisons:
    enabled: true
    default_periods: [wow, mom, yoy]
  
# Domain-specific settings
domains:
  marketing:
    auto_variants:
      channel_splits: [organic, paid, social]
  finance:
    auto_variants:
      currency_splits: [USD, EUR, GBP]

# Validation rules
validation:
  require_descriptions: true
  require_labels: true
```

### Configuration Features

- **🗂️ Path Configuration** - Custom input/output directories
- **🔗 Import Mappings** - Shortcuts for commonly imported files  
- **🔄 Auto-Variants** - Automatic metric variant generation
- **🏢 Domain Settings** - Different configs per domain (marketing, finance, etc.)
- **✅ Validation Rules** - Enforce organizational standards
- **📊 Output Control** - Customize generated file format

**📚 Full Documentation**: See [docs/configuration.md](docs/configuration.md) for complete configuration reference.

## 🚧 Roadmap

See [FEATURE_STATUS.md](FEATURE_STATUS.md) for detailed feature tracking.

### Recently Completed ✅:
- **🤖 Smart Suggestions** - Schema analysis and metric generation
- **📊 Metric Catalog** - Interactive documentation with search and lineage
- **🔍 Enhanced Error Handling** - Pre-validation and detailed error reporting
- Join path configuration for complex data models
- Window functions in measures  
- Offset windows for cumulative metrics
- Validation and testing framework
- GitHub Action package

### Coming Soon:
- **📚 Built-in Metric Library** - Pre-built templates for common business metrics
- **⚡ Performance Optimization** - Query hints and materialization recommendations
- **🧪 Testing Framework** - Automated metric validation and regression testing
- Auto-generated metric variants (WoW, MoM, YoY)
- Direct BI tool integration (Tableau, Looker, PowerBI)
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