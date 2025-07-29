# Smart Metric Suggestions

The Smart Suggestions feature analyzes your database schema and automatically suggests relevant metrics based on column patterns, data types, and relationships. This dramatically accelerates metric creation for new data sources.

## How It Works

The suggestion engine uses multiple strategies to identify potential metrics:

### 1. Column Pattern Recognition
Recognizes common column naming patterns and suggests appropriate metrics:
- **IDs** → count, count_distinct metrics
- **Amounts/Prices** → sum, average, max metrics
- **Dates** → time-based dimensions
- **Statuses** → filtered counts, conversion rates
- **Booleans** → flag-based counts
- **Scores/Ratings** → averages, percentiles

### 2. Foreign Key Analysis
Detects relationships between tables and suggests:
- Unique counts for foreign keys
- Join-based metrics across related tables

### 3. Business Pattern Detection
Identifies common business patterns like:
- **Revenue Analysis**: When amount + date columns exist
- **User Activity**: When user_id + timestamp exist
- **Conversion Funnels**: When status progression detected
- **Churn Analysis**: When customer status fields exist

### 4. Confidence Scoring
Each suggestion includes a confidence level:
- **High**: Strong pattern match, clear use case
- **Medium**: Good pattern match, review recommended
- **Low**: Possible metric, needs validation

## Usage

### Basic Command
```bash
# Analyze with example schema
better-dbt-metrics suggest

# Analyze specific tables from schema file
better-dbt-metrics suggest --schema-file schema.yml --source fct_orders

# Output to file
better-dbt-metrics suggest --schema-file schema.yml --output-file suggested_metrics.yml
```

### Command Options
- `--source, -s`: Specific table(s) to analyze
- `--schema-file, -f`: Schema definition file (YAML or JSON)
- `--output, -o`: Output format (yaml, json, text)
- `--confidence`: Filter by confidence level (all, high, medium, low)
- `--max-suggestions, -m`: Limit suggestions per table
- `--output-file`: Write to file instead of stdout

## Schema File Format

Create a schema file describing your tables:

```yaml
tables:
  fct_orders:
    schema: analytics
    description: Order fact table
    row_count: 1500000  # Optional, helps with optimization hints
    columns:
      - name: order_id
        type: bigint
        primary_key: true
        nullable: false
        description: Unique order identifier
        
      - name: customer_id
        type: bigint
        foreign_key: true
        references: dim_customers
        nullable: false
        
      - name: order_total
        type: decimal(10,2)
        nullable: false
        
      - name: order_status
        type: varchar(50)
        nullable: false
        sample_values: ['pending', 'completed', 'cancelled']
        
      - name: order_date
        type: timestamp
        nullable: false
```

## Example Output

Running the command produces suggestions like:

```yaml
suggested_metrics:
  # High confidence suggestions
  - name: order_count
    label: Order Count
    type: simple
    description: Total number of order
    source: fct_orders
    measure:
      type: count
      column: order_id
    confidence: high
    reason: "id column detected"

  - name: total_revenue
    label: Total Revenue
    type: simple
    description: Total order_total
    source: fct_orders
    measure:
      type: sum
      column: order_total
    confidence: high
    reason: "amount column detected"

  - name: unique_customers
    label: Unique Customers
    type: simple
    description: Number of unique customers
    source: fct_orders
    measure:
      type: count_distinct
      column: customer_id
    confidence: high
    reason: "Foreign key relationship detected"

  # Medium confidence suggestions
  - name: conversion_rate
    label: Conversion Rate
    type: ratio
    description: Conversion rate calculation
    source: fct_orders
    measure:
      numerator:
        source: fct_orders
        measure:
          type: count
          column: order_id
        filter: "order_status = 'completed'"
      denominator:
        source: fct_orders
        measure:
          type: count
          column: order_id
    confidence: medium
    reason: "Business pattern 'conversion_funnel' detected"
```

## Pattern Examples

### Revenue Metrics
When columns like `amount`, `price`, `total`, `revenue` are detected:
- Total revenue (sum)
- Average order value (average)
- Maximum transaction (max)

### User Activity Metrics
When `user_id` + date columns exist:
- Daily/Monthly Active Users
- User retention metrics
- Session metrics

### Status-Based Metrics
When status/state columns with known values exist:
- Conversion rates
- Status distribution
- Filtered counts by status

### Boolean Metrics
When boolean columns are detected:
- Count of true values
- Percentage calculations

## Integration with Workflow

1. **New Data Source**: Run suggestions on new tables
2. **Review & Select**: Choose relevant suggestions
3. **Customize**: Modify suggested metrics as needed
4. **Add to Metrics**: Copy to your metrics YAML files

## Best Practices

### 1. Schema Quality
Provide accurate schema information:
- Mark primary and foreign keys
- Include data types
- Add sample values for enums

### 2. Review Suggestions
- Start with high-confidence suggestions
- Validate business logic
- Customize naming to match conventions

### 3. Iterative Refinement
- Run suggestions early in development
- Use as starting point, not final metrics
- Add business context after generation

## Advanced Usage

### Custom Pattern Rules
Future versions will support custom pattern rules:

```yaml
# custom_patterns.yml
patterns:
  - name: subscription_metrics
    match_columns: ['subscription_.*', 'plan_.*']
    suggestions:
      - type: count_distinct
        suffix: active_subscriptions
        filter: "status = 'active'"
```

### Database Connections
Future support for direct database analysis:

```bash
better-dbt-metrics suggest \
  --connection "postgresql://user:pass@host/db" \
  --source public.fct_orders
```

## Limitations

- Requires well-structured column names
- Cannot detect complex business logic
- Suggestions need human validation
- No data profiling in current version

## Tips

1. **Column Naming**: Use clear, consistent naming conventions
2. **Foreign Keys**: Mark FK relationships in schema
3. **Data Types**: Provide accurate data types
4. **Batch Analysis**: Analyze multiple related tables together
5. **Output Formats**: Use YAML for easy copying to metric files

The Smart Suggestions feature turns hours of manual metric creation into minutes of review and customization.