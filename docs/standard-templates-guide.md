# Standard Semantic Model Templates Guide

Better-DBT-Metrics includes a comprehensive library of standard semantic model templates that accelerate development for common data patterns. These templates provide pre-built semantic models for typical business scenarios.

## Available Templates

### Fact Table Templates

#### `standard_fact_table`
General-purpose template for any fact table with time dimensions.

**Parameters:**
- `table_name` (required): Source table name
- `date_column` (required): Primary date column
- `primary_key` (default: "id"): Primary key column
- `grain` (default: "row"): Table grain description

**Features:**
- Creates time dimensions at day, week, month, quarter, and year levels
- Auto-infers additional dimensions and measures
- Excludes common ETL metadata columns

**Example:**
```yaml
semantic_models:
  - name: sales_facts
    template: standard_fact_table
    parameters:
      table_name: fct_sales
      date_column: sale_date
      primary_key: sale_id
```

#### `ecommerce_orders`
Specialized template for e-commerce order fact tables.

**Parameters:**
- `table_name` (required): Source table name
- `order_date_column` (default: "order_date"): Order date column
- `amount_column` (default: "order_amount"): Order amount column
- `quantity_column` (default: "quantity"): Quantity column

**Features:**
- Standard e-commerce entities (order_id, customer_id)
- Common measures (order_count, total_amount, avg_order_value)
- Time dimensions and order status

**Example:**
```yaml
semantic_models:
  - name: orders
    template: ecommerce_orders
    parameters:
      table_name: fct_orders
      amount_column: total_amount
```

#### `event_tracking`
Template for product analytics event tables.

**Parameters:**
- `table_name` (required): Source table name
- `event_timestamp_column` (default: "event_timestamp"): Event timestamp
- `user_key` (default: "user_id"): User identifier

**Features:**
- Time dimensions at second, hour, and day granularities
- Event categorization dimensions
- User and session tracking measures

#### `financial_transactions`
Template for financial transaction tables.

**Parameters:**
- `table_name` (required): Source table name
- `transaction_date_column` (default: "transaction_date"): Transaction date
- `amount_column` (default: "amount"): Transaction amount

**Features:**
- Financial transaction entities and dimensions
- Amount aggregations and averages
- Transaction type and status tracking

#### `marketing_attribution`
Template for marketing attribution fact tables.

**Parameters:**
- `table_name` (required): Source table name
- `attribution_date_column` (default: "attribution_date"): Attribution date
- `spend_column` (default: "spend"): Marketing spend
- `impressions_column` (default: "impressions"): Impressions count
- `clicks_column` (default: "clicks"): Clicks count

**Features:**
- Marketing channel and campaign dimensions
- Performance measures (CPM, CPC, CTR)
- Attribution model tracking

### Dimension Table Templates

#### `customer_dimension`
Template for customer dimension tables.

**Parameters:**
- `table_name` (required): Source table name
- `customer_key` (default: "customer_id"): Customer identifier
- `signup_date_column` (default: "created_date"): Customer signup date

**Features:**
- Customer segmentation and status dimensions
- Acquisition channel tracking
- Auto-inference for additional categorical dimensions

#### `product_dimension` 
Template for product dimension tables.

**Parameters:**
- `table_name` (required): Source table name
- `product_key` (default: "product_id"): Product identifier

**Features:**
- Product hierarchy dimensions (category, subcategory, brand)
- Product status tracking
- Pricing measures (list price, cost averages)

## Entity Sets

Entity sets provide reusable collections of entities for common patterns:

### `ecommerce_fact`
Standard entities for e-commerce fact tables:
- Primary: `id`
- Foreign: `customer_id`, `product_id`, `order_id`

### `marketing_fact`
Standard entities for marketing fact tables:
- Primary: `id` 
- Foreign: `campaign_id`, `customer_id`, `channel_id`

### `financial_fact`
Standard entities for financial fact tables:
- Primary: `transaction_id`
- Foreign: `account_id`, `customer_id`

### `user_behavior_fact`
Standard entities for user behavior fact tables:
- Primary: `event_id`
- Foreign: `user_id`, `session_id`

## Usage Patterns

### 1. Basic Template Usage

```yaml
imports:
  - ../../templates/semantic_models/standard_templates.yml as std

semantic_models:
  - name: orders
    template: std.ecommerce_orders
    parameters:
      table_name: fct_orders
      order_date_column: order_date
```

### 2. Template with Custom Additions

```yaml
semantic_models:
  - name: sales
    template: std.standard_fact_table
    parameters:
      table_name: fct_sales
      date_column: sale_date
    
    # Add custom entities beyond template
    entities:
      - name: salesperson_id
        type: foreign
        expr: salesperson_id
    
    # Add custom measures
    measures:
      - name: commission_total
        agg: sum
        expr: commission
        agg_time_dimension: sale_date
```

### 3. Using Entity Sets

```yaml
semantic_models:
  - name: order_items
    source: fct_order_items
    entity_set: std.ecommerce_fact  # Apply standard entities
    
    dimensions:
      - name: item_date
        type: time
        type_params:
          time_granularity: day
        expr: created_date
```

### 4. Template Inheritance

Create custom templates that extend standard ones:

```yaml
semantic_model_templates:
  enhanced_orders:
    extends: std.ecommerce_orders
    description: "Enhanced orders with additional metrics"
    additional_parameters:
      - name: discount_column
        type: string
        default: "discount_amount"
    
    additional_measures:
      - name: total_discount
        agg: sum
        expr: "{{ discount_column }}"
        agg_time_dimension: "{{ order_date_column }}"
```

## Best Practices

### 1. Choose the Right Template
- Use `standard_fact_table` for general fact tables
- Use specialized templates (`ecommerce_orders`, `event_tracking`) for specific domains
- Use dimension templates for lookup tables

### 2. Leverage Auto-Inference
Templates automatically exclude common ETL metadata columns and infer additional dimensions:

```yaml
# Templates automatically exclude these patterns:
exclude_columns: [_fivetran_synced, _dbt_updated_at, _loaded_at]

# And enable auto-inference:
auto_infer:
  dimensions: true
```

### 3. Customize Parameters
Always customize parameters to match your table structure:

```yaml
parameters:
  table_name: your_fact_table
  date_column: your_date_column  # Don't use defaults blindly
  amount_column: your_amount_column
```

### 4. Add Domain-Specific Elements
Extend templates with business-specific entities, dimensions, and measures:

```yaml
# Template provides base structure
template: std.ecommerce_orders
parameters: {...}

# Add your specific business logic
entities:
  - name: promotion_id
    type: foreign
    expr: applied_promotion_id

measures:
  - name: loyalty_points_earned
    agg: sum
    expr: loyalty_points
    agg_time_dimension: order_date
```

### 5. Use Entity Sets for Consistency
Apply entity sets to ensure consistent entity definitions across related tables:

```yaml
# All e-commerce fact tables use same entities
semantic_models:
  - name: orders
    entity_set: std.ecommerce_fact
  
  - name: order_items  
    entity_set: std.ecommerce_fact
  
  - name: returns
    entity_set: std.ecommerce_fact
```

## Template Development

### Creating Custom Templates

You can create organization-specific templates:

```yaml
# templates/company_templates.yml
semantic_model_templates:
  company_sales_fact:
    description: "Standard sales fact for our company"
    parameters:
      - name: table_name
        type: string
        required: true
      - name: sales_rep_column
        type: string
        default: "sales_rep_id"
    
    template:
      source: "{{ table_name }}"
      
      entities:
        - name: "{{ sales_rep_column }}"
          type: foreign
          expr: "{{ sales_rep_column }}"
      
      # Company-specific dimensions
      dimensions:
        - name: territory
          type: categorical
          expr: sales_territory
        - name: product_line
          type: categorical
          expr: product_line
```

### Template Testing

Test your templates with different parameter combinations:

```yaml
# Test template with different parameters
semantic_models:
  - name: test_template_basic
    template: company_sales_fact
    parameters:
      table_name: fct_sales_basic
  
  - name: test_template_custom
    template: company_sales_fact
    parameters:
      table_name: fct_sales_detailed
      sales_rep_column: rep_id
```

## Common Issues and Solutions

### Issue: Template Parameter Not Found
**Error:** `Parameter 'amount_column' not found in template`
**Solution:** Check template parameter names and ensure all required parameters are provided.

### Issue: Conflicting Definitions
**Error:** `Entity 'customer_id' defined multiple times`
**Solution:** Templates merge with manual definitions. Use unique names or override template definitions.

### Issue: Auto-Inference Conflicts
**Error:** `Column 'status' inferred as both dimension and measure`
**Solution:** Use `exclude_columns` in template or manual definitions to control inference.

### Issue: Missing Time Dimension
**Error:** `Measure requires agg_time_dimension but none found`
**Solution:** Ensure your date column parameter creates a time dimension, or manually add one.

## Migration from Manual Definitions

### Before (Manual Definition)
```yaml
semantic_models:
  - name: orders
    source: fct_orders
    entities:
      - name: order_id
        type: primary
      - name: customer_id
        type: foreign
    dimensions:
      - name: order_date
        type: time
        type_params:
          time_granularity: day
      - name: order_status
        type: categorical
    measures:
      - name: order_count
        agg: count
        expr: order_id
        agg_time_dimension: order_date
```

### After (Using Template)
```yaml
semantic_models:
  - name: orders
    template: std.ecommerce_orders
    parameters:
      table_name: fct_orders
      order_date_column: order_date
    # Template handles entities, dimensions, and measures automatically
```

The template approach reduces code by ~70% while providing the same functionality plus additional features like multiple time granularities and auto-inference.

## Next Steps

1. Browse the [examples directory](../examples/standard_templates/) for more usage patterns
2. Check the [auto-inference guide](./auto-inference-guide.md) to understand how templates leverage automatic detection
3. See the [entity-sets guide](./entity-sets-guide.md) for reusable entity patterns
4. Review the [template syntax guide](./template-syntax-guide.md) for creating custom templates