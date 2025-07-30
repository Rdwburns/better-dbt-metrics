# Template Syntax Guide - Avoiding JSON Issues

## The Problem

Better-DBT-Metrics templates are processed through JSON, which can cause issues with quotes and special characters. Here's how to write templates that work reliably.

## Best Practices

### 1. Use Single Quotes in SQL Strings

❌ **BAD:**
```yaml
filter: "{{ base_filter | default('date >= \"2020-01-01\"') }}"
```

✅ **GOOD:**
```yaml
filter: "{{ base_filter | default(\"date >= '2020-01-01'\") }}"
```

### 2. Use Parameter Defaults

❌ **BAD:**
```yaml
template:
  filter: "{{ filter | default('status = \"active\"') }}"
```

✅ **GOOD:**
```yaml
parameters:
  - name: filter
    type: string
    default: "status = 'active'"
template:
  filter: "{{ filter }}"
```

### 3. Use Jinja2 'or' Syntax

❌ **BAD:**
```yaml
column: "{{ column_name | default('\"user_id\"') }}"
```

✅ **GOOD:**
```yaml
column: "{{ column_name or 'user_id' }}"
```

### 4. Use YAML Block Scalars for Complex Expressions

❌ **BAD:**
```yaml
expression: "{{ formula | default('metric(\"revenue\") / metric(\"customers\")') }}"
```

✅ **GOOD:**
```yaml
expression: |
  {{ formula | default('metric("revenue") / metric("customers")') }}
```

### 5. Avoid Nested Defaults

❌ **BAD:**
```yaml
filter: "{{ custom_filter | default(base_filter) | default('date >= \"2020-01-01\"') }}"
```

✅ **GOOD:**
```yaml
filter: "{{ custom_filter or base_filter or 'date >= \\'2020-01-01\\'' }}"
```

## SQL-Safe Templates

### For Filters
```yaml
# Option 1: Use single quotes
filter: "{{ filter_expr | default(\"date >= '2020-01-01'\") }}"

# Option 2: Use block scalar
filter: |
  {{ filter_expr | default("date >= '2020-01-01'") }}

# Option 3: Build filter in template
filter: "date >= {{ start_date | default(\"'2020-01-01'\") }}"
```

### For Column Names
```yaml
# Simple column reference
column: "{{ column_name | default('revenue_amount') }}"

# With table prefix
column: "{{ table_alias | default('t1') }}.{{ column_name | default('amount') }}"
```

### For Complex Expressions
```yaml
# Use block scalars for readability
expression: |
  CASE 
    WHEN {{ condition | default("status = 'active'") }} 
    THEN {{ true_value | default('1') }}
    ELSE {{ false_value | default('0') }}
  END
```

## Template Parameter Types

### String Parameters
```yaml
parameters:
  - name: base_filter
    type: string
    default: "date >= '2020-01-01'"  # Single quotes for SQL
```

### Numeric Parameters
```yaml
parameters:
  - name: threshold
    type: number
    default: 100
```

### Boolean Parameters
```yaml
parameters:
  - name: include_nulls
    type: boolean
    default: false
```

## Common Patterns

### 1. Optional Filters
```yaml
# Template
filter: "{{ filter_expr | default('') }}"

# Usage - filter only applied if provided
parameters:
  filter_expr: "status = 'completed'"
```

### 2. Dynamic Column Names
```yaml
# Template
measure:
  type: "{{ agg_type | default('sum') }}"
  column: "{{ column_name }}"

# Usage
parameters:
  agg_type: count_distinct
  column_name: user_id
```

### 3. Conditional Dimensions
```yaml
# Template
dimensions: |
  {%- if include_time_dimensions %}
  - name: date_day
    type: time
    grain: day
  {%- endif %}
  {%- if include_geo_dimensions %}
  - name: country
    type: categorical
  {%- endif %}
```

## Testing Your Templates

1. **Validate YAML Syntax:**
   ```bash
   yamllint metrics/_base/templates.yml
   ```

2. **Test Template Expansion:**
   ```python
   # Test script
   from src.features.templates import TemplateEngine
   
   engine = TemplateEngine()
   result = engine.expand_template('your_template', {
       'param1': 'value1',
       'param2': 'value2'
   })
   print(result)
   ```

3. **Check Compilation:**
   ```bash
   better-dbt-metrics compile --debug
   ```

## Future Improvements

The template system could be enhanced to:
1. Use pure YAML processing without JSON conversion
2. Add SQL-aware string escaping
3. Support YAML-native template syntax
4. Provide better error messages for quote issues