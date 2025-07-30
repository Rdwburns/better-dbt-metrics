# Template Migration Guide - Fixing JSON Escaping Issues

## Overview

We've updated the template expansion system to process YAML directly without JSON conversion. This eliminates quote escaping issues and makes templates more intuitive to write.

## What Changed

### Old System (JSON-based)
- Templates were converted to JSON before processing
- Required complex quote escaping: `filter: "{{ filter | default('date >= \"2020-01-01\"') }}"`
- Nested defaults often failed with JSON parsing errors

### New System (YAML-safe)
- Templates are processed directly in YAML
- Simple, readable syntax: `filter: "{{ filter or 'date >= \'2020-01-01\' }}"`
- No JSON conversion means no escaping issues

## Migration Steps

### 1. Update Template Syntax

**Before:**
```yaml
filter: "{{ numerator_filter | default(base_filter) | default('date >= \"2020-01-01\"') }}"
```

**After:**
```yaml
filter: "{{ numerator_filter or base_filter or 'date >= \'2020-01-01\' }}"
```

### 2. Update Your Metrics

**Before:**
```yaml
- name: average_order_value
  $use: t.inline_ratio_metric
  parameters:
    base_filter: "date >= \"2020-01-01\" AND status = \"completed\""  # Double quotes cause issues
```

**After:**
```yaml
- name: average_order_value
  $use: t.inline_ratio_metric
  parameters:
    base_filter: "date >= '2020-01-01' AND status = 'completed'"  # Single quotes work perfectly
```

### 3. Use Multiline Syntax for Complex Filters

**Recommended for readability:**
```yaml
- name: complex_metric
  $use: t.inline_ratio_metric
  parameters:
    base_filter: |
      date >= '2020-01-01' 
      AND status IN ('completed', 'settled')
      AND region = 'US'
```

## Quick Reference

### Do's ✅
- Use single quotes in SQL strings: `status = 'active'`
- Use the `or` operator for defaults: `{{ param or 'default' }}`
- Use multiline syntax for complex expressions
- Omit optional parameters - templates handle defaults

### Don'ts ❌
- Don't use nested `default()` filters
- Don't escape quotes with backslashes
- Don't mix quote styles in SQL
- Don't include empty strings as defaults unnecessarily

## Common Patterns

### Optional Filters
```yaml
# Template definition
filter: "{{ custom_filter or '' }}"

# Usage - with filter
parameters:
  custom_filter: "status = 'active'"

# Usage - without filter (empty string used)
parameters:
  # custom_filter not provided
```

### SQL Value Quoting
```yaml
# For string values in SQL
filter: "channel = '{{ channel_name }}'"

# For numeric values
filter: "amount > {{ threshold }}"

# For IN clauses
filter: "status IN ('active', 'pending', 'completed')"
```

### Complex Conditions
```yaml
# Use Jinja2 conditionals
expression: |
  {% if include_tax %}
    revenue + tax_amount
  {% else %}
    revenue
  {% endif %}
```

## Troubleshooting

### Error: "Expecting ',' delimiter"
**Cause:** JSON parsing error from nested quotes
**Fix:** Update to use `or` syntax instead of `default()` chains

### Error: "Invalid YAML syntax"
**Cause:** Mixed quote styles
**Fix:** Use single quotes consistently in SQL strings

### Error: "Template parameter not found"
**Cause:** Typo in parameter name or missing required parameter
**Fix:** Check template documentation for correct parameter names

## Need Help?

1. Check the `examples/template_usage_guide.yml` for working examples
2. Run with `--verbose` flag to see template expansion details
3. Validate your YAML syntax with a linter before compilation

The new system is more robust and easier to use. Once migrated, you'll find templates much more maintainable!