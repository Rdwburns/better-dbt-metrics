# Syntax Corrections and Best Practices

This directory contains corrected examples showing proper Better-DBT-Metrics syntax.

## Files

### template_syntax_example.yml
Shows the correct way to use metric templates:
- ✅ Use `template:` not `$ref:` for metrics
- ✅ Pass parameters via `parameters:` or `params:`
- ✅ Override template fields at the metric level

### marketing_metrics_best_practices.yml
Demonstrates best practices for complex metrics:
- ✅ Correct version (use `version: 2`)
- ✅ Proper `$ref:` syntax for dimensions
- ✅ Simplified source references (just model names)
- ✅ Consistent formatting structure
- ✅ Proper template usage with parameters

## Common Mistakes to Avoid

1. **Wrong**: `$ref: templates.metric_name` for metrics
   **Right**: `template: templates.metric_name`

2. **Wrong**: `$ref(dims.dimension_group)`
   **Right**: `$ref: dims.dimension_group`

3. **Wrong**: `source: models.marts.schema.table_name`
   **Right**: `source: table_name`

4. **Wrong**: `version: 1`
   **Right**: `version: 2`