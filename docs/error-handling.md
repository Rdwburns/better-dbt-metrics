# Enhanced Error Handling

Better-DBT-Metrics now provides comprehensive error handling with clear, actionable messages to help you quickly identify and fix issues.

## Features

### 🔍 Pre-Compilation Validation
Catches common issues before full compilation:
- YAML syntax errors with line numbers
- Missing required fields
- Invalid metric types
- Unresolved references
- Best practice violations

### 📍 Contextual Error Messages
Every error includes:
- **Clear description** of what went wrong
- **File location** with line numbers when available
- **Metric name** for context
- **Helpful suggestions** on how to fix it
- **Related errors** when applicable

### 📊 Comprehensive Reporting
Multiple output formats for different needs:
- **Terminal**: Human-readable with colors and icons
- **JSON**: Structured data for programmatic use
- **JUnit XML**: For CI/CD integration

## Command Options

```bash
# Basic compilation with enhanced errors
better-dbt-metrics compile

# Verbose mode shows all warnings and info
better-dbt-metrics compile --verbose

# Skip pre-validation for faster compilation
better-dbt-metrics compile --no-pre-validate

# Output errors as JSON
better-dbt-metrics compile --report-format json

# For CI/CD pipelines
better-dbt-metrics compile --report-format junit > test-results.xml
```

## Error Categories

### Syntax Errors
YAML parsing issues with specific line numbers:
```
❌ ERROR: YAML syntax error: expected <block end>, but found '-'
  📍 Location: metrics/revenue.yml:15
  💡 Suggestion: Common YAML issues:
     - Check indentation (use spaces, not tabs)
     - Ensure proper quoting for special characters
     - Verify list syntax (- items)
```

### Reference Errors
Unresolved imports or references:
```
❌ ERROR: Cannot resolve reference: $ref: time.weekly
  📍 Location: metrics/sales.yml
  📊 Metric: weekly_revenue
  💡 Suggestion: Ensure the dimension is imported and the reference path is correct.
     Use '$ref:' for dimension references and '$use:' for template references.
```

### Metric Definition Errors
Missing or invalid metric configuration:
```
❌ ERROR: Missing required field 'numerator' for ratio metric
  📍 Location: metrics/ratios.yml
  📊 Metric: conversion_rate
  💡 Suggestion: A ratio metric requires these fields: name, numerator, denominator
```

### Validation Warnings
Best practice recommendations:
```
⚠️ WARNING: Missing description
  📍 Location: metrics/finance.yml
  📊 Metric: total_revenue
  💡 Suggestion: Add a 'description' field to metric 'total_revenue' to document its purpose
```

## Example Output

### Terminal Report (Default)
```
============================================================
📊 Better-DBT-Metrics Compilation Report
============================================================

📈 Compilation Statistics:
  Files processed: 12
  Metrics compiled: 45
  Models generated: 8

📋 Issues: ❌ 2 error(s) | ⚠️ 3 warning(s)

❌ Errors (must fix):
----------------------------------------

1. ❌ ERROR: Cannot find import: '../templates/dimensions/time.yml'
  📍 Location: metrics/revenue.yml:3
  💡 Suggestion: Check that the file exists and the path is correct.
     Paths can be relative to the current file or absolute from the project root.
  📋 Context:
     attempted_import: ../templates/dimensions/time.yml
     search_paths: ['Current directory', 'Project root', 'Template directories']

2. ❌ ERROR: Invalid metric type: 'percentage'
  📍 Location: metrics/kpis.yml
  📊 Metric: growth_rate
  💡 Suggestion: Valid metric types are: simple, ratio, derived, cumulative, conversion

⚠️ Warnings (should review):
----------------------------------------

1. ⚠️ WARNING: Potential performance issue in metric 'complex_calculation'
  📍 Location: metrics/derived.yml
  📊 Metric: complex_calculation
  💡 Suggestion: Consider adding filters or materializing this metric for better performance

============================================================
❌ Compilation failed with errors
Please fix the errors above and try again
============================================================
```

### JSON Report
```json
{
  "success": false,
  "statistics": {
    "files_processed": 12,
    "metrics_compiled": 43,
    "models_generated": 8
  },
  "issues": {
    "summary": {
      "errors": 2,
      "warnings": 3,
      "info": 0,
      "total": 5
    },
    "errors": [
      {
        "message": "Cannot find import: '../templates/dimensions/time.yml'",
        "category": "import",
        "severity": "error",
        "file_path": "metrics/revenue.yml",
        "line_number": 3,
        "suggestion": "Check that the file exists..."
      }
    ]
  }
}
```

## Pre-Compilation Checks

The pre-validator runs automatically and checks for:

1. **YAML Syntax**
   - Valid YAML structure
   - Proper indentation
   - Correct data types

2. **Required Fields**
   - All metric types have required fields
   - Proper structure for each type

3. **References**
   - Imported files exist
   - Referenced dimensions/templates are available
   - No circular dependencies

4. **Best Practices**
   - Metrics have descriptions
   - Naming conventions followed
   - Performance considerations

## CI/CD Integration

### GitHub Actions Example
```yaml
- name: Compile Metrics
  run: |
    better-dbt-metrics compile \
      --report-format junit \
      --verbose \
      > test-results.xml
      
- name: Upload Test Results
  if: always()
  uses: actions/upload-artifact@v3
  with:
    name: metric-compilation-results
    path: test-results.xml
    
- name: Publish Test Results
  uses: EnricoMi/publish-unit-test-result-action@v2
  if: always()
  with:
    files: test-results.xml
```

### GitLab CI Example
```yaml
compile-metrics:
  script:
    - better-dbt-metrics compile --report-format junit > compilation-results.xml
  artifacts:
    reports:
      junit: compilation-results.xml
```

## Debugging Tips

### Enable Verbose Mode
See all warnings and informational messages:
```bash
better-dbt-metrics compile --verbose
```

### Debug Mode
Get detailed stack traces for unexpected errors:
```bash
better-dbt-metrics compile --debug
```

### Check Specific Files
Validate without full compilation:
```bash
better-dbt-metrics validate --input-dir metrics/
```

## Common Error Patterns

### 1. Import Path Issues
**Error**: Cannot find import
**Solution**: Use relative paths from the current file or absolute from project root

### 2. Missing Measure Type
**Error**: Measure is missing 'type'
**Solution**: Add `type: sum` (or count, average, etc.) to your measure

### 3. Invalid Dimension Format
**Error**: Invalid dimension format
**Solution**: Use either:
- String: `'customer_id'`
- Dictionary: `{name: 'customer_id', type: 'categorical'}`
- Reference: `{$ref: 'dims.customer_id'}`

### 4. Circular Dependencies
**Error**: Circular dependency detected
**Solution**: Review metric dependencies and remove circular references

## Benefits

1. **Faster Development**: Catch errors early with pre-validation
2. **Clearer Errors**: Understand exactly what's wrong and how to fix it
3. **CI/CD Ready**: Multiple output formats for automation
4. **Best Practices**: Built-in recommendations improve metric quality
5. **Debugging Support**: Verbose and debug modes for troubleshooting

The enhanced error handling makes Better-DBT-Metrics more reliable and easier to use, especially in team environments and CI/CD pipelines.