# Configuration Guide - bdm_config.yml

Better-DBT-Metrics can be configured using a `bdm_config.yml` file to customize compilation behavior, set default paths, and define organization-specific settings.

## Configuration File Location

The framework looks for `bdm_config.yml` in the following locations (in order):
1. Current directory
2. `metrics/bdm_config.yml`
3. `config/bdm_config.yml`

The first file found will be used. If no configuration file is found, default settings will be applied.

## Configuration Structure

### Basic Example

```yaml
version: 1

paths:
  metrics_dir: metrics/
  output_dir: models/marts/metrics/
  template_dir: metrics/_base/

imports:
  mappings:
    "_base.templates": "_base/templates.yml"
    "_base.dimension_groups": "_base/dimension_groups.yml"
```

### Full Configuration Reference

```yaml
# Better-DBT-Metrics Configuration
version: 1

# Input/Output Configuration
paths:
  metrics_dir: metrics/                    # Where to find metric definitions
  output_dir: models/marts/metrics/        # Where to output compiled models
  template_dir: metrics/_base/             # Where to find templates

# Import Path Resolution
imports:
  # Map import aliases to actual file paths
  mappings:
    "_base.templates": "_base/templates.yml"
    "_base.dimension_groups": "_base/dimension_groups.yml"
    "_base.config": "_base/config.yml"
  
  # Additional paths to search for imports
  search_paths:
    - metrics/
    - metrics/_base/

# Compilation Settings
compilation:
  expand_auto_variants: true      # Auto-expand variant definitions
  inherit_dimensions: true        # Inherit dimensions from templates
  validate_dimensions: true       # Validate dimension references
  validate_sources: true         # Validate source tables exist
  
  # Template expansion settings
  template_expansion:
    enabled: true                # Enable template expansion
    recursive: true              # Allow recursive template expansion
    max_depth: 3                 # Maximum recursion depth

# Auto-Variant Configuration
auto_variants:
  # Time comparisons (WoW, MoM, YoY)
  time_comparisons:
    enabled: true
    default_periods:
      - wow
      - mom
      - yoy
    
  # Territory splits
  territory_splits:
    enabled: true
    territories:
      - UK
      - CE
      - EE
    
  # Channel splits
  channel_splits:
    enabled: true
    channels:
      - shopify
      - amazon
      - tiktok_shop

# Domain-Specific Settings
domains:
  # Settings for metrics in the 'influencer' directory
  influencer:
    auto_variants:
      tier_splits:
        - nano
        - micro
        - mid
        - macro
      platform_splits:
        - instagram
        - tiktok
        - youtube
  
  # Settings for metrics in the 'paid_media' directory
  paid_media:
    auto_variants:
      platform_splits:
        - facebook_ads
        - google_ads
        - tiktok_ads
      campaign_type_splits:
        - prospecting
        - retargeting

# Output Configuration
output:
  file_pattern: "{domain}_{subdomain}_metrics.yml"  # Output file naming
  add_dbt_meta: true                               # Add dbt metadata
  include_metadata: true                           # Include compilation metadata
  
  # Format settings
  format:
    indent: 2                    # YAML indentation
    quote_style: double          # Quote style for strings
    preserve_comments: true      # Preserve comments in output

# Validation Rules
validation:
  require_descriptions: true     # All metrics must have descriptions
  require_labels: true          # All metrics must have labels
  validate_dimension_refs: true  # Validate dimension references exist
  validate_sources: false       # Validate source tables (set true after dbt setup)

# Logging
logging:
  level: INFO                   # Log level (DEBUG, INFO, WARNING, ERROR)
  show_sql: false              # Show generated SQL in logs
  show_yaml: true              # Show YAML output in logs
```

## Configuration Options

### Paths Configuration

Controls where the framework looks for files and where it outputs results:

```yaml
paths:
  metrics_dir: metrics/              # Input directory
  output_dir: models/semantic/       # Output directory
  template_dir: metrics/_base/       # Template directory
```

### Import Mappings

Define shortcuts for commonly imported files:

```yaml
imports:
  mappings:
    "common.dimensions": "shared/common_dimensions.yml"
    "finance.templates": "domains/finance/templates.yml"
```

This allows you to use:
```yaml
imports:
  - common.dimensions
```

Instead of:
```yaml
imports:
  - shared/common_dimensions.yml
```

### Auto-Variants

Configure automatic metric variant generation:

```yaml
auto_variants:
  time_comparisons:
    enabled: true
    default_periods: [wow, mom, yoy, qoq]
```

### Domain-Specific Settings

Apply different settings based on metric location:

```yaml
domains:
  marketing:
    auto_variants:
      campaign_splits:
        - brand
        - performance
        - retention
```

Metrics in `metrics/marketing/` will automatically get these variants.

### Validation Rules

Control compilation-time validation:

```yaml
validation:
  require_descriptions: true    # Enforce metric descriptions
  require_labels: true         # Enforce metric labels
  validate_sources: true       # Check if sources exist
```

## Using Configuration

### CLI Override

Command-line options override configuration file settings:

```bash
# This overrides the output_dir in config
better-dbt-metrics compile --output-dir models/custom/
```

### Environment-Specific Configs

Create different configs for different environments:

```bash
# Development
better-dbt-metrics compile --config config/dev.yml

# Production
better-dbt-metrics compile --config config/prod.yml
```

### Debugging Configuration

Use debug mode to see which configuration is loaded:

```bash
better-dbt-metrics compile --debug
```

## Examples

### Minimal Configuration

```yaml
version: 1

imports:
  mappings:
    "_base": "_base/all.yml"
```

### Organization-Specific Configuration

```yaml
version: 1

# Your company's specific structure
paths:
  metrics_dir: analytics/metrics/
  output_dir: transformations/semantic/
  
# Your naming conventions
output:
  file_pattern: "sem_{domain}_{type}.yml"
  
# Your validation requirements
validation:
  require_descriptions: true
  require_labels: true
  validate_sources: true
```

### Multi-Domain Configuration

```yaml
version: 1

domains:
  finance:
    auto_variants:
      currency_splits: [USD, EUR, GBP]
      
  marketing:
    auto_variants:
      channel_splits: [organic, paid, email]
      
  product:
    auto_variants:
      platform_splits: [web, mobile, api]
```

## Best Practices

1. **Version Control**: Always include `bdm_config.yml` in version control
2. **Documentation**: Document any custom mappings or domain settings
3. **Validation**: Start with validation disabled, enable after setup
4. **Paths**: Use relative paths for portability
5. **Domains**: Organize metrics by domain for better auto-variant application

## Troubleshooting

### Configuration Not Loading

1. Check file location - must be in one of the search paths
2. Verify YAML syntax - use a YAML validator
3. Run with `--debug` to see configuration loading details

### Import Mappings Not Working

1. Ensure mapping keys match exactly what's used in imports
2. Check that mapped files exist
3. Use relative paths from the config file location

### Domain Settings Not Applied

1. Verify metrics are in the expected directory structure
2. Check domain name matches directory name exactly
3. Use `--debug` to see domain detection