# Configuration Quick Reference

## File Locations (searched in order)
- `./bdm_config.yml`
- `metrics/bdm_config.yml`
- `config/bdm_config.yml`

## Essential Configuration Sections

### Paths
```yaml
paths:
  metrics_dir: metrics/              # Input directory
  output_dir: models/semantic/       # Output directory
  template_dir: metrics/_base/       # Template directory
```

### Import Shortcuts
```yaml
imports:
  mappings:
    "_base.templates": "_base/templates.yml"
    "_base.dimensions": "_base/dimension_groups.yml"
    "common": "shared/common.yml"
```

### Auto-Variants
```yaml
auto_variants:
  time_comparisons:
    enabled: true
    default_periods: [wow, mom, yoy]
  territory_splits:
    enabled: true
    territories: [UK, US, EU]
  channel_splits:
    enabled: true
    channels: [web, mobile, api]
```

### Domain-Specific Settings
```yaml
domains:
  marketing:
    auto_variants:
      platform_splits: [facebook, google, tiktok]
      campaign_splits: [brand, performance]
  
  finance:
    auto_variants:
      currency_splits: [USD, EUR, GBP]
      region_splits: [AMER, EMEA, APAC]
```

### Validation Rules
```yaml
validation:
  require_descriptions: true         # All metrics must have descriptions
  require_labels: true              # All metrics must have labels
  validate_dimension_refs: true     # Check dimension references exist
  validate_sources: false           # Check if source tables exist
```

### Compilation Settings
```yaml
compilation:
  expand_auto_variants: true        # Auto-expand variant definitions
  inherit_dimensions: true          # Inherit dimensions from templates
  validate_dimensions: true         # Validate dimension references
  template_expansion:
    enabled: true                   # Enable template expansion
    recursive: true                 # Allow recursive templates
    max_depth: 3                   # Max recursion depth
```

### Output Settings
```yaml
output:
  file_pattern: "{domain}_{type}_metrics.yml"  # File naming pattern
  add_dbt_meta: true                           # Add dbt metadata
  include_metadata: true                       # Include compilation info
  format:
    indent: 2                                  # YAML indentation
    quote_style: double                        # String quote style
```

### Logging
```yaml
logging:
  level: INFO                       # DEBUG, INFO, WARNING, ERROR
  show_sql: false                  # Show generated SQL
  show_yaml: true                  # Show YAML output
```

## CLI Override Examples

```bash
# Override output directory
better-dbt-metrics compile --output-dir models/custom/

# Use specific config file
better-dbt-metrics compile --config config/prod.yml

# Enable debug logging
better-dbt-metrics compile --debug

# Disable auto-variants
better-dbt-metrics compile --no-auto-variants
```

## Common Patterns

### Multi-Environment Setup
```yaml
# config/dev.yml
validation:
  validate_sources: false

# config/prod.yml  
validation:
  validate_sources: true
  require_descriptions: true
```

### Organization-Specific Imports
```yaml
imports:
  mappings:
    "company.core": "shared/company_core.yml"
    "company.dimensions": "shared/company_dimensions.yml"
    "company.entities": "shared/company_entities.yml"
```

### Department-Specific Domains
```yaml
domains:
  marketing:
    auto_variants:
      channel_splits: [organic, paid, email, social]
      attribution_splits: [first_touch, last_touch, multi_touch]
  
  sales:
    auto_variants:
      team_splits: [inbound, outbound, enterprise]
      stage_splits: [qualified, demo, proposal, closed]
```