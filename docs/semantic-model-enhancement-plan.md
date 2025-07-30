# Better-DBT-Metrics Semantic Model Enhancement Plan

## Executive Summary

Better-DBT-Metrics will be enhanced to support full semantic model definitions, transforming it from a metrics-only tool into a complete semantic layer toolkit for dbt. This enhancement maintains all existing features while adding first-class support for semantic models with templates, imports, and auto-inference capabilities.

## Background

The current implementation generates semantic models automatically from metrics, but this approach has limitations:
- Missing dimension definitions (causing empty dimension arrays)
- No control over entity definitions
- Cannot specify time dimension expressions
- Limited ability to define joins and relationships

## Goals

1. **Full Semantic Model Support**: Enable users to define complete semantic models with all dbt semantic layer features
2. **Maintain DRY Principles**: Apply the same import, template, and reference features to semantic models
3. **Backward Compatibility**: Ensure existing metric-only files continue to work
4. **Enhanced Ergonomics**: Make semantic model definition easier than native dbt YAML
5. **Auto-Inference**: Intelligently detect common patterns to reduce boilerplate

## Technical Design

### New Top-Level Keys

```yaml
# Existing
metrics: [...]
metric_templates: {...}
dimension_groups: {...}

# New additions
semantic_models: [...]
semantic_model_templates: {...}
entities: {...}
entity_sets: {...}
```

### Semantic Model Definition

```yaml
semantic_models:
  - name: orders
    description: "Order fact table"
    source: fct_orders  # or model: ref('fct_orders')
    
    # Entity definitions
    entities:
      - name: order_id
        type: primary
        expr: order_id  # Optional, defaults to name
      - name: customer_id
        type: foreign
        expr: customer_id
    
    # Dimension definitions with full control
    dimensions:
      - name: order_date
        type: time
        type_params:
          time_granularity: day
        expr: order_date
      - name: order_week
        type: time
        type_params:
          time_granularity: week
        expr: date_trunc('week', order_date)
      - name: status
        type: categorical
        expr: order_status
    
    # Measures defined at semantic model level
    measures:
      - name: order_count
        agg: count
        expr: order_id
        agg_time_dimension: order_date
      - name: total_revenue
        agg: sum
        expr: revenue
        agg_time_dimension: order_date
    
    # Optional: auto-inference
    auto_infer:
      dimensions: true
      time_dimensions:
        from_columns: [order_date, created_at]
      exclude_columns: [_fivetran_synced]
```

### Semantic Model Templates

```yaml
semantic_model_templates:
  standard_fact_table:
    parameters:
      - name: table_name
        type: string
        required: true
      - name: date_column
        type: string
        required: true
      - name: primary_key
        type: string
        default: "{{ table_name }}_id"
    template:
      source: "{{ table_name }}"
      entities:
        - name: "{{ primary_key }}"
          type: primary
      dimensions:
        - name: date
          type: time
          type_params:
            time_granularity: day
          expr: "{{ date_column }}"
        - name: week
          type: time
          type_params:
            time_granularity: week
          expr: "date_trunc('week', {{ date_column }})"
        - name: month
          type: time
          type_params:
            time_granularity: month
          expr: "date_trunc('month', {{ date_column }})"
      dimension_groups:
        - $ref: dims.standard_dimensions
```

### Entity Management

```yaml
# Reusable entity definitions
entities:
  customer:
    name: customer_id
    type: foreign
    expr: customer_id
  
  product:
    name: product_id
    type: foreign
    expr: product_id

# Entity sets for common patterns
entity_sets:
  ecommerce_fact:
    primary_entity: 
      name: id
      type: primary
    foreign_entities:
      - $ref: entities.customer
      - $ref: entities.product
      - name: order_id
        type: foreign

# Usage in semantic models
semantic_models:
  - name: order_items
    source: fct_order_items
    entity_set: ecommerce_fact  # Applies all entities from the set
```

### Metrics Referencing Semantic Models

```yaml
# Old way (still supported for backward compatibility)
metrics:
  - name: total_revenue
    source: fct_orders
    measure:
      type: sum
      column: revenue

# New way (references semantic model)
metrics:
  - name: total_revenue
    semantic_model: orders  # References the semantic model, not the table
    measure: total_revenue  # References measure defined in semantic model
    # Dimensions and filters still work the same way
    dimensions: $ref(dims.analysis_dimensions)
```

### Auto-Inference Configuration

```yaml
# Global configuration in bdm_config.yml
semantic_models:
  auto_infer:
    enabled: true
    time_dimension_patterns:
      - suffix: [_date, _at, _time]
      - prefix: [date_, created_, updated_]
    categorical_patterns:
      - suffix: [_id, _code, _type, _status]
      - max_cardinality: 100
    exclude_patterns:
      - prefix: [_, tmp_]
      - suffix: [_raw, _hash]
```

## Implementation Phases

### Phase 1: Core Parser Support (Days 1-2)
1. Add semantic_models parsing to BetterDBTParser
2. Support entities and entity_sets
3. Extend dimension parsing for semantic model context
4. Add semantic model validation

### Phase 2: Compiler Updates (Days 3-4)
1. Modify _generate_semantic_models to prefer explicit definitions
2. Implement semantic model template expansion
3. Add measure generation at semantic model level
4. Update metric compilation to reference semantic models

### Phase 3: Templates & Auto-Inference (Days 5-6)
1. Create semantic model template library
2. Implement auto-inference engine
3. Add dimension type detection
4. Create standard templates for common patterns

### Phase 4: Documentation & Examples (Days 7-8)
1. Update README with semantic model examples
2. Create comprehensive semantic-models-guide.md
3. Update all examples to show both approaches
4. Create migration guide

### Phase 5: Testing & Polish (Days 9-10)
1. Add comprehensive test coverage
2. Test backward compatibility
3. Performance optimization
4. Error message improvements

## Migration Strategy

### For Existing Users

1. **No Action Required**: Existing metric-only files will continue to work
2. **Gradual Migration**: Add semantic models incrementally
3. **Migration Tool**: Provide script to analyze and suggest semantic model structure

### Migration Examples

```yaml
# Before (metrics only)
metrics:
  - name: revenue
    source: fct_orders
    measure:
      type: sum
      column: revenue
    dimensions:
      - name: order_date
        type: time

# After (with semantic model)
semantic_models:
  - name: orders
    source: fct_orders
    dimensions:
      - name: order_date
        type: time
        type_params:
          time_granularity: day
    measures:
      - name: revenue_sum
        agg: sum
        expr: revenue
        agg_time_dimension: order_date

metrics:
  - name: revenue
    semantic_model: orders
    measure: revenue_sum
```

## Benefits

1. **Complete Control**: Full access to all dbt semantic layer features
2. **Better Organization**: Separate concerns of data modeling and metric definition
3. **Reusability**: Share semantic models across multiple metrics
4. **Type Safety**: Explicit dimension and measure definitions
5. **Performance**: Better query optimization with proper semantic models

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Breaking existing workflows | Maintain backward compatibility, auto-generation fallback |
| Complexity for simple use cases | Provide templates and auto-inference |
| Learning curve | Comprehensive documentation and examples |
| Migration effort | Gradual migration path, tooling support |

## Success Metrics

1. All existing tests pass with no changes
2. New semantic model tests provide >90% coverage
3. Documentation covers all new features
4. Examples demonstrate common patterns
5. Performance remains the same or improves

## Future Enhancements

1. **Visual Schema Builder**: Web UI for designing semantic models
2. **Import from dbt**: Parse existing dbt models to create semantic models
3. **Validation Rules**: Custom business logic validation
4. **Lineage Tracking**: Automatic dependency graphs
5. **AI Assistant**: Natural language to semantic model generation

## Conclusion

This enhancement positions Better-DBT-Metrics as the most comprehensive and ergonomic way to define dbt semantic models, while maintaining the simplicity that makes it attractive for basic use cases. The gradual migration path ensures existing users can adopt new features at their own pace.