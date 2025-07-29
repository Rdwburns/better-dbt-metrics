# Custom Auto-Variants

Better-DBT-Metrics now supports custom auto-variants that can add multiple dimensions and filters to your metrics automatically.

## Overview

In addition to the standard `time_comparison` and `by_dimension` auto-variants, you can now define custom variant types that:
- Add multiple dimensions at once
- Apply specific filters
- Create meaningful metric variations for your business needs

## Syntax

```yaml
metrics:
  - name: revenue
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: amount
    auto_variants:
      # Standard auto-variants (still supported)
      time_comparison: [wow, mom, yoy]
      by_dimension: [channel, region]
      
      # NEW: Custom auto-variants
      regional_analysis:
        - name_suffix: _us_by_channel
          dimensions: [region, channel, customer_segment]
          filter: "country = 'US'"
        - name_suffix: _eu_by_product
          dimensions: [region, product_category]
          filter: "region = 'EU'"
      
      channel_performance:
        - shop_code: shopify
          label_suffix: _shopify
          dimensions: [shop_code, channel_type]
        - shop_code: amazon
          label_suffix: _amazon
          dimensions: [shop_code, marketplace]
```

## How It Works

### 1. Multiple Dimensions
Each variant can add multiple dimensions to the base metric:
```yaml
custom_analysis:
  - name_suffix: _detailed
    dimensions: [region, channel, product_category, customer_segment]
```

### 2. Automatic Filter Generation
Define filters explicitly or use key-value pairs:
```yaml
platform_splits:
  - name_suffix: _ios
    dimensions: [platform, device_type]
    filter: "platform = 'ios'"  # Explicit filter
  
  - platform: android  # Key-value pairs become filters
    device_type: mobile
    label_suffix: _android_mobile
    dimensions: [platform, device_type]
```

The second variant automatically generates: `filter: "platform = 'android' AND device_type = 'mobile'"`

### 3. Filter Combination
If your base metric has a filter, it's combined with the variant filter:
```yaml
metrics:
  - name: completed_orders
    filter: "status = 'completed'"
    auto_variants:
      premium_analysis:
        - name_suffix: _premium
          filter: "customer_tier = 'premium'"
          dimensions: [customer_tier]
```

This creates `completed_orders_premium` with filter: `"(status = 'completed') AND (customer_tier = 'premium')"`

### 4. Dimension Deduplication
If a dimension already exists in the base metric, it won't be duplicated:
```yaml
metrics:
  - name: revenue
    dimensions: [date, region]
    auto_variants:
      detailed:
        - name_suffix: _with_channel
          dimensions: [region, channel]  # region won't be duplicated
```

## Common Patterns

### Regional Analysis
```yaml
auto_variants:
  regional_breakdowns:
    - name_suffix: _north_america
      dimensions: [country, state, channel]
      filter: "region = 'NA'"
    - name_suffix: _europe
      dimensions: [country, channel, product_line]
      filter: "region = 'EU'"
    - name_suffix: _apac
      dimensions: [country, channel]
      filter: "region = 'APAC'"
```

### Customer Segmentation
```yaml
auto_variants:
  customer_segments:
    - name_suffix: _enterprise
      dimensions: [company_size, industry, product]
      customer_tier: enterprise
      min_employees: 1000
    - name_suffix: _mid_market
      dimensions: [company_size, industry]
      customer_tier: mid_market
      min_employees: 100
      max_employees: 999
    - name_suffix: _smb
      dimensions: [company_size, channel]
      customer_tier: smb
      max_employees: 99
```

### Platform-Specific Metrics
```yaml
auto_variants:
  platform_analysis:
    - name_suffix: _web_desktop
      dimensions: [platform, browser, user_type]
      platform: web
      device_category: desktop
    - name_suffix: _mobile_app
      dimensions: [platform, app_version, os_version]
      platform: mobile_app
    - name_suffix: _api
      dimensions: [platform, api_version, client_type]
      platform: api
```

### Channel Performance
```yaml
auto_variants:
  channel_performance:
    - channel_type: paid_search
      label_suffix: _paid_search
      dimensions: [channel, campaign_type, keyword_match_type]
    - channel_type: paid_social
      label_suffix: _paid_social
      dimensions: [channel, platform, ad_format]
    - channel_type: organic
      label_suffix: _organic
      dimensions: [channel, content_type]
```

## Integration with Templates

Custom auto-variants work seamlessly with metric templates:

```yaml
metric_templates:
  revenue_base:
    template:
      type: simple
      source: "{{ source_table }}"
      measure:
        type: sum
        column: "{{ revenue_column }}"
      auto_variants:
        territory_analysis:
          - territory: UK
            label_suffix: _uk
            dimensions: [territory, channel]
          - territory: US
            label_suffix: _us
            dimensions: [territory, state, channel]

metrics:
  - name: gross_revenue
    template: revenue_base
    parameters:
      source_table: fct_orders
      revenue_column: gross_amount
    # The territory_analysis variants are automatically included
```

## Best Practices

1. **Use descriptive names**: Choose variant type names that clearly indicate their purpose (e.g., `regional_analysis`, not `custom1`)

2. **Be consistent with suffixes**: Use either `name_suffix` or `label_suffix` consistently across your variants

3. **Document complex filters**: For complex filter logic, consider adding a comment explaining the business logic

4. **Leverage key-value filters**: For simple equality filters, use key-value pairs for cleaner YAML:
   ```yaml
   # Instead of:
   filter: "shop_code = 'shopify' AND region = 'US'"
   
   # Use:
   shop_code: shopify
   region: US
   ```

5. **Group related variants**: Organize variants by their analytical purpose to make metric definitions more maintainable

## Migration from Manual Variants

If you're currently defining variants manually, custom auto-variants can significantly reduce repetition:

**Before:**
```yaml
metrics:
  - name: revenue
    # base metric definition...
  
  - name: revenue_us_shopify
    # copy of base metric with modifications...
    filter: "country = 'US' AND shop_code = 'shopify'"
    dimensions: [date, region, channel, shop_code]
  
  - name: revenue_us_amazon
    # another copy with modifications...
    filter: "country = 'US' AND shop_code = 'amazon'"
    dimensions: [date, region, channel, shop_code]
  
  - name: revenue_eu_shopify
    # yet another copy...
    filter: "region = 'EU' AND shop_code = 'shopify'"
    dimensions: [date, region, channel, shop_code]
```

**After:**
```yaml
metrics:
  - name: revenue
    # base metric definition...
    auto_variants:
      regional_shop_analysis:
        - country: US
          shop_code: shopify
          label_suffix: _us_shopify
          dimensions: [region, channel, shop_code]
        - country: US
          shop_code: amazon
          label_suffix: _us_amazon
          dimensions: [region, channel, shop_code]
        - region: EU
          shop_code: shopify
          label_suffix: _eu_shopify
          dimensions: [region, channel, shop_code]
```

This reduces repetition and makes it easy to add new variants or modify existing ones.