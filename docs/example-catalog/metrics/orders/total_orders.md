# Total Orders

**Metric Name:** `total_orders`
**Type:** simple
**Domain:** orders

## Description

Total number of orders placed in the system. This is a fundamental metric that tracks the volume of business transactions.

## Business Context

The Total Orders metric is a key performance indicator (KPI) that measures business activity and growth. It's used to:
- Track sales volume trends
- Identify seasonal patterns
- Monitor campaign effectiveness
- Set and track sales targets

## Technical Details

### Definition

- **Aggregation:** count
- **Column:** `order_id`
- **Source Table:** `fct_orders`

## Available Dimensions

This metric can be analyzed by the following dimensions:

| Dimension | Type | Description |
|-----------|------|-------------|
| [order_date](../../dimensions/order_date.md) | time | Date when the order was placed |
| [customer_segment](../../dimensions/customer_segment.md) | categorical | Customer segmentation (Premium, Standard, etc.) |
| [channel](../../dimensions/channel.md) | categorical | Sales channel (Web, Mobile, Store) |
| [product_category](../../dimensions/product_category.md) | categorical | Product category classification |

## SQL Examples

### Basic Query

```sql
SELECT
  date_trunc('month', order_date) as month,
  COUNT(order_id) as total_orders
FROM fct_orders
GROUP BY 1
ORDER BY 1;
```

### Query with Dimension

```sql
SELECT
  date_trunc('month', order_date) as month,
  channel,
  COUNT(order_id) as total_orders
FROM fct_orders
GROUP BY 1, 2
ORDER BY 1, 2;
```

## Dependencies

*This metric has no dependencies*

Metrics that depend on this one:

- [Average Order Value](../average_order_value.md)
- [Orders Growth Rate](../orders_growth_rate.md)

## Usage Examples

### dbt Metrics Query

```sql
SELECT * FROM {{ metrics.calculate(
    metric('total_orders'),
    grain='month',
    dimensions=['customer_segment']
) }}
```

### Tableau Example

1. Connect to the semantic model `sem_fct_orders`
2. Drag the `Total Orders` metric to Rows
3. Drag `Order Date` to Columns (set to Month)
4. Add `Customer Segment` to Color

### Looker Example

```lookml
measure: total_orders {
  type: count
  sql: ${order_id} ;;
  drill_fields: [order_date, customer_segment, channel]
}
```

## Related Metrics

Other metrics that share dimensions or context:

- [Average Order Value](../average_order_value.md) - Average revenue per order
- [Cart Abandonment Rate](../cart_abandonment_rate.md) - Percentage of carts abandoned
- [Median Order Value](../median_order_value.md) - Median order value
- [Orders by Channel](../orders_by_channel.md) - Orders broken down by sales channel

## Metadata

| Property | Value |
|----------|-------|
| importance | high |
| refresh_frequency | hourly |
| data_quality_checks | not_null, positive_values |
| owner | sales_analytics_team |

**Tags:** core-metric, sales, volume