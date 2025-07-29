# Metrics Glossary

Common terms and definitions used in metrics.

## Metric Types

- **Simple**: Basic aggregation metrics (sum, count, average)
- **Ratio**: Metrics calculated as numerator/denominator
- **Derived**: Metrics calculated from other metrics
- **Cumulative**: Running totals or period-to-date metrics
- **Conversion**: Funnel or conversion rate metrics

## Aggregation Types

- **sum**: Total of all values
- **count**: Number of records
- **count_distinct**: Number of unique values
- **average/avg**: Mean value
- **median**: Middle value
- **percentile**: Value at specific percentile
- **min/max**: Minimum or maximum value

## Time Grains

- **day**: Daily granularity
- **week**: Weekly granularity (Sunday-Saturday)
- **month**: Monthly granularity
- **quarter**: Quarterly granularity
- **year**: Yearly granularity

## Dimension Types

- **categorical**: Discrete values like product categories, customer segments
- **time**: Time-based dimensions with specific granularities
- **numeric**: Continuous numeric dimensions (rare, usually binned)

## Common Business Terms

- **AOV (Average Order Value)**: Average revenue per order
- **CLV/LTV (Customer Lifetime Value)**: Total revenue expected from a customer
- **CAC (Customer Acquisition Cost)**: Cost to acquire a new customer
- **MRR (Monthly Recurring Revenue)**: Predictable revenue stream per month
- **ARR (Annual Recurring Revenue)**: MRR × 12
- **Churn Rate**: Percentage of customers who stop using the service
- **Conversion Rate**: Percentage completing a desired action
- **Cohort**: Group of users who share a common characteristic within a defined time-span

## Metric Metadata

- **Grain**: The level of detail at which a metric is calculated
- **Filter**: Conditions applied to limit the data included in a metric
- **Window**: Time period over which a cumulative metric is calculated
- **Entity**: The primary object being measured (e.g., user, order, session)

## Data Quality Terms

- **Completeness**: Percentage of non-null values
- **Freshness**: How recent the data is
- **Accuracy**: How correct the data values are
- **Consistency**: Whether data follows expected patterns

## Performance Terms

- **Materialization**: Pre-computing metrics for faster queries
- **Incremental**: Processing only new/changed data
- **Partitioning**: Dividing data by a key (often date) for efficiency