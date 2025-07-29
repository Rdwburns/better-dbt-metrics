# Metric Catalog Generator

Better-DBT-Metrics includes a powerful metric catalog generator that creates comprehensive, searchable documentation for all your metrics.

## Features

- **📚 Comprehensive Documentation**: Generates detailed pages for each metric
- **🔍 Search Interface**: Interactive search with filtering by type and domain
- **📊 Multiple Formats**: Detailed, compact, or custom templates
- **🌐 GitHub Pages Ready**: Deploy directly to GitHub Pages
- **📈 Data Lineage**: Visualize metric dependencies
- **💾 SQL Examples**: Auto-generated query examples
- **🏷️ Smart Organization**: Group by domain, type, or custom taxonomy

## Quick Start

### Generate Catalog via CLI

```bash
# Basic catalog generation
better-dbt-metrics catalog

# With all features enabled
better-dbt-metrics catalog \
  --input-dir metrics/ \
  --output-dir docs/metrics \
  --format detailed \
  --include-search \
  --include-lineage \
  --include-sql

# Compact format for quick reference
better-dbt-metrics catalog --format compact

# Custom template
better-dbt-metrics catalog \
  --format custom \
  --custom-template templates/metric-doc.md
```

### Generate via GitHub Actions

```yaml
name: Generate Metric Catalog
on:
  push:
    paths: ['metrics/**/*.yml']

jobs:
  catalog:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    
    - name: Generate Catalog
      uses: better-dbt-metrics/catalog-action@v1
      with:
        deploy-to-pages: true
```

## Catalog Structure

```
docs/metrics/
├── index.md                    # Main catalog page
├── search.html                 # Interactive search interface
├── search-index.json          # Search data
├── glossary.md                # Terms and definitions
├── domains/                   # Domain-specific pages
│   ├── orders.md
│   ├── customers.md
│   └── revenue.md
├── metrics/                   # Individual metric pages
│   ├── orders/
│   │   ├── total_orders.md
│   │   └── average_order_value.md
│   └── customers/
│       └── lifetime_value.md
├── dimensions/                # Dimension catalog
│   └── index.md
├── lineage/                   # Dependency visualizations
│   └── index.md
└── relationships.md          # Metric relationships
```

## Page Types

### Metric Pages

Each metric gets a dedicated page with:

- **Header**: Name, type, domain, description
- **Business Context**: Why this metric matters
- **Technical Details**: Calculation, source, filters
- **Dimensions**: Available analysis dimensions
- **SQL Examples**: Ready-to-use queries
- **Dependencies**: What it depends on and what depends on it
- **Usage Examples**: BI tool integration examples
- **Related Metrics**: Similar or connected metrics

### Search Interface

Interactive search with:
- Real-time filtering
- Type-based filtering (simple, ratio, derived, etc.)
- Dimension search
- Domain grouping

### Lineage Visualization

Mermaid diagrams showing:
- Metric dependency graphs
- Data flow visualization
- Impact analysis

## Configuration

### Catalog Config

```yaml
# bdm_config.yml
catalog:
  output_dir: docs/metrics
  format: detailed
  group_by_domain: true
  include_search: true
  include_lineage: true
  include_sql: true
  include_glossary: true
  
  # Custom sections
  custom_sections:
    - name: data_quality
      title: "Data Quality"
      template: |
        ## Data Quality
        - Freshness: {{ metric.meta.freshness }}
        - Completeness: {{ metric.meta.completeness }}
```

### Metric Metadata

Enhance documentation with metadata:

```yaml
metrics:
  - name: total_revenue
    description: "Total revenue from all sources"
    meta:
      business_context: |
        Primary KPI for executive dashboards.
        Used for quarterly earnings reports.
      importance: critical
      owner: finance_team
      refresh_frequency: hourly
      tags: [executive, finance, revenue]
      bi_examples:
        tableau: |
          1. Connect to semantic model
          2. Drag Total Revenue to Rows
          3. Add filters as needed
```

## Custom Templates

Create your own documentation format:

```markdown
# {{label}}

**Technical Name:** `{{name}}`
**Business Owner:** {{meta.owner}}

## Quick Reference
- **Type:** {{type}}
- **Update Frequency:** {{meta.refresh_frequency}}
- **Data Source:** {{source}}

## Calculation
{{#if type == 'simple'}}
{{measure.type}}({{measure.column}})
{{/if}}

## Available Breakdowns
{{#each dimensions}}
- {{this.name}}
{{/each}}
```

## Deployment

### GitHub Pages

Automatic deployment with Actions:

```yaml
- name: Deploy to GitHub Pages
  uses: peaceiris/actions-gh-pages@v3
  with:
    github_token: ${{ secrets.GITHUB_TOKEN }}
    publish_dir: ./docs/metrics
```

### Static Site Generators

Compatible with:
- Jekyll
- Hugo
- MkDocs
- Docusaurus

### Internal Wiki

Export to Confluence or similar:

```bash
better-dbt-metrics catalog --format confluence
```

## Best Practices

1. **Rich Descriptions**: Include business context
2. **Consistent Tagging**: Use standard tags
3. **Owner Assignment**: Every metric needs an owner
4. **Update Regularly**: Run on every metric change
5. **Link BI Tools**: Include tool-specific examples
6. **Version Control**: Track catalog in git

## Example Output

See the [example catalog](docs/example-catalog/) for a full demonstration of the generated documentation.

## Future Enhancements

- **API Documentation**: OpenAPI spec generation
- **Change Tracking**: Highlight recent changes
- **Quality Scores**: Automated quality assessment
- **Usage Analytics**: Track which metrics are most viewed
- **AI Summaries**: Auto-generate descriptions
- **Export Formats**: PDF, DOCX, Confluence