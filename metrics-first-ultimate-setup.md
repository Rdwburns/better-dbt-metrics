# Metrics-First-Ultimate Setup Guide

## Initial Setup

### 1. Initialize Git Repository

```bash
cd /Users/roryarmitage-burns/Documents/GitHub/metrics-first-ultimate
git init
git add .
git commit -m "Initial commit: Metrics-First-DBT Ultimate"
```

### 2. Create Python Virtual Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate it
source venv/bin/activate  # On Mac/Linux
# or
venv\Scripts\activate  # On Windows

# Install in development mode
pip install -e .
```

### 3. Test the CLI

```bash
# Check installation
metrics-first --version

# List available commands
metrics-first --help

# Test compilation with example
metrics-first compile --input-dir examples/simple --output-dir output/test
```

### 4. Set Up GitHub Repository

1. Create a new repository on GitHub
2. Add remote and push:
   ```bash
   git remote add origin https://github.com/yourusername/metrics-first-ultimate.git
   git branch -M main
   git push -u origin main
   ```

### 5. Enable GitHub Actions

The GitHub Action is already configured in `.github/workflows/compile-metrics.yml`. It will automatically:
- Compile metrics when you push changes to `metrics/` or `templates/`
- Validate the compiled output
- Comment on PRs with compilation results

### 6. Create Your First Metric

Create `metrics/my_first_metric.yml`:

```yaml
version: 2

metrics:
  - name: total_revenue
    description: "Total revenue from orders"
    source: fct_orders
    measure:
      type: sum
      column: amount
    dimensions:
      - name: order_date
        type: time
        grain: day
      - name: customer_segment
        type: categorical
```

### 7. Test Local Compilation

```bash
# Compile your metric
metrics-first compile --input-dir metrics --output-dir models/semantic

# Check the output
cat models/semantic/_metrics.yml
```

## Project Structure

```
metrics-first-ultimate/
├── src/                    # Source code
│   ├── core/              # Core parsing and compilation
│   ├── features/          # Templates, dimension groups
│   ├── cli/               # Command-line interface
│   └── integrations/      # Future: BI tools, etc.
├── templates/             # Reusable templates
│   ├── dimensions/        # Dimension groups
│   └── metrics/           # Metric templates
├── examples/              # Example metrics
├── .github/               # GitHub Actions
└── metrics/               # Your metrics go here (create this)
```

## Quick Start Workflow

1. **Define dimension groups** in `templates/dimensions/`
2. **Create metric templates** in `templates/metrics/`
3. **Write metrics** in `metrics/` using templates
4. **Push to GitHub** - Action compiles automatically
5. **Use compiled models** in your dbt project

## Integration with dbt Project

In your dbt project:

1. Add to `packages.yml`:
   ```yaml
   packages:
     - git: "https://github.com/yourusername/metrics-first-ultimate.git"
       revision: main
   ```

2. The compiled models will be in `models/semantic/`

3. Run dbt commands as normal:
   ```bash
   dbt deps
   dbt run
   dbt test
   ```

## Advanced Features

### Using Templates

```yaml
# metrics/revenue_with_template.yml
version: 2

imports:
  - ../templates/metrics/revenue.yml as rev_templates

metrics:
  - name: product_revenue
    template: rev_templates.revenue_base
    params:
      SOURCE_TABLE: fct_product_sales
      AMOUNT_COLUMN: sale_amount
```

### Using Dimension Groups

```yaml
# metrics/with_dimension_groups.yml
version: 2

imports:
  - ../templates/dimensions/temporal.yml as time
  - ../templates/dimensions/customer.yml as customer

metrics:
  - name: customer_metrics
    source: fct_orders
    measure:
      type: count
    dimensions:
      - $ref: time.daily
      - $ref: customer.segment
```

### Auto-Generating Variants

```yaml
metrics:
  - name: revenue
    # ... base metric definition ...
    auto_variants:
      time_comparison: [wow, mom, yoy]
      by_dimension: [region, product_category]
```

This creates:
- `revenue_wow`, `revenue_mom`, `revenue_yoy`
- `revenue_by_region`, `revenue_by_product_category`

## Troubleshooting

### Import Errors
- Check relative paths in imports
- Ensure imported files exist
- Look for circular dependencies

### Template Parameter Errors
- Check required parameters are provided
- Verify parameter types match
- Use `metrics-first list-templates` to see available templates

### Compilation Errors
- Run with `--verbose` for detailed output
- Check YAML syntax with online validators
- Ensure source tables exist in your dbt project

## Next Steps

1. **Customize templates** for your organization
2. **Build dimension libraries** for common analyses  
3. **Set up monitoring** for metric health
4. **Create documentation** for your metrics
5. **Share with team** for collaboration

---

Need help? Check the [CLAUDE.md](CLAUDE.md) file for detailed development notes and architecture decisions.