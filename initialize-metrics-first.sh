#!/bin/bash
# Quick initialization script for metrics-first-ultimate

echo "🚀 Initializing Metrics-First-Ultimate..."

# Check if we're in the right directory
if [ ! -f "setup.py" ] || [ ! -d "src" ]; then
    echo "❌ Error: This script must be run from the metrics-first-ultimate directory"
    echo "   Please cd to /Users/roryarmitage-burns/Documents/GitHub/metrics-first-ultimate"
    exit 1
fi

# Create metrics directory structure
echo "📁 Creating directory structure..."
mkdir -p metrics/{finance,product,marketing}
mkdir -p models/semantic
mkdir -p docs/metrics

# Create a sample metric
echo "📝 Creating sample metric..."
cat > metrics/sample_revenue.yml << 'EOF'
# Sample revenue metric to get started
version: 2

# Import standard dimensions
imports:
  - ../templates/dimensions/temporal.yml as time
  - ../templates/dimensions/customer.yml as customer

metrics:
  - name: total_revenue
    description: "Total revenue from completed orders"
    source: fct_orders
    measure:
      type: sum
      column: order_total
      filters:
        - "order_status = 'completed'"
    dimensions:
      - $ref: time.daily
      - $ref: customer.segment
      - name: sales_channel
        type: categorical
    # Auto-generate useful variants
    auto_variants:
      time_comparison: [mom, yoy]
      by_dimension: [sales_channel]
    meta:
      owner: "analytics_team"
      tier: "gold"
EOF

# Create a gitignore
echo "📄 Creating .gitignore..."
cat > .gitignore << 'EOF'
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
venv/
env/
*.egg-info/
dist/
build/

# IDE
.vscode/
.idea/
*.swp
*.swo
.DS_Store

# Compiled output (can be regenerated)
models/semantic/
output/

# Logs
*.log
logs/

# Environment
.env
.env.local

# Testing
.coverage
.pytest_cache/
htmlcov/

# Documentation build
docs/_build/
EOF

# Create initial metrics config
echo "⚙️  Creating metrics configuration..."
cat > metrics/config.yml << 'EOF'
# Global configuration for metrics
version: 2

config:
  # Default settings for all metrics
  defaults:
    meta:
      team: "data_team"
      slack_channel: "#data-alerts"
  
  # Validation rules
  validation:
    require_description: true
    require_owner: true
    min_description_length: 20
  
  # Auto-variant settings
  auto_variants:
    enabled: true
    default_time_comparisons: [wow, mom, yoy]
  
  # Environment-specific settings
  environments:
    dev:
      sample_data: true
      validation_mode: "warning"
    prod:
      sample_data: false
      validation_mode: "error"
      require_tests: true
EOF

# Create a simple test script
echo "🧪 Creating test script..."
cat > test_compilation.sh << 'EOF'
#!/bin/bash
# Test metrics compilation

echo "Testing metrics compilation..."

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Run compilation
metrics-first compile \
    --input-dir metrics \
    --output-dir output/test \
    --verbose

# Check if output was created
if [ -d "output/test" ] && [ -n "$(ls -A output/test)" ]; then
    echo "✅ Compilation successful!"
    echo "📁 Output files:"
    ls -la output/test/
else
    echo "❌ Compilation failed or no output generated"
    exit 1
fi
EOF

chmod +x test_compilation.sh

# Create README for metrics directory
echo "📚 Creating metrics README..."
cat > metrics/README.md << 'EOF'
# Metrics Directory

This directory contains all metrics-first definitions for our analytics.

## Structure

```
metrics/
├── finance/        # Financial metrics (revenue, costs, etc.)
├── product/        # Product metrics (usage, adoption, etc.)
├── marketing/      # Marketing metrics (CAC, conversions, etc.)
├── config.yml      # Global configuration
└── README.md       # This file
```

## Adding New Metrics

1. Choose the appropriate subdirectory
2. Create a new `.yml` file
3. Follow the metrics-first syntax
4. Push to trigger automatic compilation

## Examples

See `sample_revenue.yml` for a complete example using:
- Imports and dimension groups
- Measure definitions with filters
- Auto-variant generation
- Metadata and ownership

## Best Practices

1. Use dimension groups from templates for consistency
2. Always include descriptions and ownership
3. Use auto-variants instead of manual duplication
4. Group related metrics in the same file
5. Use meaningful metric names (e.g., `customer_lifetime_value` not `clv`)
EOF

echo "✅ Initialization complete!"
echo ""
echo "Next steps:"
echo "1. cd /Users/roryarmitage-burns/Documents/GitHub/metrics-first-ultimate"
echo "2. python3 -m venv venv"
echo "3. source venv/bin/activate"
echo "4. pip install -e ."
echo "5. ./test_compilation.sh"
echo ""
echo "📖 See metrics-first-ultimate-setup.md for detailed instructions"