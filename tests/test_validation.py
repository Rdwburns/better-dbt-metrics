"""
Tests for the validation framework
"""

import pytest
from pathlib import Path
import tempfile
import shutil

from src.validation.validator import MetricsValidator, ValidationResult, ValidationError
from src.validation.rules import (
    RequiredFieldsRule,
    ValidMetricTypeRule,
    CircularDependencyRule,
    UniqueNamesRule
)


class TestValidationFramework:
    """Test the validation framework"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp()
        self.metrics_dir = Path(self.test_dir) / "metrics"
        self.metrics_dir.mkdir(parents=True)
        
        # Create mock models directory to satisfy model validation
        self.models_dir = Path(self.test_dir) / "models"
        self.models_dir.mkdir(parents=True)
        
        # Create a mock dbt_project.yml
        dbt_project = Path(self.test_dir) / "dbt_project.yml"
        dbt_project.write_text("""
name: test_project
version: '1.0.0'
model-paths: ["models"]
""")
        
        # Create mock model files
        (self.models_dir / "fct_orders.sql").write_text("SELECT * FROM raw.orders")
        (self.models_dir / "fct_sales.sql").write_text("SELECT * FROM raw.sales")
        
    def teardown_method(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.test_dir)
        
    def test_valid_metrics_file(self):
        """Test validation of a valid metrics file"""
        metrics_file = self.metrics_dir / "valid.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: revenue
    description: "Total revenue"
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: order_total
    dimensions:
      - name: order_date
        type: time
        grain: day
""")
        
        validator = MetricsValidator(str(self.test_dir))
        result = validator.validate_file(metrics_file)
        
        assert result.is_valid
        assert len(result.errors) == 0
        assert len(result.warnings) == 0
        
    def test_missing_required_fields(self):
        """Test detection of missing required fields"""
        metrics_file = self.metrics_dir / "missing_fields.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - description: "Metric without name"
    type: simple
    source: fct_orders
    
  - name: "ratio_without_parts"
    type: ratio
    # Missing numerator and denominator
""")
        
        validator = MetricsValidator(str(self.test_dir))
        result = validator.validate_file(metrics_file)
        
        assert not result.is_valid
        assert len(result.errors) >= 3  # Missing name, numerator, denominator
        
        # Check specific errors
        error_messages = [e.message for e in result.errors]
        assert any("missing required field: name" in msg for msg in error_messages)
        assert any("must have a numerator" in msg for msg in error_messages)
        assert any("must have a denominator" in msg for msg in error_messages)
        
    def test_invalid_metric_type(self):
        """Test detection of invalid metric types"""
        metrics_file = self.metrics_dir / "invalid_type.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: bad_type
    type: complex  # Invalid type
    source: fct_orders
""")
        
        validator = MetricsValidator(str(self.test_dir))
        result = validator.validate_file(metrics_file)
        
        assert not result.is_valid
        assert any("Invalid metric type 'complex'" in e.message for e in result.errors)
        
    def test_circular_dependency_detection(self):
        """Test detection of circular dependencies"""
        metrics_file = self.metrics_dir / "circular.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: metric_a
    type: derived
    expression: "metric('metric_b') + 1"
    
  - name: metric_b
    type: derived
    expression: "metric('metric_a') * 2"
""")
        
        validator = MetricsValidator(str(self.test_dir))
        result = validator.validate_file(metrics_file)
        
        assert not result.is_valid
        assert any("Circular dependency" in e.message for e in result.errors)
        
    def test_duplicate_names(self):
        """Test detection of duplicate metric names"""
        metrics_file = self.metrics_dir / "duplicates.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: revenue
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: amount
      
  - name: revenue  # Duplicate!
    type: simple
    source: fct_sales
    measure:
      type: sum
      column: total
""")
        
        validator = MetricsValidator(str(self.test_dir))
        result = validator.validate_file(metrics_file)
        
        assert not result.is_valid
        assert any("Duplicate metric name 'revenue'" in e.message for e in result.errors)
        
    def test_invalid_dimension_type(self):
        """Test detection of invalid dimension types"""
        metrics_file = self.metrics_dir / "bad_dimension.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: test_metric
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: amount
    dimensions:
      - name: bad_dim
        type: spatial  # Invalid dimension type
""")
        
        validator = MetricsValidator(str(self.test_dir))
        result = validator.validate_file(metrics_file)
        
        assert not result.is_valid
        assert any("Invalid dimension type 'spatial'" in e.message for e in result.errors)
        
    def test_time_dimension_warnings(self):
        """Test warnings for time dimensions without grain"""
        metrics_file = self.metrics_dir / "time_no_grain.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: test_metric
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: amount
    dimensions:
      - name: order_date
        type: time
        # No grain specified
""")
        
        validator = MetricsValidator(str(self.test_dir))
        result = validator.validate_file(metrics_file)
        
        assert result.is_valid  # Warning, not error
        assert len(result.warnings) > 0
        assert any("should specify a grain" in w.message for w in result.warnings)
        
    def test_metric_filter_references(self):
        """Test validation of metric references in filters"""
        metrics_file = self.metrics_dir / "filter_refs.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: avg_order
    type: simple
    source: fct_orders
    measure:
      type: average
      column: order_total
      
  - name: high_value_orders
    type: simple
    source: fct_orders
    measure:
      type: count
      column: order_id
    filter: "order_total > metric('avg_order')"  # Valid reference
    
  - name: bad_reference
    type: simple
    source: fct_orders
    measure:
      type: count
      column: order_id
    filter: "order_total > metric('does_not_exist')"  # Invalid reference
""")
        
        validator = MetricsValidator(str(self.test_dir))
        result = validator.validate_file(metrics_file)
        
        assert not result.is_valid
        assert any("references unknown metric 'does_not_exist'" in e.message for e in result.errors)
        
    def test_entity_relationship_validation(self):
        """Test validation of entity relationships"""
        metrics_file = self.metrics_dir / "entities.yml"
        metrics_file.write_text("""
version: 2

entities:
  - name: order
    type: primary
    column: order_id
    relationships:
      - type: many_to_one
        to_entity: customer  # Not defined
        foreign_key: customer_id
        
      - type: invalid_type  # Invalid relationship type
        to_entity: product
        foreign_key: product_id
""")
        
        validator = MetricsValidator(str(self.test_dir))
        result = validator.validate_file(metrics_file)
        
        assert not result.is_valid
        assert any("unknown entity 'customer'" in e.message for e in result.errors)
        assert any("Invalid relationship type 'invalid_type'" in e.message for e in result.errors)
        
    def test_cross_file_validation(self):
        """Test validation across multiple files"""
        # File 1
        file1 = self.metrics_dir / "file1.yml"
        file1.write_text("""
version: 2

metrics:
  - name: revenue
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: amount
""")
        
        # File 2 with duplicate metric
        file2 = self.metrics_dir / "file2.yml"
        file2.write_text("""
version: 2

metrics:
  - name: revenue  # Duplicate across files
    type: simple
    source: fct_sales
    measure:
      type: sum
      column: total
""")
        
        validator = MetricsValidator(str(self.test_dir))
        result = validator.validate_directory(self.metrics_dir)
        
        assert not result.is_valid
        assert any("defined in multiple files" in e.message for e in result.errors)
        
    def test_yaml_error_handling(self):
        """Test handling of YAML syntax errors"""
        metrics_file = self.metrics_dir / "bad_yaml.yml"
        metrics_file.write_text("""
version: 2

metrics:
  - name: test
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: [invalid yaml syntax
""")
        
        validator = MetricsValidator(str(self.test_dir))
        result = validator.validate_file(metrics_file)
        
        assert not result.is_valid
        assert any("Invalid YAML" in e.message for e in result.errors)
        
    def test_validation_result_string_output(self):
        """Test ValidationResult string representation"""
        result = ValidationResult()
        
        # Add errors
        result.add_error(ValidationError(
            file_path="test.yml",
            line_number=10,
            message="Test error",
            suggestion="Fix it"
        ))
        
        # Add warnings
        result.add_warning(ValidationError(
            file_path="test.yml",
            message="Test warning"
        ))
        
        # Add info
        result.info.append("Processing test.yml")
        
        output = str(result)
        assert "Found 1 error(s)" in output
        assert "Found 1 warning(s)" in output
        assert "Test error" in output
        assert "Fix it" in output
        assert "Test warning" in output