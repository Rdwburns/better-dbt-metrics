"""Tests specifically for ratio metric template expansion fix"""

import pytest
import yaml
from pathlib import Path
import tempfile
import shutil

from core.compiler import BetterDBTCompiler, CompilerConfig


class TestRatioTemplateExpansion:
    """Test that ratio metrics work correctly with template expansion"""
    
    def setup_method(self):
        """Setup test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.output_dir = Path(self.temp_dir) / "output"
        self.output_dir.mkdir()
        
    def teardown_method(self):
        """Cleanup temp files"""
        shutil.rmtree(self.temp_dir)
        
    def create_test_file(self, filename, content):
        """Helper to create test files"""
        file_path = Path(self.temp_dir) / filename
        with open(file_path, 'w') as f:
            f.write(content)
        return file_path
        
    def test_ratio_template_with_different_sources(self):
        """Test ratio metric template expansion with different sources"""
        content = """
version: 1

metric_templates:
  ratio_base:
    description: 'Ratio metric template'
    parameters:
      - name: NUMERATOR_TABLE
        required: true
      - name: DENOMINATOR_TABLE
        required: true
      - name: VALUE_COLUMN
        default: amount
    template:
      type: ratio
      numerator:
        source: "{{ NUMERATOR_TABLE }}"
        measure:
          type: sum
          column: "{{ VALUE_COLUMN }}"
      denominator:
        source: "{{ DENOMINATOR_TABLE }}"
        measure:
          type: sum
          column: "{{ VALUE_COLUMN }}"

metrics:
  - name: refund_rate
    template: ratio_base
    parameters:
      NUMERATOR_TABLE: fct_refunds
      DENOMINATOR_TABLE: fct_orders
      VALUE_COLUMN: total_amount
    dimensions:
      - name: date
        type: time
        grain: day
"""
        self.create_test_file("ratio_template.yml", content)
        
        config = CompilerConfig(
            input_dir=self.temp_dir,
            output_dir=str(self.output_dir),
            split_files=False,
            validate=False
        )
        compiler = BetterDBTCompiler(config)
        results = compiler.compile_directory()
        
        # Should compile successfully
        assert results['errors'] == []
        assert results['metrics_compiled'] == 1
        
        # Check output
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            output = yaml.safe_load(f)
            
        # Verify metric was compiled correctly
        metric = output['metrics'][0]
        assert metric['name'] == 'refund_rate'
        assert metric['type'] == 'ratio'
        
        # Check that numerator and denominator were preserved
        assert 'type_params' in metric
        assert 'numerator' in metric['type_params']
        assert 'denominator' in metric['type_params']
        
    def test_ratio_template_with_same_source(self):
        """Test ratio metric template expansion with same source"""
        content = """
version: 1

metric_templates:
  conversion_rate_template:
    description: 'Conversion rate template'
    parameters:
      - name: SOURCE_TABLE
        required: true
      - name: CONVERTED_FILTER
        default: "is_converted = true"
      - name: ID_COLUMN
        default: session_id
    template:
      type: ratio
      numerator:
        source: "{{ SOURCE_TABLE }}"
        measure:
          type: count_distinct
          column: "{{ ID_COLUMN }}"
          filters:
            - "{{ CONVERTED_FILTER }}"
      denominator:
        source: "{{ SOURCE_TABLE }}"
        measure:
          type: count_distinct
          column: "{{ ID_COLUMN }}"

metrics:
  - name: checkout_conversion_rate
    template: conversion_rate_template
    parameters:
      SOURCE_TABLE: fct_sessions
      CONVERTED_FILTER: "has_checkout = true"
      ID_COLUMN: session_id
    dimensions:
      - name: channel
        type: categorical
"""
        self.create_test_file("conversion_template.yml", content)
        
        config = CompilerConfig(
            input_dir=self.temp_dir,
            output_dir=str(self.output_dir),
            split_files=False,
            validate=False
        )
        compiler = BetterDBTCompiler(config)
        results = compiler.compile_directory()
        
        # Should compile successfully
        assert results['errors'] == []
        assert results['metrics_compiled'] == 1
        
        # Check that the source was correctly auto-detected
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            output = yaml.safe_load(f)
            
        # Should have one semantic model for fct_sessions
        assert len(output['semantic_models']) == 1
        assert output['semantic_models'][0]['name'] == 'sem_fct_sessions'
        
    def test_ratio_template_with_override(self):
        """Test that metric-level fields override template fields"""
        content = """
version: 1

metric_templates:
  generic_ratio:
    description: 'Generic ratio template'
    parameters:
      - name: NUM_TABLE
        required: true
      - name: DEN_TABLE
        required: true
    template:
      type: ratio
      label: "Generic Ratio"
      numerator:
        source: "{{ NUM_TABLE }}"
        measure:
          type: count
          column: id
      denominator:
        source: "{{ DEN_TABLE }}"
        measure:
          type: count
          column: id

metrics:
  - name: custom_ratio
    template: generic_ratio
    parameters:
      NUM_TABLE: fct_success
      DEN_TABLE: fct_attempts
    # Override template values
    label: "Success Rate"
    description: "Percentage of successful attempts"
    numerator:
      source: fct_success
      measure:
        type: sum  # Override count with sum
        column: value
"""
        self.create_test_file("override_template.yml", content)
        
        config = CompilerConfig(
            input_dir=self.temp_dir,
            output_dir=str(self.output_dir),
            split_files=False,
            validate=False
        )
        compiler = BetterDBTCompiler(config)
        results = compiler.compile_directory()
        
        # Should compile successfully
        assert results['errors'] == []
        assert results['metrics_compiled'] == 1
        
        # Check output
        output_file = self.output_dir / "compiled_semantic_models.yml"
        with open(output_file, 'r') as f:
            output = yaml.safe_load(f)
            
        metric = output['metrics'][0]
        assert metric['label'] == 'Success Rate'  # Override worked
        assert metric['description'] == 'Percentage of successful attempts'