"""Tests for the template system"""

import pytest
import yaml
from pathlib import Path
import tempfile
import shutil

from features.templates import TemplateEngine, TemplateLibrary, TemplateParameter, MetricTemplate


class TestTemplateEngine:
    """Test the TemplateEngine functionality"""
    
    def setup_method(self):
        """Setup test fixtures"""
        self.engine = TemplateEngine()
        
    def test_register_template(self):
        """Test registering a template"""
        template_def = {
            'description': 'Test template',
            'parameters': [
                {'name': 'SOURCE', 'required': True},
                {'name': 'COLUMN', 'default': 'amount'}
            ],
            'template': {
                'source': '{{ SOURCE }}',
                'measure': {
                    'type': 'sum',
                    'column': '{{ COLUMN }}'
                }
            }
        }
        
        self.engine.register_template('test_template', template_def)
        
        assert 'test_template' in self.engine.templates
        template = self.engine.templates['test_template']
        assert isinstance(template, MetricTemplate)
        assert len(template.parameters) == 2
        
    def test_expand_template(self):
        """Test expanding a template with parameters"""
        template_def = {
            'description': 'Revenue template',
            'parameters': [
                {'name': 'TABLE', 'required': True},
                {'name': 'AMOUNT_COLUMN', 'default': 'amount'},
                {'name': 'STATUS_FILTER', 'default': "status = 'completed'"}
            ],
            'template': {
                'source': '{{ TABLE }}',
                'measure': {
                    'type': 'sum',
                    'column': '{{ AMOUNT_COLUMN }}'
                },
                'filters': ['{{ STATUS_FILTER }}']
            }
        }
        
        self.engine.register_template('revenue', template_def)
        
        # Expand with all parameters
        result = self.engine.expand_template(
            'revenue',
            {'TABLE': 'fct_orders', 'AMOUNT_COLUMN': 'order_total'}
        )
        
        assert result['source'] == 'fct_orders'
        assert result['measure']['column'] == 'order_total'
        assert result['filters'][0] == "status = 'completed'"  # Default used
        
    def test_template_parameter_validation(self):
        """Test parameter validation"""
        template_def = {
            'description': 'Test template',
            'parameters': [
                {'name': 'REQUIRED_PARAM', 'required': True},
                {'name': 'OPTIONAL_PARAM', 'required': False}
            ],
            'template': {
                'value': '{{ REQUIRED_PARAM }}'
            }
        }
        
        self.engine.register_template('test', template_def)
        
        # Should fail without required parameter
        with pytest.raises(ValueError) as exc_info:
            self.engine.expand_template('test', {})
        assert 'required' in str(exc_info.value).lower()
        
        # Should succeed with required parameter
        result = self.engine.expand_template(
            'test',
            {'REQUIRED_PARAM': 'value'}
        )
        assert result['value'] == 'value'
        
    def test_nested_template_expansion(self):
        """Test expanding nested structures"""
        template_def = {
            'description': 'Nested template',
            'parameters': [
                {'name': 'DIM_NAME', 'required': True},
                {'name': 'DIM_TYPE', 'default': 'categorical'}
            ],
            'template': {
                'dimensions': [
                    {
                        'name': '{{ DIM_NAME }}',
                        'type': '{{ DIM_TYPE }}',
                        'expr': 'table.{{ DIM_NAME }}'
                    }
                ]
            }
        }
        
        self.engine.register_template('dimension', template_def)
        
        result = self.engine.expand_template(
            'dimension',
            {'DIM_NAME': 'customer_segment'}
        )
        
        assert result['dimensions'][0]['name'] == 'customer_segment'
        assert result['dimensions'][0]['type'] == 'categorical'
        assert result['dimensions'][0]['expr'] == 'table.customer_segment'
        

class TestTemplateLibrary:
    """Test the TemplateLibrary functionality"""
    
    def setup_method(self):
        """Setup test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.library = TemplateLibrary([self.temp_dir])
        
    def teardown_method(self):
        """Cleanup temp files"""
        shutil.rmtree(self.temp_dir)
        
    def test_load_templates_from_file(self):
        """Test loading templates from YAML files"""
        template_content = """
metric_templates:
  revenue_base:
    description: "Base revenue metric template"
    parameters:
      - name: SOURCE_TABLE
        type: string
        required: true
      - name: AMOUNT_COLUMN
        type: string
        default: "amount"
    template:
      source: "{{ SOURCE_TABLE }}"
      measure:
        type: sum
        column: "{{ AMOUNT_COLUMN }}"
        
  conversion_base:
    description: "Base conversion metric template"
    parameters:
      - name: EVENT_TABLE
        required: true
    template:
      type: conversion
      source: "{{ EVENT_TABLE }}"
"""
        
        template_file = Path(self.temp_dir) / "templates.yml"
        with open(template_file, 'w') as f:
            f.write(template_content)
            
        self.library.load_templates()
        
        # Check templates were loaded
        templates = self.library.list_templates()
        assert 'revenue_base' in templates
        assert 'conversion_base' in templates
        
    def test_expand_template(self):
        """Test expanding a template from the library"""
        template_content = """
metric_templates:
  test_metric:
    description: "Test metric"
    parameters:
      - name: TABLE
        required: true
    template:
      source: "{{ TABLE }}"
      measure:
        type: count
"""
        
        template_file = Path(self.temp_dir) / "test.yml"
        with open(template_file, 'w') as f:
            f.write(template_content)
            
        self.library.load_templates()
        
        result = self.library.expand('test_metric', {'TABLE': 'fct_events'})
        
        assert result['source'] == 'fct_events'
        assert result['measure']['type'] == 'count'
        
    def test_get_template_info(self):
        """Test getting template information"""
        template_content = """
metric_templates:
  info_test:
    description: "Template with info"
    parameters:
      - name: PARAM1
        type: string
        required: true
        description: "First parameter"
      - name: PARAM2
        type: number
        default: 100
        description: "Second parameter"
    template:
      value: "test"
"""
        
        template_file = Path(self.temp_dir) / "info.yml"
        with open(template_file, 'w') as f:
            f.write(template_content)
            
        self.library.load_templates()
        
        info = self.library.get_template_info('info_test')
        
        assert info['description'] == "Template with info"
        assert len(info['parameters']) == 2
        assert info['parameters'][0]['name'] == 'PARAM1'
        assert info['parameters'][0]['required'] == True
        assert info['parameters'][1]['default'] == 100