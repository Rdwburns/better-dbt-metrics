"""
Enhanced Template System with Better YAML Support
Fixes formatting issues by avoiding JSON conversion
"""

import yaml
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from jinja2 import Environment, meta
from copy import deepcopy


class YAMLSafeTemplateEngine:
    """
    Template engine that preserves YAML syntax without JSON conversion
    """
    
    def __init__(self):
        self.templates: Dict[str, Dict[str, Any]] = {}
        self.jinja_env = Environment()
        
        # Add custom filters for safe string handling
        self.jinja_env.filters['quote_sql'] = self._quote_sql
        self.jinja_env.filters['escape_yaml'] = self._escape_yaml
        
    @staticmethod
    def _quote_sql(value: str) -> str:
        """Safely quote SQL strings"""
        # If the value contains single quotes, use double quotes
        if "'" in value:
            return f'"{value}"'
        # Otherwise use single quotes
        return f"'{value}'"
    
    @staticmethod
    def _escape_yaml(value: str) -> str:
        """Escape strings for YAML"""
        # Use YAML's own escaping
        return yaml.dump(value, default_style='"').strip()
    
    def expand_template(self, template_dict: Dict[str, Any], 
                       params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Expand template using a YAML-safe approach
        """
        # Deep copy to avoid modifying original
        result = deepcopy(template_dict)
        
        # Process each value recursively
        self._process_dict(result, params)
        
        return result
    
    def _process_dict(self, obj: Dict[str, Any], params: Dict[str, Any]):
        """Recursively process dictionary values"""
        for key, value in obj.items():
            if isinstance(value, str):
                # Check if it contains Jinja2 template syntax
                if '{{' in value or '{%' in value:
                    obj[key] = self._expand_string(value, params)
            elif isinstance(value, dict):
                self._process_dict(value, params)
            elif isinstance(value, list):
                self._process_list(value, params)
    
    def _process_list(self, obj: List[Any], params: Dict[str, Any]):
        """Recursively process list values"""
        for i, item in enumerate(obj):
            if isinstance(item, str):
                if '{{' in item or '{%' in item:
                    obj[i] = self._expand_string(item, params)
            elif isinstance(item, dict):
                self._process_dict(item, params)
            elif isinstance(item, list):
                self._process_list(item, params)
    
    def _expand_string(self, template_str: str, params: Dict[str, Any]) -> Any:
        """Expand a single template string"""
        jinja_template = self.jinja_env.from_string(template_str)
        result = jinja_template.render(**params)
        
        # Try to preserve the original type
        # If the entire string was a template, try to parse it
        if template_str.strip().startswith('{{') and template_str.strip().endswith('}}'):
            # This was a pure template expression
            try:
                # Try to evaluate as Python literal
                import ast
                return ast.literal_eval(result)
            except:
                # Return as string
                return result
        
        return result


# Alternative: Direct YAML template processing
def process_yaml_template(template_path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a YAML template file directly without JSON conversion
    """
    from jinja2 import FileSystemLoader, Environment
    import os
    
    # Set up Jinja2 environment for YAML files
    template_dir = os.path.dirname(template_path)
    template_name = os.path.basename(template_path)
    
    env = Environment(
        loader=FileSystemLoader(template_dir),
        # Preserve newlines and spacing
        keep_trailing_newline=True,
        trim_blocks=False,
        lstrip_blocks=False
    )
    
    # Add safe filters
    env.filters['sql_value'] = lambda x: f"'{x}'" if isinstance(x, str) else str(x)
    env.filters['yaml_safe'] = lambda x: yaml.dump(x).strip()
    
    # Load and render template
    template = env.get_template(template_name)
    rendered = template.render(**params)
    
    # Parse the rendered YAML
    return yaml.safe_load(rendered)


# Example usage in templates:
"""
# Instead of complex escaping:
filter: '{{ base_filter | default(''date >= "2020-01-01"'') }}'

# Use custom filters:
filter: {{ base_filter | default('date >= "2020-01-01"') | quote_sql }}

# Or use YAML block scalars:
filter: |
  {{ base_filter | default('date >= "2020-01-01"') }}

# Or use a simpler approach:
filter: {{ base_filter or 'date >= "2020-01-01"' }}
"""