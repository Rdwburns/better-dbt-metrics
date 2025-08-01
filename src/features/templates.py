"""
Template System for Better-DBT-Metrics
Handles metric templates, parameter validation, and template expansion
"""

import yaml
from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass, field
from jinja2 import Template, Environment, meta
import re


@dataclass
class TemplateParameter:
    """Represents a template parameter"""
    name: str
    type: str = "string"
    required: bool = False
    default: Optional[Any] = None
    description: Optional[str] = None
    enum: Optional[List[Any]] = None
    

@dataclass 
class MetricTemplate:
    """Represents a reusable metric template"""
    name: str
    description: str
    parameters: List[TemplateParameter]
    template: Dict[str, Any]
    abstract: bool = False  # If true, can't be used directly
    

class TemplateEngine:
    """
    Manages metric templates and their expansion
    Features:
    - Parameter validation
    - Jinja2 template expansion
    - Template inheritance
    - Default value handling
    """
    
    def __init__(self):
        self.templates: Dict[str, MetricTemplate] = {}
        self.jinja_env = Environment()
        
    def register_template(self, name: str, template_def: Dict[str, Any]):
        """Register a new metric template"""
        parameters = []
        
        # Parse parameters
        for param_def in template_def.get('parameters', []):
            if isinstance(param_def, str):
                # Simple parameter name
                param = TemplateParameter(name=param_def)
            elif isinstance(param_def, dict):
                # Full parameter definition
                param = TemplateParameter(
                    name=param_def['name'],
                    type=param_def.get('type', 'string'),
                    required=param_def.get('required', False),
                    default=param_def.get('default'),
                    description=param_def.get('description'),
                    enum=param_def.get('enum')
                )
            else:
                raise ValueError(f"Invalid parameter definition: {param_def}")
                
            parameters.append(param)
            
        # Create template
        template = MetricTemplate(
            name=name,
            description=template_def.get('description', ''),
            parameters=parameters,
            template=template_def.get('template', {}),
            abstract=template_def.get('abstract', False)
        )
        
        self.templates[name] = template
        
    def expand_template(self, template_name: str, params: Dict[str, Any], 
                       context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Expand a template with given parameters"""
        if template_name not in self.templates:
            raise ValueError(f"Template '{template_name}' not found")
            
        template = self.templates[template_name]
        
        if template.abstract:
            raise ValueError(f"Cannot use abstract template '{template_name}' directly")
            
        # Validate and prepare parameters
        final_params = self._prepare_parameters(template, params)
        
        # Add context if provided
        if context:
            final_params.update(context)
            
        # Expand template
        return self._expand_template_dict(template.template, final_params)
        
    def _prepare_parameters(self, template: MetricTemplate, 
                          provided_params: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and prepare parameters for template expansion"""
        final_params = {}
        
        # Check all parameters
        for param in template.parameters:
            if param.name in provided_params:
                value = provided_params[param.name]
                
                # Validate type
                if not self._validate_param_type(value, param.type):
                    raise ValueError(
                        f"Parameter '{param.name}' must be of type {param.type}, "
                        f"got {type(value).__name__}"
                    )
                    
                # Validate enum
                if param.enum and value not in param.enum:
                    raise ValueError(
                        f"Parameter '{param.name}' must be one of {param.enum}, "
                        f"got '{value}'"
                    )
                    
                final_params[param.name] = value
                
            elif param.required:
                raise ValueError(f"Required parameter '{param.name}' not provided")
                
            elif param.default is not None:
                final_params[param.name] = param.default
                
        # Warn about extra parameters
        extra_params = set(provided_params.keys()) - {p.name for p in template.parameters}
        if extra_params:
            print(f"Warning: Unknown parameters will be ignored: {extra_params}")
            
        return final_params
        
    def _validate_param_type(self, value: Any, param_type: str) -> bool:
        """Validate parameter type"""
        type_map = {
            'string': str,
            'number': (int, float),
            'boolean': bool,
            'array': list,
            'list': list,  # Support both 'array' and 'list'
            'object': dict,
            'dict': dict   # Support both 'object' and 'dict'
        }
        
        if param_type in type_map:
            return isinstance(value, type_map[param_type])
        return True  # Unknown types pass validation
        
    def _expand_template_dict(self, template_dict: Dict[str, Any], 
                            params: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively expand a template dictionary"""
        from copy import deepcopy
        
        # Deep copy to avoid modifying original
        result = deepcopy(template_dict)
        
        # Process each value recursively without JSON conversion
        self._process_dict_values(result, params)
        
        return result
    
    def _process_dict_values(self, obj: Dict[str, Any], params: Dict[str, Any]):
        """Recursively process dictionary values with Jinja2 templates"""
        for key, value in obj.items():
            if isinstance(value, str):
                # Check if it contains Jinja2 template syntax
                if '{{' in value or '{%' in value:
                    obj[key] = self._expand_string_template(value, params)
            elif isinstance(value, dict):
                self._process_dict_values(value, params)
            elif isinstance(value, list):
                self._process_list_values(value, params)
    
    def _process_list_values(self, obj: List[Any], params: Dict[str, Any]):
        """Recursively process list values with Jinja2 templates"""
        for i, item in enumerate(obj):
            if isinstance(item, str):
                if '{{' in item or '{%' in item:
                    obj[i] = self._expand_string_template(item, params)
            elif isinstance(item, dict):
                self._process_dict_values(item, params)
            elif isinstance(item, list):
                self._process_list_values(item, params)
    
    def _expand_string_template(self, template_str: str, params: Dict[str, Any]) -> Any:
        """Expand a single template string"""
        # Add custom filters for safe SQL handling
        self.jinja_env.filters['sql_quote'] = lambda x: f"'{x}'" if isinstance(x, str) else str(x)
        self.jinja_env.filters['safe_default'] = lambda x, d: x if x else d
        
        jinja_template = self.jinja_env.from_string(template_str)
        result = jinja_template.render(**params)
        
        # Try to preserve the original type for pure template expressions
        if template_str.strip().startswith('{{') and template_str.strip().endswith('}}'):
            # This was a pure template expression, try to evaluate it
            try:
                import ast
                # Only evaluate simple literals for safety
                return ast.literal_eval(result)
            except:
                # Return as string if evaluation fails
                pass
        
        return result
        

class TemplateLibrary:
    """
    Manages a library of templates loaded from files
    """
    
    def __init__(self, template_dirs: List[str]):
        self.template_dirs = template_dirs
        self.engine = TemplateEngine()
        self.semantic_model_engine = TemplateEngine()  # Separate engine for semantic model templates
        self._loaded = False
        
    def load_templates(self):
        """Load all templates from configured directories"""
        if self._loaded:
            return
            
        for template_dir in self.template_dirs:
            self._load_directory(template_dir)
            
        self._loaded = True
        
    def _load_directory(self, directory: str):
        """Load templates from a directory"""
        from pathlib import Path
        
        dir_path = Path(directory)
        if not dir_path.exists():
            return
            
        # Load all YAML files
        for yaml_file in dir_path.rglob("*.yml"):
            with open(yaml_file, 'r') as f:
                data = yaml.safe_load(f)
                
            # Register metric templates
            if 'metric_templates' in data:
                for name, template_def in data['metric_templates'].items():
                    self.engine.register_template(name, template_def)
                    
            # Register semantic model templates
            if 'semantic_model_templates' in data:
                for name, template_def in data['semantic_model_templates'].items():
                    self.semantic_model_engine.register_template(name, template_def)
                    
    def expand(self, template_name: str, params: Dict[str, Any], template_type: str = 'metric') -> Dict[str, Any]:
        """Expand a template from the library"""
        self.load_templates()  # Ensure loaded
        if template_type == 'semantic_model':
            return self.semantic_model_engine.expand_template(template_name, params)
        return self.engine.expand_template(template_name, params)
        
    def list_templates(self, template_type: str = 'metric') -> List[str]:
        """List all available templates"""
        self.load_templates()
        if template_type == 'semantic_model':
            return list(self.semantic_model_engine.templates.keys())
        return list(self.engine.templates.keys())
        
    def get_template_info(self, template_name: str, template_type: str = 'metric') -> Dict[str, Any]:
        """Get information about a template"""
        self.load_templates()
        
        engine = self.semantic_model_engine if template_type == 'semantic_model' else self.engine
        
        if template_name not in engine.templates:
            raise ValueError(f"Template '{template_name}' not found")
            
        template = engine.templates[template_name]
        
        return {
            'name': template.name,
            'description': template.description,
            'abstract': template.abstract,
            'parameters': [
                {
                    'name': p.name,
                    'type': p.type,
                    'required': p.required,
                    'default': p.default,
                    'description': p.description,
                    'enum': p.enum
                }
                for p in template.parameters
            ]
        }


def create_standard_templates() -> Dict[str, Dict[str, Any]]:
    """Create standard metric templates that ship with the package"""
    return {
        'metric_templates': {
            # Revenue template
            'revenue_base': {
                'description': 'Standard revenue metric template',
                'parameters': [
                    {
                        'name': 'SOURCE_TABLE',
                        'type': 'string',
                        'required': True,
                        'description': 'The fact table containing revenue data'
                    },
                    {
                        'name': 'AMOUNT_COLUMN',
                        'type': 'string',
                        'default': 'amount',
                        'description': 'The column containing the revenue amount'
                    },
                    {
                        'name': 'STATUS_FILTER',
                        'type': 'string',
                        'default': "status = 'completed'",
                        'description': 'SQL filter for valid revenue'
                    }
                ],
                'template': {
                    'source': '{{ SOURCE_TABLE }}',
                    'measure': {
                        'type': 'sum',
                        'column': '{{ AMOUNT_COLUMN }}',
                        'filters': ['{{ STATUS_FILTER }}']
                    }
                }
            },
            
            # Conversion rate template
            'conversion_rate': {
                'description': 'Standard conversion rate metric template',
                'parameters': [
                    {
                        'name': 'NUMERATOR_TABLE',
                        'type': 'string',
                        'required': True
                    },
                    {
                        'name': 'DENOMINATOR_TABLE',
                        'type': 'string',
                        'required': True
                    },
                    {
                        'name': 'JOIN_KEY',
                        'type': 'string',
                        'default': 'session_id'
                    }
                ],
                'template': {
                    'type': 'ratio',
                    'numerator': {
                        'source': '{{ NUMERATOR_TABLE }}',
                        'measure': {
                            'type': 'count_distinct',
                            'column': '{{ JOIN_KEY }}'
                        }
                    },
                    'denominator': {
                        'source': '{{ DENOMINATOR_TABLE }}',
                        'measure': {
                            'type': 'count',
                            'column': '{{ JOIN_KEY }}'
                        }
                    }
                }
            },
            
            # User activity template
            'user_activity': {
                'description': 'Standard user activity metric template',
                'parameters': [
                    {
                        'name': 'EVENT_TABLE',
                        'type': 'string',
                        'required': True
                    },
                    {
                        'name': 'USER_COLUMN',
                        'type': 'string',
                        'default': 'user_id'
                    },
                    {
                        'name': 'ACTIVITY_TYPE',
                        'type': 'string',
                        'enum': ['dau', 'wau', 'mau'],
                        'default': 'dau'
                    }
                ],
                'template': {
                    'source': '{{ EVENT_TABLE }}',
                    'measure': {
                        'type': 'count_distinct',
                        'column': '{{ USER_COLUMN }}'
                    },
                    'dimensions': [
                        {
                            'name': 'activity_date',
                            'type': 'time',
                            'grain': "{{ 'day' if ACTIVITY_TYPE == 'dau' else 'week' if ACTIVITY_TYPE == 'wau' else 'month' }}"
                        }
                    ]
                }
            }
        }
    }