"""
Pre-compilation validator for Better-DBT-Metrics
Catches common issues before full compilation
"""

import yaml
from pathlib import Path
from typing import Dict, Any, List, Set, Optional, Tuple
import re

from core.error_handler import (
    ErrorCollector, CompilationError, ErrorFactory, 
    ErrorSeverity, ErrorCategory
)


class PreCompilationValidator:
    """Validates metric files before compilation to catch issues early"""
    
    def __init__(self, debug: bool = False):
        self.debug = debug
        self.error_collector = ErrorCollector()
        self.known_imports: Dict[str, Path] = {}
        self.known_metrics: Set[str] = set()
        self.known_dimensions: Set[str] = set()
        self.known_templates: Set[str] = set()
        
    def validate_directory(self, input_dir: str) -> Tuple[bool, ErrorCollector]:
        """Validate all files in a directory"""
        input_path = Path(input_dir)
        
        if not input_path.exists():
            self.error_collector.add_error(
                CompilationError(
                    message=f"Input directory not found: {input_path}",
                    category=ErrorCategory.CONFIGURATION,
                    severity=ErrorSeverity.ERROR,
                    suggestion="Check the path and ensure the directory exists"
                )
            )
            return False, self.error_collector
            
        # First pass: collect all available resources
        self._collect_resources(input_path)
        
        # Second pass: validate each file
        yaml_files = list(input_path.rglob("*.yml")) + list(input_path.rglob("*.yaml"))
        
        if not yaml_files:
            self.error_collector.add_error(
                CompilationError(
                    message=f"No YAML files found in {input_path}",
                    category=ErrorCategory.CONFIGURATION,
                    severity=ErrorSeverity.WARNING,
                    suggestion="Add .yml or .yaml files containing metric definitions"
                )
            )
            
        for yaml_file in yaml_files:
            if yaml_file.name.startswith('_'):
                continue  # Skip private files
            self._validate_file(yaml_file)
            
        return not self.error_collector.has_errors(), self.error_collector
        
    def _collect_resources(self, input_path: Path):
        """Collect all available resources for reference validation"""
        yaml_files = list(input_path.rglob("*.yml")) + list(input_path.rglob("*.yaml"))
        
        for yaml_file in yaml_files:
            try:
                with open(yaml_file, 'r') as f:
                    data = yaml.safe_load(f)
                    
                if not data:
                    continue
                    
                # Collect metrics
                if 'metrics' in data:
                    for metric in data['metrics']:
                        if isinstance(metric, dict) and 'name' in metric:
                            self.known_metrics.add(metric['name'])
                            
                # Collect dimension groups
                if 'dimension_groups' in data:
                    for group_name in data['dimension_groups']:
                        self.known_dimensions.add(group_name)
                        
                # Collect templates
                if 'metric_templates' in data:
                    for template_name in data['metric_templates']:
                        self.known_templates.add(template_name)
                        
                # Track imports
                if 'imports' in data:
                    for import_spec in data['imports']:
                        if isinstance(import_spec, str):
                            parts = import_spec.split(' as ')
                            if len(parts) == 2:
                                alias = parts[1].strip()
                                self.known_imports[alias] = yaml_file
                                
            except Exception:
                # Ignore files that can't be parsed in this phase
                pass
                
    def _validate_file(self, file_path: Path):
        """Validate a single YAML file"""
        if self.debug:
            print(f"[PRE-VALIDATION] Checking {file_path}")
            
        # Check file readability
        if not file_path.is_file():
            self.error_collector.add_error(
                CompilationError(
                    message=f"Cannot read file: {file_path}",
                    category=ErrorCategory.CONFIGURATION,
                    severity=ErrorSeverity.ERROR,
                    file_path=file_path
                )
            )
            return
            
        # Parse YAML
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                data = yaml.safe_load(content)
        except yaml.YAMLError as e:
            # Try to extract line number from error
            line_number = None
            if hasattr(e, 'problem_mark'):
                line_number = e.problem_mark.line + 1
                
            self.error_collector.add_error(
                ErrorFactory.yaml_syntax_error(
                    str(e), file_path, line_number
                )
            )
            return
        except Exception as e:
            self.error_collector.add_error(
                CompilationError(
                    message=f"Failed to read file: {str(e)}",
                    category=ErrorCategory.CONFIGURATION,
                    severity=ErrorSeverity.ERROR,
                    file_path=file_path
                )
            )
            return
            
        if not data:
            # Empty file
            self.error_collector.add_error(
                CompilationError(
                    message="Empty YAML file",
                    category=ErrorCategory.VALIDATION,
                    severity=ErrorSeverity.WARNING,
                    file_path=file_path,
                    suggestion="Add metric definitions or remove the empty file"
                )
            )
            return
            
        # Validate structure
        file_type = self._determine_file_type(data, file_path)
        self._validate_file_structure(data, file_path)
        
        # Only validate metric-specific content for metric and template files
        if file_type != 'config':
            # Validate imports
            if 'imports' in data:
                self._validate_imports(data['imports'], file_path)
                
            # Validate metrics (only for metric files)
            if 'metrics' in data:
                self._validate_metrics(data['metrics'], file_path)
                
            # Validate dimension groups
            if 'dimension_groups' in data:
                self._validate_dimension_groups(data['dimension_groups'], file_path)
                
            # Validate templates
            if 'metric_templates' in data or 'templates' in data:
                if 'metric_templates' in data:
                    self._validate_templates(data['metric_templates'], file_path)
                if 'templates' in data:
                    self._validate_templates(data['templates'], file_path)
            
    def _validate_file_structure(self, data: Dict[str, Any], file_path: Path):
        """Validate overall file structure"""
        # Determine file type and appropriate validation
        file_type = self._determine_file_type(data, file_path)
        
        if file_type == 'config':
            self._validate_config_file_structure(data, file_path)
        elif file_type == 'template':
            self._validate_template_file_structure(data, file_path)
        else:
            self._validate_metric_file_structure(data, file_path)
    
    def _determine_file_type(self, data: Dict[str, Any], file_path: Path) -> str:
        """Determine the type of YAML file based on content and name"""
        filename = file_path.name.lower()
        
        # Configuration files
        if filename in ['bdm_config.yml', 'bdm_config.yaml', 'config.yml', 'config.yaml']:
            return 'config'
        
        # Template-only files (contain only templates, dimension groups, no metrics)
        has_templates = 'metric_templates' in data or 'templates' in data
        has_dimension_groups = 'dimension_groups' in data
        has_metrics = 'metrics' in data
        
        if (has_templates or has_dimension_groups) and not has_metrics:
            return 'template'
            
        return 'metric'
    
    def _validate_config_file_structure(self, data: Dict[str, Any], file_path: Path):
        """Validate configuration file structure"""
        # Check version
        version = data.get('version')
        if version not in [None, 1, 2]:
            self.error_collector.add_error(
                CompilationError(
                    message=f"Unsupported version: {version}",
                    category=ErrorCategory.CONFIGURATION,
                    severity=ErrorSeverity.ERROR,
                    file_path=file_path,
                    suggestion="Use version 1 or 2, or omit the version field"
                )
            )
        
        # Config files have different valid keys
        config_keys = {
            'version', 'paths', 'imports', 'compilation', 'auto_variants',
            'output', 'validation', 'domains', 'logging', 'meta'
        }
        unknown_keys = set(data.keys()) - config_keys
        if unknown_keys:
            self.error_collector.add_error(
                CompilationError(
                    message=f"Unknown configuration keys: {', '.join(unknown_keys)}",
                    category=ErrorCategory.CONFIGURATION,
                    severity=ErrorSeverity.WARNING,
                    file_path=file_path,
                    suggestion=f"Valid configuration keys are: {', '.join(sorted(config_keys))}"
                )
            )
    
    def _validate_template_file_structure(self, data: Dict[str, Any], file_path: Path):
        """Validate template file structure"""
        # Template files can have more flexible structure
        template_keys = {
            'version', 'imports', 'dimension_groups', 'metric_templates', 
            'templates', 'meta', 'config', 'auto_variant_configurations',
            'metadata_standards', 'categories', 'seasonal_periods', 'common_filters'
        }
        unknown_keys = set(data.keys()) - template_keys
        if unknown_keys:
            self.error_collector.add_error(
                CompilationError(
                    message=f"Unknown template file keys: {', '.join(unknown_keys)}",
                    category=ErrorCategory.SYNTAX,
                    severity=ErrorSeverity.WARNING,
                    file_path=file_path,
                    suggestion=f"Valid template file keys are: {', '.join(sorted(template_keys))}"
                )
            )
    
    def _validate_metric_file_structure(self, data: Dict[str, Any], file_path: Path):
        """Validate metric definition file structure"""
        # Check version
        version = data.get('version')
        if version not in [None, 1, 2]:
            self.error_collector.add_error(
                CompilationError(
                    message=f"Unsupported version: {version}",
                    category=ErrorCategory.CONFIGURATION,
                    severity=ErrorSeverity.ERROR,
                    file_path=file_path,
                    suggestion="Use version 1 or 2, or omit the version field"
                )
            )
            
        # Check for unknown top-level keys in metric files
        valid_keys = {
            'version', 'imports', 'metrics', 'dimension_groups', 
            'metric_templates', 'meta', 'config'
        }
        unknown_keys = set(data.keys()) - valid_keys
        if unknown_keys:
            self.error_collector.add_error(
                CompilationError(
                    message=f"Unknown top-level keys: {', '.join(unknown_keys)}",
                    category=ErrorCategory.SYNTAX,
                    severity=ErrorSeverity.WARNING,
                    file_path=file_path,
                    suggestion=f"Valid top-level keys are: {', '.join(sorted(valid_keys))}"
                )
            )
            
    def _validate_imports(self, imports: List[Any], file_path: Path):
        """Validate import statements"""
        if not isinstance(imports, list):
            self.error_collector.add_error(
                CompilationError(
                    message="'imports' must be a list",
                    category=ErrorCategory.SYNTAX,
                    severity=ErrorSeverity.ERROR,
                    file_path=file_path
                )
            )
            return
            
        for i, import_spec in enumerate(imports):
            if not isinstance(import_spec, str):
                self.error_collector.add_error(
                    CompilationError(
                        message=f"Import #{i+1} must be a string",
                        category=ErrorCategory.SYNTAX,
                        severity=ErrorSeverity.ERROR,
                        file_path=file_path,
                        suggestion="Use format: '- path/to/file.yml as alias'"
                    )
                )
                continue
                
            # Validate import format
            if ' as ' not in import_spec:
                self.error_collector.add_error(
                    CompilationError(
                        message=f"Invalid import format: '{import_spec}'",
                        category=ErrorCategory.IMPORT,
                        severity=ErrorSeverity.ERROR,
                        file_path=file_path,
                        suggestion="Use format: 'path/to/file.yml as alias'"
                    )
                )
                
    def _validate_metrics(self, metrics: List[Any], file_path: Path):
        """Validate metric definitions"""
        if not isinstance(metrics, list):
            self.error_collector.add_error(
                CompilationError(
                    message="'metrics' must be a list",
                    category=ErrorCategory.SYNTAX,
                    severity=ErrorSeverity.ERROR,
                    file_path=file_path
                )
            )
            return
            
        seen_names = set()
        
        for i, metric in enumerate(metrics):
            if not isinstance(metric, dict):
                self.error_collector.add_error(
                    CompilationError(
                        message=f"Metric #{i+1} must be a dictionary",
                        category=ErrorCategory.SYNTAX,
                        severity=ErrorSeverity.ERROR,
                        file_path=file_path
                    )
                )
                continue
                
            # Validate metric structure
            self._validate_single_metric(metric, file_path, i)
            
            # Check for duplicate names
            name = metric.get('name')
            if name:
                if name in seen_names:
                    self.error_collector.add_error(
                        CompilationError(
                            message=f"Duplicate metric name: '{name}'",
                            category=ErrorCategory.VALIDATION,
                            severity=ErrorSeverity.ERROR,
                            file_path=file_path,
                            metric_name=name,
                            suggestion="Each metric must have a unique name"
                        )
                    )
                seen_names.add(name)
                
    def _validate_single_metric(self, metric: Dict[str, Any], file_path: Path, index: int):
        """Validate a single metric definition"""
        # Check required fields
        if 'name' not in metric:
            self.error_collector.add_error(
                CompilationError(
                    message=f"Metric #{index+1} is missing required field 'name'",
                    category=ErrorCategory.METRIC_DEFINITION,
                    severity=ErrorSeverity.ERROR,
                    file_path=file_path,
                    suggestion="Every metric must have a 'name' field"
                )
            )
            return
            
        name = metric['name']
        
        # Check if this is a templated metric
        template_fields = ['template', 'extends', '$use', '$ref']
        is_templated = any(field in metric for field in template_fields)
        
        # Get metric type - skip type validation for templated metrics
        metric_type = metric.get('type', 'simple' if not is_templated else None)
        
        # Validate metric type (skip for templated metrics)
        if metric_type and not is_templated:
            valid_types = {'simple', 'ratio', 'derived', 'cumulative', 'conversion'}
            if metric_type not in valid_types:
                self.error_collector.add_error(
                    ErrorFactory.invalid_metric_type(name, metric_type, file_path)
                )
                return
                
        # Skip type-specific validation for templated metrics
        if is_templated:
            return
            
        # Type-specific validation
        if metric_type == 'simple':
            self._validate_simple_metric(metric, name, file_path)
        elif metric_type == 'ratio':
            self._validate_ratio_metric(metric, name, file_path)
        elif metric_type == 'derived':
            self._validate_derived_metric(metric, name, file_path)
        elif metric_type == 'cumulative':
            self._validate_cumulative_metric(metric, name, file_path)
        elif metric_type == 'conversion':
            self._validate_conversion_metric(metric, name, file_path)
            
        # Validate dimensions
        if 'dimensions' in metric:
            self._validate_dimensions(metric['dimensions'], name, file_path)
            
        # Check for common issues
        self._check_metric_best_practices(metric, name, file_path)
        
    def _validate_simple_metric(self, metric: Dict[str, Any], name: str, file_path: Path):
        """Validate simple metric structure"""
        # Skip validation for templated metrics
        template_fields = ['template', 'extends', '$use', '$ref']
        if any(field in metric for field in template_fields):
            return
            
        # Check for source
        if 'source' not in metric:
            self.error_collector.add_error(
                ErrorFactory.missing_required_field('source', name, 'simple', file_path)
            )
            
        # Check for measure
        if 'measure' not in metric:
            self.error_collector.add_error(
                ErrorFactory.missing_required_field('measure', name, 'simple', file_path)
            )
        elif 'measure' in metric:
            measure = metric['measure']
            if isinstance(measure, dict):
                if 'type' not in measure:
                    self.error_collector.add_error(
                        CompilationError(
                            message=f"Measure for metric '{name}' is missing 'type'",
                            category=ErrorCategory.METRIC_DEFINITION,
                            severity=ErrorSeverity.ERROR,
                            file_path=file_path,
                            metric_name=name,
                            suggestion="Add measure.type (e.g., sum, count, average)"
                        )
                    )
                    
    def _validate_ratio_metric(self, metric: Dict[str, Any], name: str, file_path: Path):
        """Validate ratio metric structure"""
        # Check for numerator and denominator
        if 'numerator' not in metric:
            self.error_collector.add_error(
                ErrorFactory.missing_required_field('numerator', name, 'ratio', file_path)
            )
            
        if 'denominator' not in metric:
            self.error_collector.add_error(
                ErrorFactory.missing_required_field('denominator', name, 'ratio', file_path)
            )
            
    def _validate_derived_metric(self, metric: Dict[str, Any], name: str, file_path: Path):
        """Validate derived metric structure"""
        # Check for expression or formula
        if 'expression' not in metric and 'formula' not in metric:
            self.error_collector.add_error(
                ErrorFactory.missing_required_field('expression', name, 'derived', file_path)
            )
            
        # Check expression syntax
        expr = metric.get('expression', metric.get('formula', ''))
        if expr:
            # Look for metric references
            metric_refs = re.findall(r"metric\s*\(\s*['\"]([^'\"]+)['\"]\s*\)", expr)
            for ref in metric_refs:
                if ref not in self.known_metrics and ref != name:
                    self.error_collector.add_error(
                        CompilationError(
                            message=f"Reference to unknown metric '{ref}' in expression",
                            category=ErrorCategory.REFERENCE,
                            severity=ErrorSeverity.WARNING,
                            file_path=file_path,
                            metric_name=name,
                            suggestion="Ensure the referenced metric is defined"
                        )
                    )
                    
    def _validate_cumulative_metric(self, metric: Dict[str, Any], name: str, file_path: Path):
        """Validate cumulative metric structure"""
        # Check for required fields
        if 'source' not in metric:
            self.error_collector.add_error(
                ErrorFactory.missing_required_field('source', name, 'cumulative', file_path)
            )
            
        if 'measure' not in metric:
            self.error_collector.add_error(
                ErrorFactory.missing_required_field('measure', name, 'cumulative', file_path)
            )
            
        if 'window' not in metric:
            self.error_collector.add_error(
                ErrorFactory.missing_required_field('window', name, 'cumulative', file_path)
            )
            
    def _validate_conversion_metric(self, metric: Dict[str, Any], name: str, file_path: Path):
        """Validate conversion metric structure"""
        # Check for required fields
        if 'entity' not in metric:
            self.error_collector.add_error(
                ErrorFactory.missing_required_field('entity', name, 'conversion', file_path)
            )
            
        if 'calculation' not in metric:
            self.error_collector.add_error(
                ErrorFactory.missing_required_field('calculation', name, 'conversion', file_path)
            )
            
        if 'window' not in metric:
            self.error_collector.add_error(
                ErrorFactory.missing_required_field('window', name, 'conversion', file_path)
            )
            
    def _validate_dimensions(self, dimensions: List[Any], metric_name: str, file_path: Path):
        """Validate dimension list"""
        if not isinstance(dimensions, list):
            self.error_collector.add_error(
                CompilationError(
                    message=f"Dimensions for metric '{metric_name}' must be a list",
                    category=ErrorCategory.DIMENSION,
                    severity=ErrorSeverity.ERROR,
                    file_path=file_path,
                    metric_name=metric_name
                )
            )
            return
            
        for i, dim in enumerate(dimensions):
            # Valid formats: string, dict, or reference
            if isinstance(dim, str):
                # Simple string dimension
                pass
            elif isinstance(dim, dict):
                # Check for valid dimension dict
                if 'name' not in dim and '$ref' not in dim:
                    self.error_collector.add_error(
                        CompilationError(
                            message=f"Dimension #{i+1} in metric '{metric_name}' must have 'name' or '$ref'",
                            category=ErrorCategory.DIMENSION,
                            severity=ErrorSeverity.ERROR,
                            file_path=file_path,
                            metric_name=metric_name
                        )
                    )
            else:
                self.error_collector.add_error(
                    ErrorFactory.invalid_dimension_format(dim, metric_name, file_path)
                )
                
    def _validate_dimension_groups(self, groups: Dict[str, Any], file_path: Path):
        """Validate dimension group definitions"""
        if not isinstance(groups, dict):
            self.error_collector.add_error(
                CompilationError(
                    message="'dimension_groups' must be a dictionary",
                    category=ErrorCategory.SYNTAX,
                    severity=ErrorSeverity.ERROR,
                    file_path=file_path
                )
            )
            return
            
        for group_name, group_def in groups.items():
            if not isinstance(group_def, dict):
                self.error_collector.add_error(
                    CompilationError(
                        message=f"Dimension group '{group_name}' must be a dictionary",
                        category=ErrorCategory.DIMENSION,
                        severity=ErrorSeverity.ERROR,
                        file_path=file_path
                    )
                )
                continue
                
            # Check for dimensions list
            if 'dimensions' not in group_def and 'extends' not in group_def:
                self.error_collector.add_error(
                    CompilationError(
                        message=f"Dimension group '{group_name}' must have 'dimensions' or 'extends'",
                        category=ErrorCategory.DIMENSION,
                        severity=ErrorSeverity.ERROR,
                        file_path=file_path,
                        suggestion="Add a 'dimensions' list or 'extends' to inherit from another group"
                    )
                )
                
    def _validate_templates(self, templates: Dict[str, Any], file_path: Path):
        """Validate metric template definitions"""
        if not isinstance(templates, dict):
            self.error_collector.add_error(
                CompilationError(
                    message="'metric_templates' must be a dictionary",
                    category=ErrorCategory.SYNTAX,
                    severity=ErrorSeverity.ERROR,
                    file_path=file_path
                )
            )
            return
            
        for template_name, template_def in templates.items():
            if not isinstance(template_def, dict):
                self.error_collector.add_error(
                    CompilationError(
                        message=f"Template '{template_name}' must be a dictionary",
                        category=ErrorCategory.TEMPLATE,
                        severity=ErrorSeverity.ERROR,
                        file_path=file_path
                    )
                )
                continue
                
            # Check for required fields
            if 'template' not in template_def:
                self.error_collector.add_error(
                    CompilationError(
                        message=f"Template '{template_name}' is missing 'template' field",
                        category=ErrorCategory.TEMPLATE,
                        severity=ErrorSeverity.ERROR,
                        file_path=file_path,
                        suggestion="Add a 'template' field with the metric definition"
                    )
                )
                
            # Check parameters
            if 'parameters' in template_def:
                params = template_def['parameters']
                if not isinstance(params, list):
                    self.error_collector.add_error(
                        CompilationError(
                            message=f"Parameters for template '{template_name}' must be a list",
                            category=ErrorCategory.TEMPLATE,
                            severity=ErrorSeverity.ERROR,
                            file_path=file_path
                        )
                    )
                    
    def _check_metric_best_practices(self, metric: Dict[str, Any], name: str, file_path: Path):
        """Check for best practice violations"""
        # Check for description
        if 'description' not in metric:
            self.error_collector.add_error(
                ErrorFactory.best_practice_hint(
                    "Missing description",
                    f"Add a 'description' field to metric '{name}' to document its purpose",
                    file_path,
                    name
                )
            )
            
        # Check for label
        if 'label' not in metric:
            self.error_collector.add_error(
                ErrorFactory.best_practice_hint(
                    "Missing label",
                    f"Add a 'label' field to metric '{name}' for better display in BI tools",
                    file_path,
                    name
                )
            )
            
        # Check naming conventions
        if name:
            if not re.match(r'^[a-z][a-z0-9_]*$', name):
                self.error_collector.add_error(
                    CompilationError(
                        message=f"Metric name '{name}' doesn't follow naming conventions",
                        category=ErrorCategory.VALIDATION,
                        severity=ErrorSeverity.WARNING,
                        file_path=file_path,
                        metric_name=name,
                        suggestion="Use lowercase letters, numbers, and underscores (e.g., 'total_revenue')"
                    )
                )
                
        # Check for potential performance issues
        if metric.get('type') == 'derived':
            expr = metric.get('expression', metric.get('formula', ''))
            if expr.count('metric(') > 5:
                self.error_collector.add_error(
                    ErrorFactory.performance_warning(
                        name,
                        "Complex expression with many metric references",
                        file_path
                    )
                )