"""
Validation rules for Better-DBT-Metrics
"""

from typing import Dict, List, Any, Optional, Set
from pathlib import Path
from abc import ABC, abstractmethod
import re

from .types import ValidationResult, ValidationError


class ValidationRule(ABC):
    """Base class for validation rules"""
    
    @abstractmethod
    def validate(self, data: Dict[str, Any], file_path: Path, validator: Any) -> ValidationResult:
        """Validate the data and return results"""
        pass


class RequiredFieldsRule(ValidationRule):
    """Validates that required fields are present"""
    
    def validate(self, data: Dict[str, Any], file_path: Path, validator: Any) -> ValidationResult:
        result = ValidationResult()
        
        # Check metrics
        for metric in data.get('metrics', []):
            # Required fields for all metrics
            if not metric.get('name'):
                result.add_error(ValidationError(
                    file_path=str(file_path),
                    message="Metric missing required field: name"
                ))
                
            if not metric.get('type'):
                result.add_error(ValidationError(
                    file_path=str(file_path),
                    message=f"Metric '{metric.get('name', 'unknown')}' missing required field: type"
                ))
                
            # Type-specific requirements
            metric_type = metric.get('type', 'simple')
            
            if metric_type == 'simple':
                if not metric.get('source') and not metric.get('measure', {}).get('source'):
                    result.add_error(ValidationError(
                        file_path=str(file_path),
                        message=f"Simple metric '{metric.get('name')}' must have a source"
                    ))
                if not metric.get('measure'):
                    result.add_error(ValidationError(
                        file_path=str(file_path),
                        message=f"Simple metric '{metric.get('name')}' must have a measure"
                    ))
                    
            elif metric_type == 'ratio':
                if not metric.get('numerator'):
                    result.add_error(ValidationError(
                        file_path=str(file_path),
                        message=f"Ratio metric '{metric.get('name')}' must have a numerator"
                    ))
                if not metric.get('denominator'):
                    result.add_error(ValidationError(
                        file_path=str(file_path),
                        message=f"Ratio metric '{metric.get('name')}' must have a denominator"
                    ))
                    
            elif metric_type == 'derived':
                if not metric.get('expression') and not metric.get('formula'):
                    result.add_error(ValidationError(
                        file_path=str(file_path),
                        message=f"Derived metric '{metric.get('name')}' must have an expression or formula"
                    ))
                    
            elif metric_type == 'conversion':
                if not metric.get('base_measure'):
                    result.add_error(ValidationError(
                        file_path=str(file_path),
                        message=f"Conversion metric '{metric.get('name')}' must have a base_measure"
                    ))
                if not metric.get('conversion_measure'):
                    result.add_error(ValidationError(
                        file_path=str(file_path),
                        message=f"Conversion metric '{metric.get('name')}' must have a conversion_measure"
                    ))
                if not metric.get('entity'):
                    result.add_warning(ValidationError(
                        file_path=str(file_path),
                        message=f"Conversion metric '{metric.get('name')}' should specify an entity",
                        suggestion="Add entity: user_id or appropriate entity"
                    ))
                    
        return result


class ValidMetricTypeRule(ValidationRule):
    """Validates metric types"""
    
    VALID_TYPES = {'simple', 'ratio', 'derived', 'cumulative', 'conversion', 'time_comparison'}
    
    def validate(self, data: Dict[str, Any], file_path: Path, validator: Any) -> ValidationResult:
        result = ValidationResult()
        
        for metric in data.get('metrics', []):
            metric_type = metric.get('type')
            if metric_type and metric_type not in self.VALID_TYPES:
                result.add_error(ValidationError(
                    file_path=str(file_path),
                    message=f"Invalid metric type '{metric_type}' for metric '{metric.get('name')}'",
                    suggestion=f"Valid types are: {', '.join(sorted(self.VALID_TYPES))}"
                ))
                
        return result


class ValidDimensionTypeRule(ValidationRule):
    """Validates dimension types"""
    
    VALID_TYPES = {'time', 'categorical'}
    
    def validate(self, data: Dict[str, Any], file_path: Path, validator: Any) -> ValidationResult:
        result = ValidationResult()
        
        # Check dimensions in metrics
        for metric in data.get('metrics', []):
            for dim in metric.get('dimensions', []):
                if isinstance(dim, dict):
                    dim_type = dim.get('type')
                    if dim_type and dim_type not in self.VALID_TYPES:
                        result.add_error(ValidationError(
                            file_path=str(file_path),
                            message=f"Invalid dimension type '{dim_type}' in metric '{metric.get('name')}'",
                            suggestion=f"Valid types are: {', '.join(self.VALID_TYPES)}"
                        ))
                        
                    # Time dimension specific validation
                    if dim_type == 'time' and not dim.get('grain'):
                        result.add_warning(ValidationError(
                            file_path=str(file_path),
                            message=f"Time dimension '{dim.get('name')}' should specify a grain",
                            suggestion="Add grain: day, week, month, quarter, or year"
                        ))
                        
        return result


class ValidMeasureTypeRule(ValidationRule):
    """Validates measure types"""
    
    VALID_TYPES = {
        'sum', 'average', 'avg', 'count', 'count_distinct', 
        'min', 'max', 'median', 'percentile', 'stddev', 'variance',
        'sum_boolean', 'last_value', 'first_value', 'window'
    }
    
    def validate(self, data: Dict[str, Any], file_path: Path, validator: Any) -> ValidationResult:
        result = ValidationResult()
        
        for metric in data.get('metrics', []):
            measure = metric.get('measure', {})
            if isinstance(measure, dict):
                measure_type = measure.get('type')
                if measure_type and measure_type not in self.VALID_TYPES:
                    result.add_error(ValidationError(
                        file_path=str(file_path),
                        message=f"Invalid measure type '{measure_type}' in metric '{metric.get('name')}'",
                        suggestion=f"Valid types are: {', '.join(sorted(self.VALID_TYPES))}"
                    ))
                    
                # Percentile specific validation
                if measure_type == 'percentile' and 'percentile' not in measure and 'percentile_value' not in measure:
                    result.add_error(ValidationError(
                        file_path=str(file_path),
                        message=f"Percentile measure in metric '{metric.get('name')}' must specify percentile value",
                        suggestion="Add percentile: 0.5 (for median) or percentile_value: 0.95"
                    ))
                    
                # Window function specific validation
                if measure_type == 'window' and not measure.get('window_function'):
                    result.add_error(ValidationError(
                        file_path=str(file_path),
                        message=f"Window measure in metric '{metric.get('name')}' must specify window_function",
                        suggestion="Add window_function with SQL window expression"
                    ))
                    
        return result


class CircularDependencyRule(ValidationRule):
    """Detects circular dependencies in metrics"""
    
    def validate(self, data: Dict[str, Any], file_path: Path, validator: Any) -> ValidationResult:
        result = ValidationResult()
        
        # Build dependency graph
        dependencies = {}
        all_metrics = validator.get_all_metrics()
        
        for metric in all_metrics:
            name = metric.get('name')
            if not name:
                continue
                
            deps = set()
            
            # Check derived metrics
            if metric.get('type') == 'derived':
                expr = metric.get('expression', metric.get('formula', ''))
                # Extract metric references
                refs = re.findall(r"metric\s*\(\s*['\"]([^'\"]+)['\"]\s*\)", expr)
                deps.update(refs)
                
            # Check filter references
            if 'filter' in metric:
                refs = re.findall(r"metric\s*\(\s*['\"]([^'\"]+)['\"]\s*\)", metric['filter'])
                deps.update(refs)
                
            dependencies[name] = deps
            
        # Check for cycles
        def has_cycle(node: str, visited: Set[str], rec_stack: Set[str]) -> bool:
            visited.add(node)
            rec_stack.add(node)
            
            for dep in dependencies.get(node, set()):
                if dep not in visited:
                    if has_cycle(dep, visited, rec_stack):
                        return True
                elif dep in rec_stack:
                    return True
                    
            rec_stack.remove(node)
            return False
            
        visited = set()
        for metric_name in dependencies:
            if metric_name not in visited:
                if has_cycle(metric_name, visited, set()):
                    result.add_error(ValidationError(
                        file_path=str(file_path),
                        message=f"Circular dependency detected involving metric '{metric_name}'",
                        suggestion="Review metric dependencies and remove circular references"
                    ))
                    
        return result


class ReferenceResolutionRule(ValidationRule):
    """Validates that all references can be resolved"""
    
    def validate(self, data: Dict[str, Any], file_path: Path, validator: Any) -> ValidationResult:
        result = ValidationResult()
        
        # Get all available references
        imports = validator.parser.imports_cache
        dimension_groups = validator.get_all_dimension_groups()
        templates = validator.get_all_templates()
        
        # Check dimension references
        for metric in data.get('metrics', []):
            for dim in metric.get('dimensions', []):
                if isinstance(dim, dict) and '$ref' in dim:
                    ref = dim['$ref']
                    if not self._can_resolve_ref(ref, imports, dimension_groups):
                        result.add_error(ValidationError(
                            file_path=str(file_path),
                            message=f"Cannot resolve dimension reference '{ref}' in metric '{metric.get('name')}'",
                            suggestion="Check that the reference exists and imports are correct"
                        ))
                        
            # Check template references
            if 'template' in metric:
                template_ref = metric['template']
                if not self._can_resolve_template(template_ref, imports, templates):
                    result.add_error(ValidationError(
                        file_path=str(file_path),
                        message=f"Cannot resolve template reference '{template_ref}' in metric '{metric.get('name')}'",
                        suggestion="Check that the template exists and imports are correct"
                    ))
                    
        return result
        
    def _can_resolve_ref(self, ref: str, imports: Dict, groups: Dict) -> bool:
        """Check if a reference can be resolved"""
        parts = ref.split('.')
        
        # Check imports
        if parts[0] in imports:
            # Would need to traverse the import to check
            return True
            
        # Check local groups
        if ref in groups:
            return True
            
        return False
        
    def _can_resolve_template(self, ref: str, imports: Dict, templates: Dict) -> bool:
        """Check if a template reference can be resolved"""
        parts = ref.split('.')
        
        # Check imports
        if parts[0] in imports:
            return True
            
        # Check local templates
        if ref in templates:
            return True
            
        return False


class TemplateParameterRule(ValidationRule):
    """Validates template parameters"""
    
    def validate(self, data: Dict[str, Any], file_path: Path, validator: Any) -> ValidationResult:
        result = ValidationResult()
        
        # Check template definitions
        for name, template in data.get('metric_templates', {}).items():
            params = template.get('parameters', [])
            required_params = {p['name'] for p in params if p.get('required', False)}
            
            # Check that template body uses parameters
            template_str = str(template.get('template', {}))
            for param in params:
                param_name = param['name']
                if f"{{{{{param_name}}}}}" not in template_str and f"$({param_name})" not in template_str:
                    result.add_warning(ValidationError(
                        file_path=str(file_path),
                        message=f"Template parameter '{param_name}' is defined but not used in template '{name}'",
                        suggestion="Remove unused parameter or use it in the template"
                    ))
                    
        # Check template usage
        for metric in data.get('metrics', []):
            if 'template' in metric:
                provided_params = set(metric.get('parameters', {}).keys())
                provided_params.update(metric.get('params', {}).keys())
                
                # Would need to resolve template to check required params
                # For now, just check that some params are provided if template is used
                if not provided_params and 'parameters' not in metric and 'params' not in metric:
                    result.add_warning(ValidationError(
                        file_path=str(file_path),
                        message=f"Metric '{metric.get('name')}' uses template but provides no parameters",
                        suggestion="Add parameters or params field with template values"
                    ))
                    
        return result


class EntityRelationshipRule(ValidationRule):
    """Validates entity relationships"""
    
    def validate(self, data: Dict[str, Any], file_path: Path, validator: Any) -> ValidationResult:
        result = ValidationResult()
        
        entities = {e['name']: e for e in data.get('entities', [])}
        
        # Check entity relationships
        for entity in data.get('entities', []):
            for rel in entity.get('relationships', []):
                to_entity = rel.get('to_entity')
                if to_entity and to_entity not in entities:
                    # Check if it might be defined in another file
                    all_entities = set()
                    for cached_data in validator.parsed_cache.values():
                        all_entities.update(e['name'] for e in cached_data.get('entities', []))
                        
                    if to_entity not in all_entities:
                        result.add_error(ValidationError(
                            file_path=str(file_path),
                            message=f"Entity '{entity['name']}' has relationship to unknown entity '{to_entity}'",
                            suggestion="Define the entity or check the entity name"
                        ))
                        
                # Validate relationship type
                rel_type = rel.get('type')
                valid_types = {'one_to_one', 'one_to_many', 'many_to_one', 'many_to_many'}
                if rel_type and rel_type not in valid_types:
                    result.add_error(ValidationError(
                        file_path=str(file_path),
                        message=f"Invalid relationship type '{rel_type}' in entity '{entity['name']}'",
                        suggestion=f"Valid types are: {', '.join(valid_types)}"
                    ))
                    
        return result


class TimeSpineValidationRule(ValidationRule):
    """Validates time spine configurations"""
    
    def validate(self, data: Dict[str, Any], file_path: Path, validator: Any) -> ValidationResult:
        result = ValidationResult()
        
        # Check time spine definitions
        for name, spine in data.get('time_spine', {}).items():
            if not spine.get('model'):
                result.add_error(ValidationError(
                    file_path=str(file_path),
                    message=f"Time spine '{name}' must have a model reference",
                    suggestion="Add model: ref('your_date_dimension_table')"
                ))
                
            if not spine.get('columns'):
                result.add_error(ValidationError(
                    file_path=str(file_path),
                    message=f"Time spine '{name}' must define columns",
                    suggestion="Add columns mapping grain names to column names"
                ))
                
        # Check time spine usage in metrics
        defined_spines = set(data.get('time_spine', {}).keys())
        all_spines = defined_spines.copy()
        
        # Add spines from other files
        for cached_data in validator.parsed_cache.values():
            all_spines.update(cached_data.get('time_spine', {}).keys())
            
        for metric in data.get('metrics', []):
            spine_ref = metric.get('time_spine')
            if spine_ref and isinstance(spine_ref, str) and spine_ref not in all_spines:
                result.add_error(ValidationError(
                    file_path=str(file_path),
                    message=f"Metric '{metric.get('name')}' references unknown time spine '{spine_ref}'",
                    suggestion="Define the time spine or check the spine name"
                ))
                
        return result


class MetricFilterReferencesRule(ValidationRule):
    """Validates metric references in filters"""
    
    def validate(self, data: Dict[str, Any], file_path: Path, validator: Any) -> ValidationResult:
        result = ValidationResult()
        
        # Get all metric names
        all_metrics = {m.get('name') for m in validator.get_all_metrics() if m.get('name')}
        
        # Check filter references
        for metric in data.get('metrics', []):
            if 'filter' in metric:
                # Extract metric references
                refs = re.findall(r"metric\s*\(\s*['\"]([^'\"]+)['\"]\s*\)", metric['filter'])
                
                for ref in refs:
                    if ref not in all_metrics:
                        result.add_error(ValidationError(
                            file_path=str(file_path),
                            message=f"Metric '{metric.get('name')}' filter references unknown metric '{ref}'",
                            suggestion="Check that the referenced metric exists"
                        ))
                        
                    # Check for self-reference
                    if ref == metric.get('name'):
                        result.add_error(ValidationError(
                            file_path=str(file_path),
                            message=f"Metric '{metric.get('name')}' cannot reference itself in filter",
                            suggestion="Remove self-reference or use a different metric"
                        ))
                        
        return result


class UniqueNamesRule(ValidationRule):
    """Validates uniqueness of names within a file"""
    
    def validate(self, data: Dict[str, Any], file_path: Path, validator: Any) -> ValidationResult:
        result = ValidationResult()
        
        # Check metric names
        metric_names = [m.get('name') for m in data.get('metrics', []) if m.get('name')]
        duplicates = self._find_duplicates(metric_names)
        
        for name in duplicates:
            result.add_error(ValidationError(
                file_path=str(file_path),
                message=f"Duplicate metric name '{name}' in file",
                suggestion="Use unique names for all metrics"
            ))
            
        # Check entity names
        entity_names = [e.get('name') for e in data.get('entities', []) if e.get('name')]
        duplicates = self._find_duplicates(entity_names)
        
        for name in duplicates:
            result.add_error(ValidationError(
                file_path=str(file_path),
                message=f"Duplicate entity name '{name}' in file",
                suggestion="Use unique names for all entities"
            ))
            
        # Check dimension group names
        group_names = list(data.get('dimension_groups', {}).keys())
        duplicates = self._find_duplicates(group_names)
        
        for name in duplicates:
            result.add_error(ValidationError(
                file_path=str(file_path),
                message=f"Duplicate dimension group name '{name}' in file",
                suggestion="Use unique names for all dimension groups"
            ))
            
        return result
        
    def _find_duplicates(self, items: List[str]) -> Set[str]:
        """Find duplicate items in a list"""
        seen = set()
        duplicates = set()
        
        for item in items:
            if item in seen:
                duplicates.add(item)
            seen.add(item)
            
        return duplicates


# Convenience classes for specific validation contexts
class OffsetWindowValidationRule(ValidationRule):
    """Validates offset window configurations"""
    
    def validate(self, data: Dict[str, Any], file_path: Path, validator: Any) -> ValidationResult:
        result = ValidationResult()
        
        # Check metrics with offsets
        for metric in data.get('metrics', []):
            if metric.get('type') != 'cumulative':
                # Only cumulative metrics can have offsets
                if 'offsets' in metric:
                    result.add_error(ValidationError(
                        file_path=str(file_path),
                        message=f"Non-cumulative metric '{metric.get('name')}' cannot have offsets",
                        suggestion="Only cumulative metrics support offset windows"
                    ))
                continue
                
            # Validate offsets
            for i, offset in enumerate(metric.get('offsets', [])):
                # Required fields
                if 'period' not in offset:
                    result.add_error(ValidationError(
                        file_path=str(file_path),
                        message=f"Offset {i} in metric '{metric.get('name')}' missing required field: period",
                        suggestion="Add period: day, week, month, quarter, or year"
                    ))
                    
                if 'offset' not in offset:
                    result.add_error(ValidationError(
                        file_path=str(file_path),
                        message=f"Offset {i} in metric '{metric.get('name')}' missing required field: offset",
                        suggestion="Add offset: -1 for previous period"
                    ))
                elif not isinstance(offset['offset'], int):
                    result.add_error(ValidationError(
                        file_path=str(file_path),
                        message=f"Offset value must be an integer in metric '{metric.get('name')}'",
                        suggestion="Use an integer value like -1, -7, -30"
                    ))
                    
                # Validate period
                valid_periods = {'day', 'week', 'month', 'quarter', 'year', 'fiscal_year', 'fiscal_quarter'}
                if 'period' in offset and offset['period'] not in valid_periods:
                    result.add_error(ValidationError(
                        file_path=str(file_path),
                        message=f"Invalid offset period '{offset['period']}' in metric '{metric.get('name')}'",
                        suggestion=f"Valid periods are: {', '.join(valid_periods)}"
                    ))
                    
                # Validate calculations
                if 'calculations' in offset:
                    valid_calculations = {'difference', 'percent_change', 'absolute_change'}
                    for calc in offset['calculations']:
                        calc_type = calc.get('type') if isinstance(calc, dict) else calc
                        if calc_type not in valid_calculations:
                            result.add_error(ValidationError(
                                file_path=str(file_path),
                                message=f"Invalid calculation type '{calc_type}' in metric '{metric.get('name')}'",
                                suggestion=f"Valid types are: {', '.join(valid_calculations)}"
                            ))
                            
        # Check offset patterns
        if 'offset_window_config' in data and 'offset_patterns' in data['offset_window_config']:
            for pattern_name, pattern_offsets in data['offset_window_config']['offset_patterns'].items():
                if not isinstance(pattern_offsets, list):
                    result.add_error(ValidationError(
                        file_path=str(file_path),
                        message=f"Offset pattern '{pattern_name}' must be a list of offset configurations",
                        suggestion="Define pattern as a list of offset objects"
                    ))
                    
        return result


class MetricValidationRule(ValidationRule):
    """Base class for metric-specific validation rules"""
    pass


class DimensionValidationRule(ValidationRule):
    """Base class for dimension-specific validation rules"""
    pass