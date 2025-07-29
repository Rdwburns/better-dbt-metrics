"""
Main validator for Better-DBT-Metrics
"""

from typing import Dict, List, Any, Optional, Set, Tuple
from pathlib import Path
import yaml

from core.parser import BetterDBTParser
from .types import ValidationError, ValidationResult
from .rules import (
    RequiredFieldsRule,
    ValidMetricTypeRule,
    ValidDimensionTypeRule,
    ValidMeasureTypeRule,
    CircularDependencyRule,
    ReferenceResolutionRule,
    TemplateParameterRule,
    EntityRelationshipRule,
    TimeSpineValidationRule,
    MetricFilterReferencesRule,
    UniqueNamesRule,
    OffsetWindowValidationRule,
    ModelReferenceRule
)


class MetricsValidator:
    """
    Main validator that orchestrates all validation rules
    """
    
    def __init__(self, base_dir: str = "."):
        self.base_dir = Path(base_dir)
        self.parser = BetterDBTParser(base_dir)
        
        # Initialize validation rules
        self.rules = [
            RequiredFieldsRule(),
            ValidMetricTypeRule(),
            ValidDimensionTypeRule(),
            ValidMeasureTypeRule(),
            CircularDependencyRule(),
            ReferenceResolutionRule(),
            TemplateParameterRule(),
            EntityRelationshipRule(),
            TimeSpineValidationRule(),
            MetricFilterReferencesRule(),
            UniqueNamesRule(),
            OffsetWindowValidationRule(),
            ModelReferenceRule()
        ]
        
        # Cache for parsed data
        self.parsed_cache: Dict[str, Any] = {}
        
    def validate_file(self, file_path: Path) -> ValidationResult:
        """Validate a single metrics file"""
        result = ValidationResult()
        
        try:
            # Parse the file
            parsed_data = self.parser.parse_file(file_path)
            self.parsed_cache[str(file_path)] = parsed_data
            
            # Add basic info
            result.info.append(f"Validating {file_path}")
            
            # Check version
            if parsed_data.get('version') not in [1, 2]:
                result.add_error(ValidationError(
                    file_path=str(file_path),
                    message=f"Unsupported version: {parsed_data.get('version')}",
                    suggestion="Use version: 2"
                ))
                
            # Run all validation rules
            for rule in self.rules:
                rule_result = rule.validate(parsed_data, file_path, self)
                result.merge(rule_result)
                
        except Exception as e:
            result.add_error(ValidationError(
                file_path=str(file_path),
                message=f"Failed to parse file: {str(e)}",
                suggestion="Check YAML syntax and file permissions"
            ))
            
        return result
        
    def validate_directory(self, directory: str, fix: bool = False) -> ValidationResult:
        """Validate all metrics files in a directory"""
        result = ValidationResult()
        dir_path = Path(directory)
        
        if not dir_path.exists():
            result.add_error(ValidationError(
                message=f"Directory not found: {directory}"
            ))
            return result
            
        # Find all YAML files
        yaml_files = list(dir_path.rglob("*.yml")) + list(dir_path.rglob("*.yaml"))
        
        if not yaml_files:
            result.add_warning(ValidationError(
                message=f"No YAML files found in {directory}"
            ))
            return result
            
        result.info.append(f"Found {len(yaml_files)} YAML files to validate")
        
        # Validate each file
        for file_path in yaml_files:
            # Skip files starting with underscore
            if file_path.name.startswith('_'):
                continue
                
            file_result = self.validate_file(file_path)
            result.merge(file_result)
            
        return result
        
    def get_all_metrics(self) -> List[Dict[str, Any]]:
        """Get all metrics from parsed files"""
        all_metrics = []
        for data in self.parsed_cache.values():
            all_metrics.extend(data.get('metrics', []))
        return all_metrics
        
    def get_all_dimension_groups(self) -> Dict[str, Any]:
        """Get all dimension groups from parsed files"""
        all_groups = {}
        for data in self.parsed_cache.values():
            groups = data.get('dimension_groups', {})
            all_groups.update(groups)
        return all_groups
        
    def get_all_templates(self) -> Dict[str, Any]:
        """Get all templates from parsed files"""
        all_templates = {}
        for data in self.parsed_cache.values():
            templates = data.get('metric_templates', {})
            all_templates.update(templates)
        return all_templates