"""
Enhanced Error Handling for Better-DBT-Metrics
Provides clear, actionable error messages with context
"""

from typing import Dict, Any, List, Optional, Union
from pathlib import Path
import yaml
import json
from dataclasses import dataclass
from enum import Enum


class ErrorSeverity(Enum):
    """Error severity levels"""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class ErrorCategory(Enum):
    """Error categories for better organization"""
    SYNTAX = "syntax"
    REFERENCE = "reference"
    VALIDATION = "validation"
    TEMPLATE = "template"
    IMPORT = "import"
    DIMENSION = "dimension"
    METRIC_DEFINITION = "metric_definition"
    OUTPUT = "output"
    CONFIGURATION = "configuration"


@dataclass
class CompilationError:
    """Structured error information"""
    message: str
    category: ErrorCategory
    severity: ErrorSeverity
    file_path: Optional[str] = None
    line_number: Optional[int] = None
    column_number: Optional[int] = None
    metric_name: Optional[str] = None
    context: Optional[Dict[str, Any]] = None
    suggestion: Optional[str] = None
    related_errors: List['CompilationError'] = None
    
    def __post_init__(self):
        if self.related_errors is None:
            self.related_errors = []
            
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'message': self.message,
            'category': self.category.value,
            'severity': self.severity.value,
            'file_path': str(self.file_path) if self.file_path else None,
            'line_number': self.line_number,
            'column_number': self.column_number,
            'metric_name': self.metric_name,
            'context': self.context,
            'suggestion': self.suggestion,
            'related_errors': [e.to_dict() for e in self.related_errors]
        }
        
    def format_terminal(self, verbose: bool = False) -> str:
        """Format error for terminal output"""
        # Severity icon
        icon = "❌" if self.severity == ErrorSeverity.ERROR else "⚠️" if self.severity == ErrorSeverity.WARNING else "ℹ️"
        
        # Build error message
        parts = [f"{icon} {self.severity.value.upper()}: {self.message}"]
        
        # Add location info
        if self.file_path:
            location = str(self.file_path)
            if self.line_number:
                location += f":{self.line_number}"
                if self.column_number:
                    location += f":{self.column_number}"
            parts.append(f"  📍 Location: {location}")
            
        # Add metric name if available
        if self.metric_name:
            parts.append(f"  📊 Metric: {self.metric_name}")
            
        # Add suggestion
        if self.suggestion:
            parts.append(f"  💡 Suggestion: {self.suggestion}")
            
        # Add context in verbose mode
        if verbose and self.context:
            parts.append("  📋 Context:")
            for key, value in self.context.items():
                parts.append(f"     {key}: {value}")
                
        # Add related errors
        if self.related_errors:
            parts.append(f"  🔗 Related errors: {len(self.related_errors)}")
            if verbose:
                for related in self.related_errors:
                    parts.append("     " + related.format_terminal(verbose=False).replace("\n", "\n     "))
                    
        return "\n".join(parts)


class ErrorCollector:
    """Collects and manages compilation errors"""
    
    def __init__(self):
        self.errors: List[CompilationError] = []
        self.warnings: List[CompilationError] = []
        self.info: List[CompilationError] = []
        
    def add_error(self, error: CompilationError):
        """Add an error to the collection"""
        if error.severity == ErrorSeverity.ERROR:
            self.errors.append(error)
        elif error.severity == ErrorSeverity.WARNING:
            self.warnings.append(error)
        else:
            self.info.append(error)
            
    def has_errors(self) -> bool:
        """Check if there are any errors"""
        return len(self.errors) > 0
        
    def has_warnings(self) -> bool:
        """Check if there are any warnings"""
        return len(self.warnings) > 0
        
    def get_summary(self) -> Dict[str, int]:
        """Get error summary"""
        return {
            'errors': len(self.errors),
            'warnings': len(self.warnings),
            'info': len(self.info),
            'total': len(self.errors) + len(self.warnings) + len(self.info)
        }
        
    def format_summary(self) -> str:
        """Format a summary for terminal output"""
        summary = self.get_summary()
        
        parts = []
        if summary['errors'] > 0:
            parts.append(f"❌ {summary['errors']} error(s)")
        if summary['warnings'] > 0:
            parts.append(f"⚠️  {summary['warnings']} warning(s)")
        if summary['info'] > 0:
            parts.append(f"ℹ️  {summary['info']} info message(s)")
            
        return " | ".join(parts) if parts else "✅ No issues found"
        
    def to_json(self) -> str:
        """Convert to JSON for reporting"""
        return json.dumps({
            'summary': self.get_summary(),
            'errors': [e.to_dict() for e in self.errors],
            'warnings': [e.to_dict() for e in self.warnings],
            'info': [e.to_dict() for e in self.info]
        }, indent=2)
        
    def to_junit_xml(self) -> str:
        """Convert to JUnit XML format for CI/CD"""
        # Simple JUnit XML format
        xml_parts = ['<?xml version="1.0" encoding="UTF-8"?>']
        xml_parts.append('<testsuites>')
        xml_parts.append('  <testsuite name="BetterDBTMetrics.Compilation" tests="1" failures="{}" errors="0">'.format(
            len(self.errors)
        ))
        
        if self.errors:
            xml_parts.append('    <testcase name="Compilation" classname="BetterDBTMetrics">')
            for error in self.errors:
                xml_parts.append('      <failure message="{}" type="{}">'.format(
                    error.message.replace('"', '&quot;'),
                    error.category.value
                ))
                xml_parts.append(f'        {error.format_terminal(verbose=False)}')
                xml_parts.append('      </failure>')
            xml_parts.append('    </testcase>')
        else:
            xml_parts.append('    <testcase name="Compilation" classname="BetterDBTMetrics"/>')
            
        xml_parts.append('  </testsuite>')
        xml_parts.append('</testsuites>')
        
        return '\n'.join(xml_parts)


class ErrorFactory:
    """Factory for creating common error types with helpful messages"""
    
    @staticmethod
    def missing_import(import_path: str, file_path: Path, line_number: int = None) -> CompilationError:
        """Create error for missing import"""
        return CompilationError(
            message=f"Cannot find import: '{import_path}'",
            category=ErrorCategory.IMPORT,
            severity=ErrorSeverity.ERROR,
            file_path=file_path,
            line_number=line_number,
            suggestion=(
                "Check that the file exists and the path is correct. "
                "Paths can be relative to the current file or absolute from the project root."
            ),
            context={
                'attempted_import': import_path,
                'search_paths': ['Current directory', 'Project root', 'Template directories']
            }
        )
        
    @staticmethod
    def unresolved_reference(ref: str, metric_name: str, file_path: Path) -> CompilationError:
        """Create error for unresolved reference"""
        ref_type = "template" if ref.startswith('$use') else "dimension" if 'dimension' in ref else "unknown"
        
        return CompilationError(
            message=f"Cannot resolve reference: {ref}",
            category=ErrorCategory.REFERENCE,
            severity=ErrorSeverity.ERROR,
            file_path=file_path,
            metric_name=metric_name,
            suggestion=(
                f"Ensure the {ref_type} is imported and the reference path is correct. "
                f"Use '$ref:' for dimension references and '$use:' for template references."
            ),
            context={
                'reference': ref,
                'reference_type': ref_type
            }
        )
        
    @staticmethod
    def invalid_metric_type(metric_name: str, metric_type: str, file_path: Path) -> CompilationError:
        """Create error for invalid metric type"""
        valid_types = ['simple', 'ratio', 'derived', 'cumulative', 'conversion']
        
        return CompilationError(
            message=f"Invalid metric type: '{metric_type}'",
            category=ErrorCategory.METRIC_DEFINITION,
            severity=ErrorSeverity.ERROR,
            file_path=file_path,
            metric_name=metric_name,
            suggestion=f"Valid metric types are: {', '.join(valid_types)}",
            context={
                'provided_type': metric_type,
                'valid_types': valid_types
            }
        )
        
    @staticmethod
    def missing_required_field(field_name: str, metric_name: str, metric_type: str, 
                             file_path: Path) -> CompilationError:
        """Create error for missing required field"""
        # Type-specific requirements
        requirements = {
            'simple': ['name', 'source', 'measure'],
            'ratio': ['name', 'numerator', 'denominator'],
            'derived': ['name', 'expression'],
            'cumulative': ['name', 'source', 'measure', 'window'],
            'conversion': ['name', 'entity', 'calculation', 'window']
        }
        
        return CompilationError(
            message=f"Missing required field '{field_name}' for {metric_type} metric",
            category=ErrorCategory.METRIC_DEFINITION,
            severity=ErrorSeverity.ERROR,
            file_path=file_path,
            metric_name=metric_name,
            suggestion=f"A {metric_type} metric requires these fields: {', '.join(requirements.get(metric_type, []))}",
            context={
                'missing_field': field_name,
                'required_fields': requirements.get(metric_type, [])
            }
        )
        
    @staticmethod
    def invalid_dimension_format(dimension: Any, metric_name: str, file_path: Path) -> CompilationError:
        """Create error for invalid dimension format"""
        return CompilationError(
            message=f"Invalid dimension format: {type(dimension).__name__}",
            category=ErrorCategory.DIMENSION,
            severity=ErrorSeverity.ERROR,
            file_path=file_path,
            metric_name=metric_name,
            suggestion=(
                "Dimensions should be either:\n"
                "  - A string: 'customer_id'\n"
                "  - A dictionary: {name: 'customer_id', type: 'categorical'}\n"
                "  - A reference: {$ref: 'time.daily'}"
            ),
            context={
                'provided_dimension': str(dimension),
                'dimension_type': type(dimension).__name__
            }
        )
        
    @staticmethod
    def circular_dependency(metric_name: str, dependency_chain: List[str], file_path: Path) -> CompilationError:
        """Create error for circular dependency"""
        return CompilationError(
            message=f"Circular dependency detected: {' -> '.join(dependency_chain)}",
            category=ErrorCategory.REFERENCE,
            severity=ErrorSeverity.ERROR,
            file_path=file_path,
            metric_name=metric_name,
            suggestion="Review the metric dependencies and remove the circular reference",
            context={
                'dependency_chain': dependency_chain
            }
        )
        
    @staticmethod
    def template_parameter_error(template_name: str, missing_params: List[str], 
                               extra_params: List[str], file_path: Path) -> CompilationError:
        """Create error for template parameter mismatch"""
        message_parts = []
        if missing_params:
            message_parts.append(f"Missing required parameters: {', '.join(missing_params)}")
        if extra_params:
            message_parts.append(f"Unknown parameters: {', '.join(extra_params)}")
            
        return CompilationError(
            message=f"Template parameter error for '{template_name}': {'; '.join(message_parts)}",
            category=ErrorCategory.TEMPLATE,
            severity=ErrorSeverity.ERROR,
            file_path=file_path,
            suggestion="Check the template definition for required and optional parameters",
            context={
                'template': template_name,
                'missing_parameters': missing_params,
                'extra_parameters': extra_params
            }
        )
        
    @staticmethod
    def yaml_syntax_error(error_message: str, file_path: Path, line_number: int = None) -> CompilationError:
        """Create error for YAML syntax issues"""
        return CompilationError(
            message=f"YAML syntax error: {error_message}",
            category=ErrorCategory.SYNTAX,
            severity=ErrorSeverity.ERROR,
            file_path=file_path,
            line_number=line_number,
            suggestion=(
                "Common YAML issues:\n"
                "  - Check indentation (use spaces, not tabs)\n"
                "  - Ensure proper quoting for special characters\n"
                "  - Verify list syntax (- items)"
            )
        )
        
    @staticmethod
    def performance_warning(metric_name: str, reason: str, file_path: Path) -> CompilationError:
        """Create warning for potential performance issues"""
        return CompilationError(
            message=f"Potential performance issue in metric '{metric_name}': {reason}",
            category=ErrorCategory.VALIDATION,
            severity=ErrorSeverity.WARNING,
            file_path=file_path,
            metric_name=metric_name,
            suggestion="Consider adding filters or materializing this metric for better performance"
        )
        
    @staticmethod
    def deprecated_syntax(old_syntax: str, new_syntax: str, file_path: Path, 
                         metric_name: str = None) -> CompilationError:
        """Create warning for deprecated syntax"""
        return CompilationError(
            message=f"Deprecated syntax: '{old_syntax}'",
            category=ErrorCategory.SYNTAX,
            severity=ErrorSeverity.WARNING,
            file_path=file_path,
            metric_name=metric_name,
            suggestion=f"Use '{new_syntax}' instead",
            context={
                'deprecated': old_syntax,
                'replacement': new_syntax
            }
        )
        
    @staticmethod
    def best_practice_hint(issue: str, recommendation: str, file_path: Path, 
                          metric_name: str = None) -> CompilationError:
        """Create info message for best practices"""
        return CompilationError(
            message=f"Best practice suggestion: {issue}",
            category=ErrorCategory.VALIDATION,
            severity=ErrorSeverity.INFO,
            file_path=file_path,
            metric_name=metric_name,
            suggestion=recommendation
        )


class CompilationReport:
    """Generates comprehensive compilation reports"""
    
    def __init__(self, error_collector: ErrorCollector, compilation_results: Dict[str, Any]):
        self.error_collector = error_collector
        self.results = compilation_results
        
    def generate_terminal_report(self, verbose: bool = False) -> str:
        """Generate a formatted report for terminal output"""
        lines = []
        
        # Header
        lines.append("\n" + "="*60)
        lines.append("📊 Better-DBT-Metrics Compilation Report")
        lines.append("="*60)
        
        # Summary statistics
        lines.append("\n📈 Compilation Statistics:")
        lines.append(f"  Files processed: {self.results.get('files_processed', 0)}")
        lines.append(f"  Metrics compiled: {self.results.get('metrics_compiled', 0)}")
        lines.append(f"  Models generated: {self.results.get('models_generated', 0)}")
        
        # Issue summary
        lines.append(f"\n📋 Issues: {self.error_collector.format_summary()}")
        
        # Errors section
        if self.error_collector.errors:
            lines.append("\n❌ Errors (must fix):")
            lines.append("-" * 40)
            for i, error in enumerate(self.error_collector.errors, 1):
                lines.append(f"\n{i}. {error.format_terminal(verbose=verbose)}")
                
        # Warnings section
        if self.error_collector.warnings:
            lines.append("\n⚠️  Warnings (should review):")
            lines.append("-" * 40)
            for i, warning in enumerate(self.error_collector.warnings, 1):
                lines.append(f"\n{i}. {warning.format_terminal(verbose=verbose)}")
                
        # Info section (only in verbose mode)
        if verbose and self.error_collector.info:
            lines.append("\nℹ️  Information:")
            lines.append("-" * 40)
            for i, info in enumerate(self.error_collector.info, 1):
                lines.append(f"\n{i}. {info.format_terminal(verbose=verbose)}")
                
        # Success/failure message
        lines.append("\n" + "="*60)
        if self.error_collector.has_errors():
            lines.append("❌ Compilation failed with errors")
            lines.append("Please fix the errors above and try again")
        else:
            lines.append("✅ Compilation completed successfully!")
            if self.error_collector.has_warnings():
                lines.append("⚠️  Review warnings for potential issues")
                
        lines.append("="*60 + "\n")
        
        return "\n".join(lines)
        
    def generate_json_report(self) -> str:
        """Generate JSON report for programmatic consumption"""
        return json.dumps({
            'success': not self.error_collector.has_errors(),
            'statistics': {
                'files_processed': self.results.get('files_processed', 0),
                'metrics_compiled': self.results.get('metrics_compiled', 0),
                'models_generated': self.results.get('models_generated', 0)
            },
            'issues': json.loads(self.error_collector.to_json()),
            'timestamp': self.results.get('timestamp'),
            'version': self.results.get('version', '2.0.0')
        }, indent=2)
        
    def generate_html_report(self) -> str:
        """Generate HTML report for web viewing"""
        # Simple HTML report
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Better-DBT-Metrics Compilation Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .error {{ background: #fee; padding: 10px; margin: 10px 0; border-left: 4px solid #f00; }}
        .warning {{ background: #ffe; padding: 10px; margin: 10px 0; border-left: 4px solid #fa0; }}
        .info {{ background: #eef; padding: 10px; margin: 10px 0; border-left: 4px solid #00f; }}
        .success {{ background: #efe; padding: 10px; margin: 10px 0; border-left: 4px solid #0f0; }}
        .metric {{ font-weight: bold; color: #007; }}
        .location {{ color: #666; font-size: 0.9em; }}
        .suggestion {{ color: #060; font-style: italic; }}
        pre {{ background: #f5f5f5; padding: 10px; overflow-x: auto; }}
    </style>
</head>
<body>
    <h1>Better-DBT-Metrics Compilation Report</h1>
    
    <h2>Summary</h2>
    <div class="{'success' if not self.error_collector.has_errors() else 'error'}">
        {self.error_collector.format_summary()}
    </div>
    
    <h2>Statistics</h2>
    <ul>
        <li>Files processed: {self.results.get('files_processed', 0)}</li>
        <li>Metrics compiled: {self.results.get('metrics_compiled', 0)}</li>
        <li>Models generated: {self.results.get('models_generated', 0)}</li>
    </ul>
"""
        
        # Add errors
        if self.error_collector.errors:
            html += "\n<h2>Errors</h2>\n"
            for error in self.error_collector.errors:
                html += self._format_error_html(error, 'error')
                
        # Add warnings
        if self.error_collector.warnings:
            html += "\n<h2>Warnings</h2>\n"
            for warning in self.error_collector.warnings:
                html += self._format_error_html(warning, 'warning')
                
        html += "\n</body>\n</html>"
        return html
        
    def _format_error_html(self, error: CompilationError, css_class: str) -> str:
        """Format a single error as HTML"""
        html = f'<div class="{css_class}">\n'
        html += f'  <div>{error.message}</div>\n'
        
        if error.metric_name:
            html += f'  <div class="metric">Metric: {error.metric_name}</div>\n'
            
        if error.file_path:
            location = str(error.file_path)
            if error.line_number:
                location += f":{error.line_number}"
            html += f'  <div class="location">Location: {location}</div>\n'
            
        if error.suggestion:
            html += f'  <div class="suggestion">Suggestion: {error.suggestion}</div>\n'
            
        html += '</div>\n'
        return html