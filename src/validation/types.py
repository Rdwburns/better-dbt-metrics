"""
Validation types and data structures
"""

from typing import List, Optional
from dataclasses import dataclass, field


@dataclass
class ValidationError:
    """Represents a validation error"""
    file_path: Optional[str] = None
    line_number: Optional[int] = None
    message: str = ""
    level: str = "error"  # error or warning
    suggestion: Optional[str] = None
    
    def __str__(self):
        location = ""
        if self.file_path:
            location = f"{self.file_path}"
            if self.line_number:
                location += f":{self.line_number}"
        
        error_msg = f"{location} - {self.level}: {self.message}"
        if self.suggestion:
            error_msg += f"\n    Suggestion: {self.suggestion}"
            
        return error_msg
    

@dataclass  
class ValidationResult:
    """Result of validation with errors and warnings"""
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationError] = field(default_factory=list)
    info: List[str] = field(default_factory=list)
    
    @property
    def is_valid(self) -> bool:
        """Check if validation passed (no errors, warnings ok)"""
        return len(self.errors) == 0
        
    def add_error(self, error: ValidationError):
        """Add an error to the result"""
        error.level = "error"
        self.errors.append(error)
        
    def add_warning(self, warning: ValidationError):
        """Add a warning to the result"""
        warning.level = "warning"
        self.warnings.append(warning)
        
    def merge(self, other: 'ValidationResult'):
        """Merge another result into this one"""
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)
        self.info.extend(other.info)
    
    def has_errors(self) -> bool:
        """Check if there are any errors"""
        return len(self.errors) > 0
    
    def __str__(self) -> str:
        """String representation of validation result"""
        lines = []
        
        if self.errors:
            lines.append(f"Found {len(self.errors)} error(s):")
            for error in self.errors:
                lines.append(f"  {error}")
                
        if self.warnings:
            lines.append(f"Found {len(self.warnings)} warning(s):")
            for warning in self.warnings:
                lines.append(f"  {warning}")
                
        if self.info:
            for info in self.info:
                lines.append(info)
                
        return "\n".join(lines)
    
    def print_summary(self):
        """Print a summary of validation results"""
        if self.errors:
            print("\n🔴 ERRORS:")
            for error in self.errors:
                print(f"  {error}")
                
        if self.warnings:
            print("\n🟡 WARNINGS:")
            for warning in self.warnings:
                print(f"  {warning}")
                
        if self.info:
            print("\n🔵 INFO:")
            for info in self.info:
                print(f"  {info}")
        
        # Summary line
        if self.errors or self.warnings:
            print(f"\nSummary: {len(self.errors)} errors, {len(self.warnings)} warnings")
        else:
            print("\n✅ All validation checks passed")