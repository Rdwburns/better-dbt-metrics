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